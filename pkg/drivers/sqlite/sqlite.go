//go:build cgo
// +build cgo

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/rancher/kine/pkg/drivers/generic"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/sqllog"
	"github.com/rancher/kine/pkg/server"
	"github.com/sirupsen/logrus"

	// sqlite db driver
	_ "github.com/mattn/go-sqlite3"
)

func New(ctx context.Context, dataSourceName string) (server.Backend, error) {
	backend, _, err := NewVariant(ctx, "sqlite3", dataSourceName)
	return backend, err
}

func NewVariant(ctx context.Context, driverName, dataSourceName string) (server.Backend, *generic.Generic, error) {
	if dataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return nil, nil, err
		}
		dataSourceName = "./db/state.db?_journal=WAL&cache=shared"
	}

	dialect, err := generic.Open(ctx, driverName, dataSourceName, "?", false)
	if err != nil {
		return nil, nil, err
	}
	dialect.LastInsertID = true
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.GetSizeSQL = `SELECT (page_count - freelist_count) * page_size FROM pragma_page_count(), pragma_page_size(), pragma_freelist_count()`
	// this is the first SQL that will be executed on a new DB conn so
	// loop on failure here because in the case of dqlite it could still be initializing
	for i := 0; i < 300; i++ {
		err = setup(dialect)
		if err == nil {
			break
		}
		logrus.Errorf("failed to setup db: %v", err)
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(time.Second):
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, "db table creation failed")
	}

	if err := dialect.Prepare(); err != nil {
		return nil, nil, errors.Wrap(err, "query preparation failed")
	}

	return logstructured.New(sqllog.New(dialect)), dialect, nil
}

// setup performs table setup, which may include creation of the Kine table if
// it doesn't already exist, migrating key_value table contents to the Kine
// table if the key_value table exists, all in a single database transaction.
// changes are rolled back if an error occurs.
func setup(dialect *generic.Generic) error {
	ctx := context.Background()
	txn, err := dialect.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	if err := migration(ctx, txn, dialect); err != nil {
		return errors.Wrap(err, "migration failed")
	}

	return txn.Commit()
}

func createKineTable(txn *sql.Tx) error {
	createTableSQL := `CREATE TABLE IF NOT EXISTS kine
			(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL,
				created INTEGER,
				deleted INTEGER,
				create_revision INTEGER NOT NULL,
				prev_revision INTEGER,
				lease INTEGER,
				value BLOB,
				old_value BLOB
			)`

	_, err := txn.Exec(createTableSQL)
	if err != nil {
		return err
	}

	return nil
}

// removeTable drops the table with the given name, in the given transaction.
func removeTable(txn *sql.Tx, tableName string) error {
	dropTableSQL := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName)
	_, err := txn.Exec(dropTableSQL)
	if err != nil {
		return err
	}

	return nil
}

// alterIndices drops the given old database indices and creates
// the given new ones.
func alterIndices(txn *sql.Tx, d *generic.Generic) error {
	if d.LockWrites {
		d.Lock()
		defer d.Unlock()
	}

	dropIndices := []string{
		`DROP INDEX IF EXISTS kine_name_index`,
		`DROP INDEX IF EXISTS kine_name_prev_revision_uindex`,
	}

	// Drop the old indices
	for _, stmt := range dropIndices {
		_, err := txn.Exec(stmt)
		if err != nil {
			return err
		}
	}

	createIndices := []string{
		`CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name, id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (prev_revision, name)`,
	}

	// Create the new indices
	for _, stmt := range createIndices {
		_, err := txn.Exec(stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

// countTable counts the number of tables with the given name, in the given transaction.
func hasTable(ctx context.Context, txn *sql.Tx, tableName string) (bool, error) {
	tableListSQL := fmt.Sprintf(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '%s'`, tableName)
	row := txn.QueryRowContext(ctx, tableListSQL)
	var tableCount int
	if err := row.Scan(&tableCount); err != nil {
		return false, err
	}

	return tableCount != 0, nil
}

// migration copies rows of the old key value table to the kine
// table ONLY IF the key value table exists and migration has not been
// done already. It creates the kine table if it doesn't exist.
func migration(ctx context.Context, txn *sql.Tx, d *generic.Generic) error {
	userVersionSQL := `PRAGMA user_version`
	row := txn.QueryRowContext(ctx, userVersionSQL)

	var userVersion int
	if err := row.Scan(&userVersion); err != nil {
		return err
	}
	// No need for migration - marker has already been set
	if userVersion == databaseSchemaVersion {
		return nil
	}

	if userVersion > databaseSchemaVersion {
		return errors.Errorf("unsupported version: %d", userVersion)
	}

	kineTableExists, err := hasTable(ctx, txn, "kine")
	if err != nil {
		return err
	}
	// Create Kine table if it doesn't already exist
	if !kineTableExists {
		if err := createKineTable(txn); err != nil {
			return err
		}
	}

	kvTableExists, err := hasTable(ctx, txn, "key_value")
	if err != nil {
		return err
	}
	// If the key_value table exists, perform migration from key_value table to kine table
	if kvTableExists {
		if err := d.CheckTableRowCounts(txn, ctx); err != nil {
			msg := "table row count issue before migration"
			logrus.Errorf("%s: %v", msg, err)
			return errors.Wrap(err, msg)
		}

		if err := d.MigrateRows(txn, ctx); err != nil {
			msg := "failed to migrate rows"
			logrus.Errorf("%s: %v", msg, err)
			return errors.Wrap(err, msg)
		}

		if err := removeTable(txn, "key_value"); err != nil {
			msg := "unable to remove key_value table during migration"
			logrus.Errorf("%s: %v", msg, err)
			return errors.Wrap(err, msg)
		}
	}

	if err := alterIndices(txn, d); err != nil {
		msg := "index changes failed during migration"
		logrus.Errorf("%s: %v", msg, err)
		return errors.Wrap(err, msg)
	}

	setUserVersionSQL := `PRAGMA user_version = 1`
	_, err = txn.ExecContext(ctx, setUserVersionSQL)
	if err != nil {
		msg := "version setting failed during migration"
		logrus.Errorf("%s: %v", msg, err)
		return errors.Wrap(err, msg)
	}

	return nil
}
