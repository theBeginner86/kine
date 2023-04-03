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
			fmt.Printf("DB setup successful\n")
			break
		}
		fmt.Printf("failed to setup db: %v\n", err)
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

	// if err := doMigrate(context.Background(), dialect); err != nil {
	// 	return nil, nil, errors.Wrap(err, "migration failed")
	// }

	if err := dialect.Prepare(); err != nil {
		return nil, nil, errors.Wrap(err, "query preparation failed")
	}

	return logstructured.New(sqllog.New(dialect)), dialect, nil
}

func setup(dialect *generic.Generic) error {
	if err := createTable(dialect.DB); err != nil {
		return err
	}

	_, count := countTable(context.Background(), dialect.DB, "kine")
	fmt.Printf("number of tables: %d\n", count)

	if err := doMigrate(context.Background(), dialect); err != nil {
		return errors.Wrap(err, "migration failed")
	}

	return nil
}

func createTable(db *sql.DB) error {
	fmt.Printf("Create Table\n")
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

	_, err := db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	return nil
}

// alterTableIndices drops the given old table indices from the existing
// kine table (if it exists, and if the indices exists), and then creates
// the given new ones on the kine table.
func alterTableIndices(d *generic.Generic) error {
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
		_, err := d.DB.Exec(stmt)
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
		_, err := d.DB.Exec(stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func countTables(ctx context.Context, db *sql.DB) (error, int) {
	// Check if the key_value table exists
	allTablesSQL := `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'`
	row := db.QueryRowContext(ctx, allTablesSQL)
	var count int
	if err := row.Scan(&count); err != nil {
		return err, 0
	}

	return nil, count
}

func countTable(ctx context.Context, db *sql.DB, tableName string) (error, int) {
	// Check if the key_value table exists
	tableListSQL := fmt.Sprintf(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '%s'`, tableName)
	row := db.QueryRowContext(ctx, tableListSQL)
	var tableCount int
	if err := row.Scan(&tableCount); err != nil {
		return err, 0
	}

	return nil, tableCount
}

// checkMigrate performs migration from an old key value table to the kine
// table only if the old key value table exists and migration has not been
// done already.
func doMigrate(ctx context.Context, d *generic.Generic) error {
	fmt.Printf("do migrate\n")
	userVersionSQL := `PRAGMA user_version`
	row := d.DB.QueryRowContext(ctx, userVersionSQL)

	var userVersion int
	if err := row.Scan(&userVersion); err != nil {
		return err
	}
	// No need for migration - marker has already been set
	// if userVersion == 1 {
	// 	return nil
	// }

	fmt.Printf("user version pass\n")

	_, allTables := countTables(context.Background(), d.DB)
	fmt.Printf("all tables count :%d\n", allTables)

	// Check if the key_value table exists
	_, tableCount := countTable(context.Background(), d.DB, "key_value")
	// tableListSQL := `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'key_value'`
	// row = d.DB.QueryRowContext(ctx, tableListSQL)
	// var tableCount int
	// if err := row.Scan(&tableCount); err != nil {
	// 	return err
	// }
	fmt.Printf("table count :%d\n", tableCount)

	// Perform migration from key_value table to kine table
	if tableCount > 0 {
		fmt.Printf("CheckTableRowCounts")
		if err := d.CheckTableRowCounts(ctx); err != nil {
			msg := "table rows could not be counted during migration"
			logrus.Errorf("%s: %v", msg, err)
			return errors.Wrap(err, msg)
		}

		fmt.Printf("MigrateRows")
		err := d.MigrateRows(ctx)
		if err != nil {
			msg := "failed to migrate rows"
			logrus.Errorf("%s: %v", msg, err)
			return errors.Wrap(err, msg)
		}
	}

	if err := alterTableIndices(d); err != nil {
		msg := "table index changes failed during migration"
		logrus.Errorf("%s: %v", msg, err)
		return errors.Wrap(err, msg)
	}

	setUserVersionSQL := `PRAGMA user_version = 1`
	_, err := d.DB.ExecContext(ctx, setUserVersionSQL)
	if err != nil {
		msg := "version setting failed during migration"
		logrus.Errorf("%s: %v", msg, err)
		return errors.Wrap(err, msg)
	}

	return nil
}
