//go:build cgo
// +build cgo

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"

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

	if err := setup(ctx, dialect.DB); err != nil {
		msg := "failed to setup db"
		logrus.Errorf("%s: %v", msg, err)
		return nil, nil, errors.Wrap(err, msg)
	}

	dialect.LastInsertID = true
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.GetSizeSQL = `SELECT (page_count - freelist_count) * page_size FROM pragma_page_count(), pragma_page_size(), pragma_freelist_count()`

	if err := dialect.Prepare(); err != nil {
		return nil, nil, errors.Wrap(err, "query preparation failed")
	}

	return logstructured.New(sqllog.New(dialect)), dialect, nil
}

// setup performs table setup, which may include creation of the Kine table if
// it doesn't already exist, migrating key_value table contents to the Kine
// table if the key_value table exists, all in a single database transaction.
// changes are rolled back if an error occurs.
func setup(ctx context.Context, db *sql.DB) error {
	const retryAttempts = 300
	// Optimistically ask for the user_version without starting a transaction
	var schemaVersion int

	row := db.QueryRowContext(ctx, `PRAGMA user_version`)
	if err := row.Scan(&schemaVersion); err != nil {
		return err
	}

	if schemaVersion == databaseSchemaVersion {
		return nil
	}

	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	for i := 0; i < retryAttempts; i++ {
		if err := migrate(ctx, txn); err != nil {
			if sqliteErr, ok := err.(sqlite3.Error); ok && sqliteErr.ExtendedCode == sqlite3.ErrBusySnapshot {
				// Another write transaction got in the way (SERIALIZABLE isolation).
				// The whole transaction can be retried.
				continue
			}
			return errors.Wrap(err, "migration failed")
		}
		break
	}

	return txn.Commit()
}

// migrate tries to migrate from a version of the database
// to the target one.
func migrate(ctx context.Context, txn *sql.Tx) error {
	var userVersion int

	row := txn.QueryRowContext(ctx, `PRAGMA user_version`)
	if err := row.Scan(&userVersion); err != nil {
		return err
	}

	switch userVersion {
	case 0:
		if err := applySchemaV1(ctx, txn); err != nil {
			return err
		}
		fallthrough
	case databaseSchemaVersion:
		break
	default:
		// FIXME this needs better handling
		return errors.Errorf("unsupported version: %d", userVersion)
	}

	setUserVersionSQL := fmt.Sprintf(`PRAGMA user_version = %d`, databaseSchemaVersion)
	if _, err := txn.ExecContext(ctx, setUserVersionSQL); err != nil {
		return err
	}

	return nil
}
