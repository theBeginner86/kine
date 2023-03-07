package generic

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Rican7/retry/jitter"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	columns = "kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"
	//revSQL  = `
	//	SELECT rkv.id
	//	FROM kine rkv
	//	ORDER BY rkv.id
	//	DESC LIMIT 1`

	revSQL = `
		SELECT MAX(rkv.id) AS id
		FROM kine AS rkv` 

	revisionIntervalSQL = `
		SELECT (
			SELECT crkv.prev_revision
			FROM kine AS crkv
			WHERE crkv.name = 'compact_rev_key'
			ORDER BY prev_revision
			DESC LIMIT 1
		) AS low, (
			SELECT id
			FROM kine
			ORDER BY id
			DESC LIMIT 1
		) AS high`

	// remove
	compactRevSQL = `
		SELECT crkv.prev_revision
		FROM kine crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC LIMIT 1`

	// remove
	idOfKey = `
		AND mkv.id <= ? AND mkv.id > (
			SELECT ikv.id
			FROM kine ikv
			WHERE
				ikv.name = ? AND
				ikv.id <= ?
			ORDER BY ikv.id DESC LIMIT 1)`

	listSQL = fmt.Sprintf(`SELECT (%s), (%s), %s
		FROM kine kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine mkv
			WHERE
				mkv.name LIKE ?
				%%s
			GROUP BY mkv.name) maxkv
	    ON maxkv.id = kv.id
		WHERE
			  (kv.deleted = 0 OR ?)
		ORDER BY kv.id ASC
		`, revSQL, compactRevSQL, columns)

	//listSQL = fmt.Sprintf(`
	//	SELECT %s
	//	FROM kine as kv
	//		LEFT JOIN kine kv2
	//			ON kv.name = kv2.name
	//			AND kv.id < kv2.id
	//	WHERE kv2.name IS NULL
	//		AND kv.name >= ? AND kv.name < ?
	//		AND (? OR kv.deleted = 0)
	//		%%s
	//	ORDER BY kv.id ASC
	//	`, columns)

	revisionAfterSQL = fmt.Sprintf(`
		SELECT *
		FROM (
			SELECT %s
			FROM kine AS kv
			JOIN (
				SELECT MAX(mkv.id) AS id
				FROM kine AS mkv
				WHERE mkv.name >= ? AND mkv.name < ?
					AND mkv.id <= ?
					AND mkv.id > (
						SELECT ikv.id
						FROM kine ikv
						WHERE
							ikv.name = ? AND
							ikv.id <= ?
						ORDER BY ikv.id DESC
						LIMIT 1
					)
				GROUP BY mkv.name
			) AS maxkv
				ON maxkv.id = kv.id
			WHERE ? OR kv.deleted = 0
		) AS lkv
		ORDER BY lkv.theid ASC`, columns)
)

type Stripped string

func (s Stripped) String() string {
	str := strings.ReplaceAll(string(s), "\n", "")
	return regexp.MustCompile("[\t ]+").ReplaceAllString(str, " ")
}

type ErrRetry func(error) bool
type TranslateErr func(error) error
type ErrCode func(error) string


type Generic struct {
	sync.Mutex

	LockWrites            		bool
	LastInsertID          		bool
	DB                    		*sql.DB
	GetCurrentSQL         		string
	getCurrentSQLPrepared 		*sql.Stmt
	GetRevisionSQL        		string
	getRevisionSQLPrepared		*sql.Stmt
	RevisionSQL           		string
	revisionSQLPrepared		*sql.Stmt
	ListRevisionStartSQL  		string
	listRevisionStartSQLPrepared	*sql.Stmt
	GetRevisionAfterSQL   		string
	getRevisionAfterSQLPrepared	*sql.Stmt
	CountSQL              		string
	countSQLPrepared		*sql.Stmt
	AfterSQLPrefixed		string
	afterSQLPrefixedPrepared	*sql.Stmt
	AfterSQL              		string
	afterSQLPrepared		*sql.Stmt
	DeleteSQL             		string
	deleteSQLPrepared		*sql.Stmt
	CompactSQL	      		string
	compactSQLPrepared		*sql.Stmt
	UpdateCompactSQL      		string
	updateCompactSQLPrepared	*sql.Stmt
	InsertSQL             		string
	insertSQLPrepared		*sql.Stmt
	FillSQL               		string
	fillSQLPrepared			*sql.Stmt	
	InsertLastInsertIDSQL 		string
	insertLastInsertIDSQLPrepared	*sql.Stmt
	GetSizeSQL            		string
	getSizeSQLPrepared		*sql.Stmt
	Retry                 		ErrRetry
	TranslateErr          		TranslateErr
	ErrCode		      		ErrCode

	// CompactInterval is interval between database compactions performed by kine.
	CompactInterval time.Duration
	// PollInterval is the event poll interval used by kine.
	PollInterval time.Duration
}

func configureConnectionPooling(db *sql.DB) {
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(5)
	db.SetConnMaxLifetime(60 * time.Second)
}

func q(sql, param string, numbered bool) string {
	if param == "?" && !numbered {
		return sql
	}

	regex := regexp.MustCompile(`\?`)
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		if numbered {
			n++
			return param + strconv.Itoa(n)
		}
		return param
	})
}

func (d *Generic) Migrate(ctx context.Context) {
	var (
		count = 0
		countKV = d.queryRow(ctx, "SELECT COUNT(*) FROM key_value")
		countKine = d.queryRow(ctx, "SELECT COUNT(*) FROM kine")
	)

	if err := countKV.Scan(&count); err!= nil || count == 0 {
		return
	}

	count, err := d.queryInt64(ctx, "SELECT COUNT(*) FROM key_value")
	if err != nil || count == 0 {
		return
	}

	count, err = d.queryInt64(ctx, "SELECT COUNT(*) FROM kine")
	if err != nil || count != 0 {
		return
	}

	logrus.Infof("Migrating content from old table")
	_, err = d.execute(ctx,
		`INSERT INTO kine(deleted, create_revision, prev_revision, name, value, created, lease)
					SELECT 0, 0, 0, kv.name, kv.value, 1, CASE WHEN kv.ttl > 0 THEN 15 ELSE 0 END
					FROM key_value kv
						WHERE kv.id IN (SELECT MAX(kvd.id) FROM key_value kvd GROUP BY kvd.name)`)
	if err != nil {
		logrus.Errorf("Migration failed: %v", err)
	}
}

func openAndTest(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func Open(ctx context.Context, driverName, dataSourceName string, paramCharacter string, numbered bool) (*Generic, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = openAndTest(driverName, dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("failed to ping connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	configureConnectionPooling(db)

	return &Generic{
		DB: db,

		GetRevisionSQL: q(fmt.Sprintf(`
			SELECT
			0, 0, %s
			FROM kine kv
			WHERE kv.id = ?`, columns), paramCharacter, numbered),

		GetCurrentSQL:        q(fmt.Sprintf(listSQL, ""), paramCharacter, numbered),
		ListRevisionStartSQL: q(fmt.Sprintf(listSQL, "AND mkv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterSQL:  q(fmt.Sprintf(listSQL, idOfKey), paramCharacter, numbered),

		CountSQL: q(fmt.Sprintf(`
			SELECT (%s), COUNT(c.theid)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "")), paramCharacter, numbered),

		AfterSQL: q(fmt.Sprintf(`
			SELECT (%s), (%s), %s
			FROM kine kv
			WHERE
				kv.name LIKE ? AND
				kv.id > ?
			ORDER BY kv.id ASC`, revSQL, compactRevSQL, columns), paramCharacter, numbered),

		DeleteSQL: q(`
			DELETE FROM kine
			WHERE id = ?`, paramCharacter, numbered),

		UpdateCompactSQL: q(`
			UPDATE kine
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),

		InsertSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`, paramCharacter, numbered),

		FillSQL: q(`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),
	}, err
}

func (d *Generic) query(ctx context.Context, sql string, args ...interface{}) (rows *sql.Rows, err error) {
	i := uint(0)
	defer func() {
		if err != nil {
			err = fmt.Errorf("query (try: %d): %w", i, err)
		}
	}()
	for ; i < 500; i++ {
		if i > 2 {
			logrus.Debugf("QUERY (try: %d) %v : %s", i, args, Stripped(sql))
		} else {
			logrus.Tracef("QUERY (try: %d) %v : %s", i, args, Stripped(sql))
		}
		rows, err = d.DB.QueryContext(ctx, sql, args...)
		if err != nil && d.Retry != nil && d.Retry(err) {
			time.Sleep(jitter.Deviation(nil, 0.3)(2 * time.Millisecond))
			continue
		}
		return rows, err
	}
	return
}

func (d *Generic) queryInt64(ctx context.Context, sql string, args ...interface{}) (n int64, err error) {
	i := uint(0)
	defer func() {
		if err != nil {
			err = fmt.Errorf("query int64 (try: %d): %w", i, err)
		}
	}()
	for ; i < 500; i++ {
		if i > 2 {
			logrus.Debugf("QUERY INT64 (try: %d) %v : %s", i, args, Stripped(sql))
		} else {
			logrus.Tracef("QUERY INT64 (try: %d) %v : %s", i, args, Stripped(sql))
		}
		row := d.DB.QueryRowContext(ctx, sql, args...)
		err = row.Scan(&n)
		if err != nil && d.Retry != nil && d.Retry(err) {
			time.Sleep(jitter.Deviation(nil, 0.3)(2 * time.Millisecond))
			continue
		}
		return n, err
	}
	return
}

func (d *Generic) execute(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	i := uint(0)
	defer func() {
		if err != nil {
			err = fmt.Errorf("exec (try: %d): %w", i, err)
		}
	}()
	if d.LockWrites {
		d.Lock()
		defer d.Unlock()
	}

	for ; i < 500; i++ {
		if i > 2 {
			logrus.Debugf("EXEC (try: %d) %v : %s", i, args, Stripped(sql))
		} else {
			logrus.Tracef("EXEC (try: %d) %v : %s", i, args, Stripped(sql))
		}
		result, err = d.DB.ExecContext(ctx, sql, args...)
		if err != nil && d.Retry != nil && d.Retry(err) {
			time.Sleep(jitter.Deviation(nil, 0.3)(2 * time.Millisecond))
			continue
		}
		return result, err
	}
	return
}

func (d *Generic) GetCompactRevision(ctx context.Context) (int64, error) {
	id, err := d.queryInt64(ctx, compactRevSQL)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) SetCompactRevision(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.UpdateCompactSQL, revision)
	return err
}

func (d *Generic) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return d.query(ctx, d.GetRevisionSQL, revision)
}

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.DeleteSQL, revision)
	return err
}

func (d *Generic) ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	sql := d.GetCurrentSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, includeDeleted)
}

func (d *Generic) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	if startKey == "" {
		sql := d.ListRevisionStartSQL
		if limit > 0 {
			sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
		}
		return d.query(ctx, sql, prefix, revision, includeDeleted)
	}

	sql := d.GetRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, revision, startKey, revision, includeDeleted)
}

func (d *Generic) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
		err error
		i   uint
	)

	for ; i < 500; i++ {
		if i > 0 {
			logrus.Debugf("COUNT (try: %d) : %s", i, prefix)
		} else {
			logrus.Tracef("COUNT (try: %d) : %s", i, prefix)
		}
		row := d.DB.QueryRowContext(ctx, d.CountSQL, prefix, false)
		err = row.Scan(&rev, &id)
		if err != nil && d.Retry != nil && d.Retry(err) {
			time.Sleep(jitter.Deviation(nil, 0.3)(2 * time.Millisecond))
			continue
		}
		break
	}
	if err != nil {
		err = fmt.Errorf("count %s (try: %d): %w", prefix, i, err)
	}
	return rev.Int64, id, err
}

func (d *Generic) CurrentRevision(ctx context.Context) (int64, error) {
	id, err := d.queryInt64(ctx, revSQL)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) After(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
	sql := d.AfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, rev)
}

func (d *Generic) Fill(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.FillSQL, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil)
	return err
}

func (d *Generic) IsFill(key string) bool {
	return strings.HasPrefix(key, "gap-")
}

func (d *Generic) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (id int64, err error) {
	if d.TranslateErr != nil {
		defer func() {
			if err != nil {
				err = d.TranslateErr(err)
			}
		}()
	}

	cVal := 0
	dVal := 0
	if create {
		cVal = 1
	}
	if delete {
		dVal = 1
	}

	if d.LastInsertID {
		row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 0, err
		}
		return row.LastInsertId()
	}

	id, err = d.queryInt64(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
	return id, err
}

func (d *Generic) GetSize(ctx context.Context) (int64, error) {
	if d.GetSizeSQL == "" {
		return 0, errors.New("driver does not support size reporting")
	}
	return d.queryInt64(ctx, d.GetSizeSQL)
}

func (d *Generic) GetCompactInterval() time.Duration {
	if v := d.CompactInterval; v > 0 {
		return v
	}
	return 5 * time.Minute
}

func (d *Generic) GetPollInterval() time.Duration {
	if v := d.PollInterval; v > 0 {
		return v
	}
	return time.Second
}
