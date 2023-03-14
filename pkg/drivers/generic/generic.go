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

	"github.com/k3s-io/kine/pkg/util"
	"github.com/Rican7/retry/jitter"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	columns = "kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"

	revSQL = `
		SELECT MAX(rkv.id) AS id
		FROM kine AS rkv` 
	
	// For new additions to upstream kine for better compaction only - GetCompactRevision()
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

	compactRevSQL = `
		SELECT crkv.prev_revision
		FROM kine crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC LIMIT 1`

	listCurrentSQL = fmt.Sprintf(`SELECT (%s), (%s), %s
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

	listSQL = fmt.Sprintf(`
		SELECT %s
		FROM kine as kv
			LEFT JOIN kine kv2
				ON kv.name = kv2.name
				AND kv.id < kv2.id
		WHERE kv2.name IS NULL
			AND kv.name >= ? AND kv.name < ?
			AND (? OR kv.deleted = 0)
			%%s
		ORDER BY kv.id ASC
		`, columns)

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
	AfterSQLPrefix			string
	afterSQLPrefixPrepared		*sql.Stmt
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

	if err := countKV.Scan(&count); err != nil || count == 0 {
		return
	}

	if err := countKine.Scan(&count); err != nil || count != 0 {
		return
	}	

	logrus.Infof("Migrating content from old table")
	// Migrate the latest revision rows from the old table to the new
	_, err := d.execute(ctx,
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
			FROM kine AS kv
			WHERE kv.id = ?`, columns), paramCharacter, numbered),

		GetCurrentSQL:        q(fmt.Sprintf(listCurrentSQL, ""), paramCharacter, numbered),
		ListRevisionStartSQL: q(fmt.Sprintf(listSQL, "AND kv.id <= ?"), paramCharacter, numbered),
		
		GetRevisionAfterSQL:  q(revisionAfterSQL, paramCharacter, numbered),

		CountSQL: q(fmt.Sprintf(`
			SELECT (%s), COUNT(*)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "")), paramCharacter, numbered),

		AfterSQLPrefix: q(fmt.Sprintf(`
			SELECT %s
			FROM kine AS kv
			WHERE
				kv.name >= ? AND kv.name < ?
				AND kv.id > ?
			ORDER BY kv.id ASC`, columns), paramCharacter, numbered),			

		AfterSQL: q(fmt.Sprintf(`
			SELECT %s
				FROM kine AS kv
				WHERE kv.id > ?
				ORDER BY kv.id ASC
			`, columns), paramCharacter, numbered),
		
		DeleteSQL: q(`
			DELETE FROM kine AS kv
			WHERE kv.id = ?`, paramCharacter, numbered),

		UpdateCompactSQL: q(`
			UPDATE kine
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),

		InsertSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value) VALUES(?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`, paramCharacter, numbered),

		FillSQL: q(`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),
	}, err
}

// Prepare uses the Dialect SQL queries and calls the backend database to produce prepared statements. 
func (d* Generic) Prepare() error {
	var	err error

	d.getRevisionSQLPrepared, err = d.DB.Prepare(d.GetRevisionSQL)
	if err != nil {
		return err
	}

	d.countSQLPrepared, err = d.DB.Prepare(d.CountSQL)
	if err != nil {
		return err
	}

	d.deleteSQLPrepared, err = d.DB.Prepare(d.DeleteSQL)
	if err != nil {
		return err
	}

	// could not test with just running k8s-dqlite - may need unit test
	d.updateCompactSQLPrepared, err = d.DB.Prepare(d.UpdateCompactSQL)
	if err != nil {
		return err
	}

	d.fillSQLPrepared, err = d.DB.Prepare(d.FillSQL)
	if err != nil {
		return err
	}

	d.insertLastInsertIDSQLPrepared, err = d.DB.Prepare(d.InsertLastInsertIDSQL)
	if err != nil {
		return err
	}

	d.getSizeSQLPrepared, err = d.DB.Prepare(d.GetSizeSQL)
	if err != nil {
		return err
	}

	return nil
}

func (d *Generic) queryPrepared(ctx context.Context, sql string, prepared *sql.Stmt, args ...interface{})(result *sql.Rows, err error) {
	logrus.Tracef("QUERY %v : %s", args, util.Stripped(sql))
	return prepared.QueryContext(ctx, args...)
}

func (d *Generic) query(ctx context.Context, sql string, args ...interface{})(rows *sql.Rows, err error) {
	logrus.Tracef("QUERY %v : %s", args, util.Stripped(sql))
	return d.DB.QueryContext(ctx, sql, args...)
}

func (d* Generic) queryRow(ctx context.Context, sql string, args ...interface{})(result *sql.Row) {
	logrus.Tracef("QUERY ROW %v : %s", args, util.Stripped(sql))
	return d.DB.QueryRowContext(ctx, sql, args...)	
}

func (d* Generic) queryRowPrepared(ctx context.Context, sql string, prepared *sql.Stmt, args ...interface{})(result *sql.Row) {
	logrus.Tracef("QUERY %v : %s", args, util.Stripped(sql))
	return prepared.QueryRowContext(ctx, args...)
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

func (d *Generic) executePrepared(ctx context.Context, sql string, prepared *sql.Stmt, args ...interface{}) (result sql.Result, err error) {
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
		result, err = prepared.ExecContext(ctx, args...)
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

// Could not test with just running k8sdqlite - may need unit tests
func (d *Generic) SetCompactRevision(ctx context.Context, revision int64) error {
	_, err := d.executePrepared(ctx, d.UpdateCompactSQL, d.updateCompactSQLPrepared, revision)
	return err
}

func (d* Generic) GetRevision(ctx context.Context, revision int64)(*sql.Rows, error) {
	return d.queryPrepared(ctx, d.GetRevisionSQL, d.getRevisionSQLPrepared, revision)
}

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("DELETEREVISION %v", revision)	
	_, err := d.executePrepared(ctx, d.DeleteSQL, d.deleteSQLPrepared, revision)
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
	start, end := getPrefixRange(prefix)

	if startKey == "" {
		sql := d.ListRevisionStartSQL
		if limit > 0 {
			sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
		}
		return d.query(ctx, sql, start, end, revision, includeDeleted)
	}

	sql := d.GetRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, start, end, revision, startKey, revision, includeDeleted)
}

func (d *Generic) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	start, end := getPrefixRange(prefix)	
	row := d.queryRowPrepared(ctx, d.CountSQL, d.countSQLPrepared, start, end, false)
	err := row.Scan(&rev, &id)
	return rev.Int64, id, err
}

func (d *Generic) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, revSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil	
	}
	return id, err
}

func (d *Generic) AfterPrefix(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
	start, end := getPrefixRange(prefix)	
	sql := d.AfterSQLPrefix
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, start, end, rev)
}

func (d *Generic) After(ctx context.Context, rev, limit int64) (*sql.Rows, error) {
	sql := d.AfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, rev)
}

func (d *Generic) Fill(ctx context.Context, revision int64) error {
	_, err := d.executePrepared(ctx, d.FillSQL, d.fillSQLPrepared, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil)
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
		row, err := d.executePrepared(ctx, d.InsertLastInsertIDSQL, d.insertLastInsertIDSQLPrepared, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 0, err
		}
		return row.LastInsertId()
	}

	// For DBs that use RETURNING as a keyword instead of implementing LastInsertId.  
	// We don't prepare the statement though, as the version of sqlite doesn't suppory this keyword.
	id, err = d.queryInt64(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)

	return id, err
}

func (d *Generic) GetSize(ctx context.Context)(int64, error){
	if d.GetSizeSQL == "" {
		return 0, errors.New("driver does not support size reporting")
	}

	if d.getSizeSQLPrepared == nil {
		return 0, errors.New("internal error: prepared statement is nil when getting total size")
	}
	
	var size int64
	row := d.queryRowPrepared(ctx, d.GetSizeSQL, d.getSizeSQLPrepared)
	
	if err := row.Scan(&size); err != nil {
		return 0, err
	}
	
	return size, nil
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

func getPrefixRange(prefix string) (start, end string) {
	start = prefix
	if strings.HasSuffix(prefix, "/") {
		end = prefix[0:len(prefix)-1] + "0"
	} else {
		end = prefix + "\x00"
	}
	
	return start, end
}

