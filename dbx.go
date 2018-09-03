package dbx

import (
	"database/sql"

	"time"

	"log"

	"regexp"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const (
	LevelSlowQuery = "SLOW_QUERY"
	LevelDebug     = "DEBUG"
	LevelError     = "ERROR"
)

func init() {
	regSpaceTrim = regexp.MustCompile("(\n)|(\\s+)|(\t+)")
}

type Querier interface {
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	Select(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (sql.Result, error)
	NamedExec(query string, arg interface{}) (sql.Result, error)
	Rebind(query string) string
}

type Querierx interface {
	Querier
	NamedInsert(target interface{}, tableName string, params []string, arg map[string]interface{}) (string, []interface{}, error)
	NamedSelect(dest interface{}, query string, arg interface{}) error
	SkipLog()
}

type dbxInternal interface {
	Querier
	getDB() Querier
	logQuery(query string, execTime time.Duration, err error, args ...interface{}) error
}

type DBX struct {
	db     *sqlx.DB
	driver string

	errorLog   *log.Logger
	debugLog   *log.Logger
	slowLog    *log.Logger
	slowLogMin time.Duration
	logAsync   bool
	skipLog    bool
}

func (dbx *DBX) MustBegin() *Tx {
	return &Tx{
		dbx.db.MustBegin(),
		dbx.errorLog,
		dbx.debugLog,
		dbx.slowLog,
		dbx.slowLogMin,
		dbx.logAsync,
		false,
	}
}

func (dbx *DBX) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return queryX(dbx, query, args...)
}

func (dbx *DBX) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	return queryRowx(dbx, query, args...)
}

func (dbx *DBX) Select(dest interface{}, query string, args ...interface{}) error {
	return selectX(dbx, dest, query, args...)
}

func (dbx *DBX) Exec(query string, args ...interface{}) (sql.Result, error) {
	return exec(dbx, query, args...)
}

func (dbx *DBX) Rebind(query string) string {
	return dbx.db.Rebind(query)
}

func (dbx *DBX) Close() error {
	return dbx.db.Close()
}

func (dbx *DBX) Unsafe() *DBX {
	unsafe := dbx.db.Unsafe()
	return &DBX{db: unsafe, errorLog: dbx.errorLog, debugLog: dbx.debugLog, slowLog: dbx.slowLog, slowLogMin: dbx.slowLogMin, logAsync: dbx.logAsync}
}

func (dbx *DBX) SetMaxOpenConns(n int) {
	dbx.db.SetMaxOpenConns(n)
}

func (dbx *DBX) SetMaxIdleConns(n int) {
	dbx.db.SetMaxIdleConns(n)
}

func (dbx *DBX) NamedSelect(dest interface{}, query string, arg interface{}) error {
	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}

	return dbx.Select(dest, query, args...)
}

func (dbx *DBX) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return namedExec(dbx, query, arg)
}

func (dbx *DBX) NamedInsert(target interface{}, tableName string, paramNames []string, m map[string]interface{}) (string, []interface{}, error) {
	return namedInsert(target, tableName, paramNames, m)
}

func (dbx *DBX) getDB() Querier {
	return dbx.db
}

func (dbx *DBX) SkipLog() {
	dbx.skipLog = true
}

func (dbx *DBX) logQuery(query string, execTime time.Duration, err error, args ...interface{}) error {
	if dbx.logAsync == true {
		go dbx.log(query, execTime, err, args...)
		return nil
	}

	return dbx.log(query, execTime, err, args...)
}

func (dbx *DBX) log(query string, execTime time.Duration, err error, args ...interface{}) error {
	if dbx.skipLog == true {
		dbx.skipLog = false
		return nil
	}

	if err != nil && dbx.errorLog != nil {
		if err2 := logMsg(dbx.errorLog, LevelError, query, execTime, errors.WithStack(err), args...); err2 != nil {
			return err2
		}
	}

	if execTime >= dbx.slowLogMin && dbx.slowLog != nil {
		if err3 := logMsg(dbx.slowLog, LevelSlowQuery, query, execTime, nil, args...); err3 != nil {
			return err3
		}
	}

	if dbx.debugLog != nil {
		if err4 := logMsg(dbx.debugLog, LevelDebug, query, execTime, nil, args...); err4 != nil {
			return err4
		}
	}

	return nil
}
