package dbx

import (
	"database/sql"

	"time"

	"log"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type Tx struct {
	tx *sqlx.Tx

	errorLog   *log.Logger
	debugLog   *log.Logger
	slowLog    *log.Logger
	slowLogMin time.Duration
	logAsync   bool
	skipLog    bool
}

func (tx *Tx) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return queryX(tx, query, args...)
}

func (tx *Tx) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	return queryRowx(tx, query, args...)
}

func (tx *Tx) Select(dest interface{}, query string, args ...interface{}) error {
	return selectX(tx, dest, query, args...)
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return exec(tx, query, args...)
}

func (tx *Tx) Rebind(query string) string {
	return tx.tx.Rebind(query)
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Unsafe() *Tx {
	unsafe := tx.tx.Unsafe()
	return &Tx{tx: unsafe, errorLog: tx.errorLog, debugLog: tx.debugLog, slowLog: tx.slowLog, slowLogMin: tx.slowLogMin, logAsync: tx.logAsync}
}

func (tx *Tx) SkipLog() {
	tx.skipLog = true
}

func (tx *Tx) NamedSelect(dest interface{}, query string, arg interface{}) error {
	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}

	return tx.Select(dest, query, args...)
}

func (tx *Tx) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return namedExec(tx, query, arg)
}

func (tx *Tx) NamedInsert(target interface{}, tableName string, paramNames []string, m map[string]interface{}) (string, []interface{}, error) {
	return namedInsert(target, tableName, paramNames, m)
}

func (tx *Tx) getDB() Querier {
	return tx.tx
}

func (tx *Tx) logQuery(query string, execTime time.Duration, err error, args ...interface{}) error {
	if tx.logAsync == true {
		go tx.log(query, execTime, err, args...)
		return nil
	}

	return tx.log(query, execTime, err, args...)
}

func (tx *Tx) log(query string, execTime time.Duration, err error, args ...interface{}) error {
	if tx.skipLog == true {
		tx.skipLog = false
		return nil
	}

	if err != nil && tx.errorLog != nil {
		if err2 := logMsg(tx.errorLog, LevelError, query, execTime, errors.WithStack(err)); err2 != nil {
			return err2
		}
	}

	if execTime >= tx.slowLogMin && tx.slowLog != nil {
		if err3 := logMsg(tx.slowLog, LevelSlowQuery, query, execTime, nil, args...); err3 != nil {
			return err3
		}
	}

	if tx.debugLog != nil {
		if err4 := logMsg(tx.debugLog, LevelDebug, query, execTime, nil, args...); err4 != nil {
			return err4
		}
	}

	return nil
}
