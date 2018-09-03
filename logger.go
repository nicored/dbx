package dbx

import (
	"encoding/json"
	"io"
	"log"
	"regexp"
	"time"

	"fmt"

	"github.com/pkg/errors"
)

const (
	LogError = 1 << iota
	LogDebug
	LogSLow

	DefaultSlowLogMin = 1 * time.Second
)

type qLog struct {
	Level    string
	Time     time.Time
	Query    string        `json:"query"`
	ExecTime time.Duration `json:"exec_time_ns"`
	Error    string        `json:"error_msg,omitempty"`
	Trace    string        `json:"trace,omitempty"`
	Args     []interface{} `json:"args,omitempty"`
}

var regSpaceTrim *regexp.Regexp

func (dbx *DBX) SetLogger(logType int8, out io.Writer) error {
	if logType != LogError && logType != LogDebug && logType != LogSLow {
		return errors.New("given log type doesn't exist")
	}

	newLogger := log.New(out, "", log.LUTC)

	switch logType {
	case LogError:
		newLogger.SetPrefix("")
		dbx.errorLog = newLogger
	case LogDebug:
		newLogger.SetPrefix("")
		dbx.debugLog = newLogger
	case LogSLow:
		newLogger.SetPrefix("")
		dbx.slowLog = newLogger
	}

	return nil
}

func (dbx *DBX) SetSlowLogMin(minDur time.Duration) {
	dbx.slowLogMin = minDur
}

func (dbx *DBX) SetLoggerAsync(async bool) {
	dbx.logAsync = async
}

func logMsg(logger *log.Logger, level string, query string, execTime time.Duration, err error, args ...interface{}) error {
	msg, parseErr := parseQuery(level, query, execTime, err, args...)
	if parseErr != nil {
		return parseErr
	}

	logger.Println(string(msg))
	return nil
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func parseQuery(level string, query string, dur time.Duration, err error, args ...interface{}) ([]byte, error) {
	query = removeComments(query)
	query = regSpaceTrim.ReplaceAllString(query, " ")

	errMsg := ""
	trace := ""
	if err != nil {
		serr := errors.Wrap(err, "")
		errMsg = serr.Error()

		if sterr, ok := serr.(stackTracer); ok {
			for i, f := range sterr.StackTrace() {
				trace += fmt.Sprintf("%+s:%d", f, i)
			}
		}
	}

	l := qLog{level, time.Now(), query, dur, errMsg, trace, args}

	lB, err := json.Marshal(l)
	if err != nil {
		return nil, err
	}

	return lB, nil
}
