package dbx

import (
	"time"

	"database/sql"

	"fmt"
	"reflect"

	"strings"

	"regexp"

	"github.com/jmoiron/sqlx"
)

var regCmts = regexp.MustCompile("(--.*)(\\n)")

func queryX(querier dbxInternal, query string, args ...interface{}) (*sqlx.Rows, error) {
	query = querier.getDB().Rebind(query)

	timeStart := time.Now()

	rows, err := querier.getDB().Queryx(query, args...)

	querier.logQuery(query, time.Now().Sub(timeStart), err, args...)

	return rows, err
}

func queryRowx(querier dbxInternal, query string, args ...interface{}) *sqlx.Row {
	query = querier.getDB().Rebind(query)

	timeStart := time.Now()

	row := querier.getDB().QueryRowx(query, args...)

	querier.logQuery(query, time.Now().Sub(timeStart), row.Err(), args...)

	return row
}

func selectX(querier dbxInternal, dest interface{}, query string, args ...interface{}) error {
	query = querier.getDB().Rebind(query)

	timeStart := time.Now()

	err := querier.getDB().Select(dest, query, args...)

	querier.logQuery(query, time.Now().Sub(timeStart), err, args...)

	return err
}

func exec(querier dbxInternal, query string, args ...interface{}) (sql.Result, error) {
	query = querier.getDB().Rebind(query)

	timeStart := time.Now()

	res, err := querier.getDB().Exec(query, args...)

	querier.logQuery(query, time.Now().Sub(timeStart), err, args...)

	return res, err
}

func namedExec(querier dbxInternal, query string, arg interface{}) (sql.Result, error) {
	query = querier.getDB().Rebind(query)

	timeStart := time.Now()

	res, err := querier.getDB().NamedExec(query, arg)

	querier.logQuery(query, time.Now().Sub(timeStart), err, arg)

	return res, err
}

// namedInsert generates the query and arguments for an insert
// target can either be a slice, ptr to a slice, struct, ptr to a struct
// params is the list of params/columns to update
// valueMapper holds the values you want to override. When the process is done, valueMapper will list all
// generated params with their values as well as all original parameters
func namedInsert(target interface{}, tableName string, params []string, valueMapper map[string]interface{}) (string, []interface{}, error) {
	var err error
	var valuesPart string
	value := reflect.ValueOf(target)

	if valueMapper == nil {
		valueMapper = map[string]interface{}{}
	}

	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return "", nil, &NilPointerErr{"nil pointer passed to namedInsert target"}
		}

		value = reflect.Indirect(value)
	}

	if value.Kind() == reflect.Struct {
		valuesPart, err = buildInsertQueryValues(0, value.Interface(), params, valueMapper)
		if err != nil {
			return "", nil, err
		}
	} else if value.Kind() == reflect.Slice {
		valuesPart, err = namedInsertSlice(value, params, valueMapper)
		if err != nil {
			return "", nil, err
		}
	} else {
		return "", nil, &WrongTypeErr{"target type not accepted"}
	}

	q := fmt.Sprintf("Insert into %s (%s) Values %s", tableName, strings.Join(params, ","), valuesPart)

	return sqlx.Named(q, valueMapper)
}

func namedInsertSlice(sliceValue reflect.Value, paramNames []string, m map[string]interface{}) (string, error) {
	var valuesPart string

	if sliceValue.Len() == 0 {
		return "", &EmptySliceErr{"target slice is empty"}
	}

	for i := 0; i < sliceValue.Len(); i++ {
		itemValue := sliceValue.Index(i)

		if sliceValue.Index(i).Kind() == reflect.Ptr {
			if itemValue.IsNil() {
				return "", &NilPointerErr{"nil pointer passed to namedInsertSlice target"}
			}

			itemValue = itemValue.Elem()
		}

		qValues, err := buildInsertQueryValues(i, itemValue.Interface(), paramNames, m)
		if err != nil {
			return "", err
		}

		valuesPart += qValues
	}

	return valuesPart, nil
}

// buildInsertQueryValues returns the values part query for a struct. eg. (id_0, name_0, timestamp_0)
func buildInsertQueryValues(index int, target interface{}, paramNames []string, m map[string]interface{}) (string, error) {
	value := reflect.ValueOf(target)

	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return "", &NilPointerErr{"nil pointer passed to buildInsertValues target"}
		}

		value = value.Elem()
	}

	tagMap, err := mapTypeFieldsTag("db", reflect.TypeOf(value.Interface()))
	if err != nil {
		return "", err
	}

	var values []interface{}

	for _, param := range paramNames {
		if _, ok := m[param]; ok {
			values = append(values, m[param])
			continue
		}

		f := value.FieldByName(param)
		if f.Kind() != reflect.Invalid {
			values = append(values, value.FieldByName(param).Interface())
			continue
		}

		if field, ok := tagMap[param]; ok {
			values = append(values, value.FieldByName(field.Name).Interface())
			continue
		}

		return "", &MissingParamErr{error: fmt.Sprintf("param '%s' not found", param)}
	}

	return BuildInsertParams(m, index, paramNames, values), nil
}

// mapTypeFieldsTag values of tags of all fields in the given struct Type
func mapTypeFieldsTag(tagName string, t reflect.Type) (map[string]reflect.StructField, error) {
	if t.Kind() != reflect.Struct {
		return nil, &WrongTypeErr{"not a struct"}
	}

	m := map[string]reflect.StructField{}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		v, ok := field.Tag.Lookup(tagName)
		if ok {
			m[v] = field
		}
	}

	return m, nil
}

func ReturningAll(query string) string {
	return query + " Returning *"
}

func ReturningID(query string) string {
	return query + " Returning id"
}

func ReturningCustom(query string, fields []string) string {
	return query + " Returning " + strings.Join(fields, ",")
}

func removeComments(query string) string {
	return regCmts.ReplaceAllString(query, "")
}
