package dbx

import (
	"testing"

	"fmt"

	"github.com/jackc/pgx/pgtype"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func Test_namedInsert_WithSliceOfStructs(t *testing.T) {
	type person struct {
		ID       int         `db:"id"`
		Name     string      `db:"name"`
		Location string      `db:"location"`
		IsAlive  bool        `db:"is_alive"`
		Token    pgtype.UUID `db:"user_token"`
	}

	tableName := "person"

	token := pgtype.UUID{}
	err := token.Set(uuid.NewV4().String())
	require.NoError(t, err)

	cases := []struct {
		testName      string
		data          []person
		paramNames    []string
		expectedQuery string
		expectedArgs  []interface{}
		mapValues     map[string]interface{}
		expectedErr   error
	}{
		{
			testName: "Test with a single row",
			data: []person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
			},
			paramNames:    []string{"name", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "Insert into person (name,is_alive,user_token) Values (?,?,?)",
			expectedArgs:  []interface{}{"Alpha", true, token},
			expectedErr:   nil,
		},
		{
			testName: "Test with multiple rows",
			data: []person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
				{ID: 2, Name: "Beta", Location: "France", IsAlive: false, Token: token},
			},
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "Insert into person (name,location,is_alive,user_token) Values (?,?,?,?),(?,?,?,?)",
			expectedArgs:  []interface{}{"Alpha", "Australia", true, token, "Beta", "France", false, token},
			expectedErr:   nil,
		},
		{
			testName: "Test by field name instead of tag",
			data: []person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
			},
			paramNames:    []string{"Name", "IsAlive", "Token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "Insert into person (Name,IsAlive,Token) Values (?,?,?)",
			expectedArgs:  []interface{}{"Alpha", true, token},
			expectedErr:   nil,
		},
		{
			testName: "Test override struct value with mapper value",
			data: []person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
				{ID: 2, Name: "Beta", Location: "France", IsAlive: false, Token: token},
			},
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{"location": "China"},
			expectedQuery: "Insert into person (name,location,is_alive,user_token) Values (?,?,?,?),(?,?,?,?)",
			expectedArgs:  []interface{}{"Alpha", "China", true, token, "Beta", "China", false, token},
			expectedErr:   nil,
		},
		{
			testName: "Test with invalid param should return MissingParamErr",
			data: []person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
				{ID: 2, Name: "Beta", Location: "France", IsAlive: false, Token: token},
			},
			paramNames:    []string{"name", "location", "is_alive", "user_token", "unknown_param"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "",
			expectedArgs:  nil,
			expectedErr:   &MissingParamErr{"param 'unknown_param' not found"},
		},
		{
			testName:      "Test with empty slice",
			data:          []person{},
			paramNames:    []string{"name", "location", "is_alive", "user_token", "unknown_param"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "",
			expectedArgs:  nil,
			expectedErr:   &EmptySliceErr{"target slice is empty"},
		},
	}

	for _, c := range cases {
		query, args, err := namedInsert(c.data, tableName, c.paramNames, c.mapValues)
		if c.expectedErr != nil {
			require.EqualValues(t, c.expectedErr, err)
		} else if err != nil {
			require.NoError(t, err, c.testName)
		}

		require.Equal(t, c.expectedQuery, query, c.testName)
		require.EqualValues(t, c.expectedArgs, args, c.testName)
	}
}

func Test_namedInsert_WithSlicePtrOfStructs(t *testing.T) {
	type person struct {
		ID       int         `db:"id"`
		Name     string      `db:"name"`
		Location string      `db:"location"`
		IsAlive  bool        `db:"is_alive"`
		Token    pgtype.UUID `db:"user_token"`
	}

	tableName := "person"

	token := pgtype.UUID{}
	err := token.Set(uuid.NewV4().String())
	require.NoError(t, err)

	cases := []struct {
		testName      string
		data          *[]person
		paramNames    []string
		expectedQuery string
		expectedArgs  []interface{}
		mapValues     map[string]interface{}
		expectedErr   error
	}{
		{
			testName: "Test with a single row",
			data: &[]person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
			},
			paramNames:    []string{"name", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "Insert into person (name,is_alive,user_token) Values (?,?,?)",
			expectedArgs:  []interface{}{"Alpha", true, token},
			expectedErr:   nil,
		},
		{
			testName: "Test with multiple rows",
			data: &[]person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
				{ID: 2, Name: "Beta", Location: "France", IsAlive: false, Token: token},
			},
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "Insert into person (name,location,is_alive,user_token) Values (?,?,?,?),(?,?,?,?)",
			expectedArgs:  []interface{}{"Alpha", "Australia", true, token, "Beta", "France", false, token},
			expectedErr:   nil,
		},
		{
			testName: "Test with override struct value with mapper value",
			data: &[]person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
				{ID: 2, Name: "Beta", Location: "France", IsAlive: false, Token: token},
			},
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{"location": "China"},
			expectedQuery: "Insert into person (name,location,is_alive,user_token) Values (?,?,?,?),(?,?,?,?)",
			expectedArgs:  []interface{}{"Alpha", "China", true, token, "Beta", "China", false, token},
			expectedErr:   nil,
		},
		{
			testName:      "Test with nil slice ptr",
			data:          nil,
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "",
			expectedArgs:  nil,
			expectedErr:   &NilPointerErr{"nil pointer passed to namedInsert target"},
		},
	}

	for _, c := range cases {
		query, args, err := namedInsert(c.data, tableName, c.paramNames, c.mapValues)
		if c.expectedErr != nil {
			require.EqualValues(t, c.expectedErr, err)
		}

		require.Equal(t, c.expectedQuery, query)
		require.EqualValues(t, c.expectedArgs, args)
	}
}

func Test_namedInsert_WithSliceOfStructPtrs(t *testing.T) {
	type person struct {
		ID       int         `db:"id"`
		Name     string      `db:"name"`
		Location string      `db:"location"`
		IsAlive  bool        `db:"is_alive"`
		Token    pgtype.UUID `db:"user_token"`
	}

	tableName := "person"

	token := pgtype.UUID{}
	err := token.Set(uuid.NewV4().String())
	require.NoError(t, err)

	cases := []struct {
		testName      string
		data          []*person
		paramNames    []string
		expectedQuery string
		expectedArgs  []interface{}
		mapValues     map[string]interface{}
		expectedErr   error
	}{
		{
			testName: "Test with a single row",
			data: []*person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
			},
			paramNames:    []string{"name", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "Insert into person (name,is_alive,user_token) Values (?,?,?)",
			expectedArgs:  []interface{}{"Alpha", true, token},
			expectedErr:   nil,
		},
		{
			testName: "Test with multiple rows",
			data: []*person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
				{ID: 2, Name: "Beta", Location: "France", IsAlive: false, Token: token},
			},
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "Insert into person (name,location,is_alive,user_token) Values (?,?,?,?),(?,?,?,?)",
			expectedArgs:  []interface{}{"Alpha", "Australia", true, token, "Beta", "France", false, token},
			expectedErr:   nil,
		},
		{
			testName: "Test override struct value with mapper value",
			data: []*person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
				{ID: 2, Name: "Beta", Location: "France", IsAlive: false, Token: token},
			},
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{"location": "China"},
			expectedQuery: "Insert into person (name,location,is_alive,user_token) Values (?,?,?,?),(?,?,?,?)",
			expectedArgs:  []interface{}{"Alpha", "China", true, token, "Beta", "China", false, token},
			expectedErr:   nil,
		},
		{
			testName: "Test with nil person ptr in slice",
			data: []*person{
				{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
				nil,
			},
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "",
			expectedArgs:  nil,
			expectedErr:   &NilPointerErr{"nil pointer passed to namedInsertSlice target"},
		},
	}

	db := &DBX{}
	for _, c := range cases {
		query, args, err := db.NamedInsert(c.data, tableName, c.paramNames, c.mapValues)
		if c.expectedErr != nil {
			require.EqualValues(t, c.expectedErr, err)
		}

		require.Equal(t, c.expectedQuery, query)
		require.EqualValues(t, c.expectedArgs, args)
	}
}

func Test_namedInsert_WithStruct(t *testing.T) {
	type person struct {
		ID       int         `db:"id"`
		Name     string      `db:"name"`
		Location string      `db:"location"`
		IsAlive  bool        `db:"is_alive"`
		Token    pgtype.UUID `db:"user_token"`
	}

	tableName := "person"

	token := pgtype.UUID{}
	err := token.Set(uuid.NewV4().String())
	require.NoError(t, err)

	cases := []struct {
		testName      string
		data          person
		paramNames    []string
		expectedQuery string
		expectedArgs  []interface{}
		mapValues     map[string]interface{}
		expectedErr   error
	}{
		{
			testName:      "Test a valid struct",
			data:          person{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
			paramNames:    []string{"name", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "Insert into person (name,is_alive,user_token) Values (?,?,?)",
			expectedArgs:  []interface{}{"Alpha", true, token},
			expectedErr:   nil,
		},
		{
			testName:      "Test override struct value with map value",
			data:          person{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{"location": "China"},
			expectedQuery: "Insert into person (name,location,is_alive,user_token) Values (?,?,?,?)",
			expectedArgs:  []interface{}{"Alpha", "China", true, token},
			expectedErr:   nil,
		},
	}

	for _, c := range cases {
		query, args, err := namedInsert(c.data, tableName, c.paramNames, c.mapValues)
		if c.expectedErr != nil {
			require.EqualValues(t, c.expectedErr, err)
		}

		require.Equal(t, c.expectedQuery, query)
		require.EqualValues(t, c.expectedArgs, args)
	}
}

func Test_namedInsert_WithStructPtr(t *testing.T) {
	type person struct {
		ID       int         `db:"id"`
		Name     string      `db:"name"`
		Location string      `db:"location"`
		IsAlive  bool        `db:"is_alive"`
		Token    pgtype.UUID `db:"user_token"`
	}

	tableName := "person"

	token := pgtype.UUID{}
	err := token.Set(uuid.NewV4().String())
	require.NoError(t, err)

	cases := []struct {
		testName      string
		data          *person
		paramNames    []string
		expectedQuery string
		expectedArgs  []interface{}
		mapValues     map[string]interface{}
		expectedErr   error
	}{
		{
			testName:      "Test a valid struct ptr",
			data:          &person{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
			paramNames:    []string{"name", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "Insert into person (name,is_alive,user_token) Values (?,?,?)",
			expectedArgs:  []interface{}{"Alpha", true, token},
			expectedErr:   nil,
		},
		{
			testName:      "Test override struct ptr value with mapper value",
			data:          &person{ID: 1, Name: "Alpha", Location: "Australia", IsAlive: true, Token: token},
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{"location": "China"},
			expectedQuery: "Insert into person (name,location,is_alive,user_token) Values (?,?,?,?)",
			expectedArgs:  []interface{}{"Alpha", "China", true, token},
			expectedErr:   nil,
		},
		{
			testName:      "Test with nil struct ptr",
			data:          nil,
			paramNames:    []string{"name", "location", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{"location": "China"},
			expectedQuery: "",
			expectedArgs:  nil,
			expectedErr:   &NilPointerErr{"nil pointer passed to namedInsert target"},
		},
	}

	for _, c := range cases {
		query, args, err := namedInsert(c.data, tableName, c.paramNames, c.mapValues)
		if c.expectedErr != nil {
			require.EqualValues(t, c.expectedErr, err)
		}

		require.Equal(t, c.expectedQuery, query)
		require.EqualValues(t, c.expectedArgs, args)
	}
}

func Test_namedInsert_WithSliceOfStrings(t *testing.T) {
	tableName := "person"

	token := pgtype.UUID{}
	err := token.Set(uuid.NewV4().String())
	require.NoError(t, err)

	cases := []struct {
		testName      string
		data          []string
		paramNames    []string
		expectedQuery string
		expectedArgs  []interface{}
		mapValues     map[string]interface{}
		expectedErr   error
	}{
		{
			testName:      "Test with slice of strings",
			data:          []string{"data_1", "data_2"},
			paramNames:    []string{"name", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "",
			expectedArgs:  nil,
			expectedErr:   &WrongTypeErr{"not a struct"},
		},
	}

	for _, c := range cases {
		query, args, err := namedInsert(c.data, tableName, c.paramNames, c.mapValues)
		if c.expectedErr != nil {
			require.EqualValues(t, c.expectedErr, err)
		}

		require.Equal(t, c.expectedQuery, query)
		require.EqualValues(t, c.expectedArgs, args)
	}
}

func Test_namedInsert_WithString(t *testing.T) {
	tableName := "person"

	token := pgtype.UUID{}
	err := token.Set(uuid.NewV4().String())
	require.NoError(t, err)

	cases := []struct {
		testName      string
		data          string
		paramNames    []string
		expectedQuery string
		expectedArgs  []interface{}
		mapValues     map[string]interface{}
		expectedErr   error
	}{
		{
			testName:      "Test with string",
			data:          "data_1",
			paramNames:    []string{"name", "is_alive", "user_token"},
			mapValues:     map[string]interface{}{},
			expectedQuery: "",
			expectedArgs:  nil,
			expectedErr:   &WrongTypeErr{"target type not accepted"},
		},
	}

	for _, c := range cases {
		query, args, err := namedInsert(c.data, tableName, c.paramNames, c.mapValues)
		if c.expectedErr != nil {
			require.EqualValues(t, c.expectedErr, err)
		}

		require.Equal(t, c.expectedQuery, query)
		require.EqualValues(t, c.expectedArgs, args)
	}
}

func Test_removeComments(t *testing.T) {
	query := `
	-- some comment
	select * from blah where id = ?; -- some comment here too
-- and some comment here
--
-- header cmt
	Insert into someTable (id, value) values (1, 'some value') --
	Insert into someTable (id, value) values (2, 'some value 2') --
	Insert into someTable (id, value) values -- start insert
		(3, 'v3'), -- boom
		(4, 'v4'), -- boom
		(5, 'v5'), -- boom
		(6, 'v6'), -- boom
-- some other
`

	output := removeComments(query)
	fmt.Println(output)
}
