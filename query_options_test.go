package dbx

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_trimSemilColumn(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "Without semi column",
			query:    "Select * from table where id = 2",
			expected: "Select * from table where id = 2",
		},
		{
			name:     "Without semi column and with spaces",
			query:    "Select * from table where id = 2     ",
			expected: "Select * from table where id = 2     ",
		},
		{
			name:     "Without semi column and with tabs",
			query:    "Select * from table where id = 2\t",
			expected: "Select * from table where id = 2\t",
		},
		{
			name:     "With semi column",
			query:    "Select * from table where id = 2;",
			expected: "Select * from table where id = 2",
		},
		{
			name:     "With semi column and spaces",
			query:    "Select * from table where id = 2    ;    ",
			expected: "Select * from table where id = 2    ",
		},
	}

	for _, test := range tests {
		actual := trimSemiColumn(test.query)
		require.Equal(t, test.expected, actual)
	}
}
