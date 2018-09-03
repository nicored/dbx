package dbx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildInsertParams(t *testing.T) {
	testCases := []struct {
		m         map[string]interface{}
		q         string
		p         []string
		v         []interface{}
		expectedQ string
		expectedM map[string]interface{}
		i         int
	}{
		{
			m:         map[string]interface{}{},
			q:         "INSERT INTO (a, b, c) VALUES ",
			p:         []string{"a", "b", "c"},
			v:         []interface{}{1, 2, 3},
			expectedQ: "INSERT INTO (a, b, c) VALUES (:a_0,:b_0,:c_0)",
			expectedM: map[string]interface{}{"a_0": 1, "b_0": 2, "c_0": 3},
			i:         0,
		},
	}

	for _, c := range testCases {
		actual := c.q + BuildInsertParams(c.m, c.i, c.p, c.v)
		require.Equal(t, c.expectedQ, actual)
		require.EqualValues(t, c.expectedM, c.m)
	}
}
