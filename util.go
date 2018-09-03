package dbx

import (
	"strconv"
	"strings"
	"os"
	"runtime"
)

func BuildInsertParams(m map[string]interface{}, rowIt int, params []string, values []interface{}) string {
	out := "("

	it := strconv.Itoa(rowIt)
	for pi, p := range params {
		if pi > 0 {
			out += ","
		}

		p += "_" + it
		out += ":" + p

		m[p] = values[pi]
	}
	out += ")"

	if rowIt > 0 {
		out = "," + out
	}

	return out
}

const homeEnv = "HOME"
func parsePath(path string) string {
	index := strings.Index(path, "~")
	if index < 0 {
		return path
	}

	homeDir, _ := os.LookupEnv(homeEnv)

	switch runtime.GOOS {
	case "linux":
		fallthrough
	case "darwin":
		return strings.Replace(path, "~", homeDir, -1)
	}

	return path
}

