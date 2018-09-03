package dbx

import (
	"regexp"
)

const (
	qTypePage   = 1
	qTypeOffset = 2
)

var regEndSemiCol = regexp.MustCompile("(;\\s*)$")

type QueryOption struct {
	qType  int32
	page   int
	offset int
	limit  int
}

func NewPageOption(page int, length int) QueryOption {
	offset := getOffsetFromPageAndLimit(page, length, 25)

	return QueryOption{
		qType:  qTypePage,
		page:   page,
		limit:  length,
		offset: offset,
	}
}

func NewOffsetOption(offset int, limit int) QueryOption {
	page := getPageFromOffsetAndLimit(offset, limit, 25)

	return QueryOption{
		qType:  qTypePage,
		page:   page,
		limit:  limit,
		offset: offset,
	}
}

func (qo QueryOption) IncPage(inc int) {
	qo.page += inc
	qo.offset = getOffsetFromPageAndLimit(qo.page, qo.limit, 25)
}

func (qo QueryOption) IncOffset(inc int) {
	qo.offset += inc
	qo.page = getPageFromOffsetAndLimit(qo.offset, qo.limit, 25)
}

func (qo QueryOption) Page() int {
	return qo.page
}

func WithOptions(option []QueryOption, query string, args ...interface{}) (string, []interface{}) {
	if len(option) == 0 {
		return query, args
	}

	query = trimSemiColumn(query)

	query += " Offset ? Limit ?"
	args = append(args, option[0].offset, option[0].limit)

	return query, args
}

func trimSemiColumn(query string) string {
	return regEndSemiCol.ReplaceAllString(query, "")
}

func getOffsetFromPageAndLimit(page, limit, defaultLimit int) int {
	if page < 1 {
		page = 1
	}

	if limit < 1 {
		limit = defaultLimit
	}

	return (page - 1) * limit
}

func getPageFromOffsetAndLimit(offset, limit, defaultLimit int) int {
	if offset < 1 || offset <= defaultLimit {
		return 1
	}

	return (offset + limit) / limit
}
