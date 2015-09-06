package qb

import (
	"strconv"
	"strings"
)

func pqWhere(fields []string) string {
	w := make([]string, len(fields))
	for i, field := range fields {
		w[i] = field + " = $" + strconv.Itoa(i+1)
	}
	where := "WHERE " + strings.Join(w, " AND ")
	return where
}

func pqFilter(filters []filter) (where string, args []interface{}) {
	if len(filters) == 0 {
		return
	}
	args = make([]interface{}, len(filters))
	w := make([]string, len(filters))
	for i, filter := range filters {
		w[i] = filter.field + " " + filter.op + " $" + strconv.Itoa(i+1)
		args[i] = filter.value
	}
	where = "WHERE " + strings.Join(w, " AND ")
	return where, args
}

func pqMakePlaceholder(n int) string {
	p := make([]string, n)
	for i := 0; i < n; i++ {
		p[i] = "$" + strconv.Itoa(i+1)
	}
	return strings.Join(p, ",")
}
