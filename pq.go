package qb

import (
	"strconv"
	"strings"
)

func pqWhere(fields []string, starting int) (query string, next int) {
	query, next = pqFieldsWithPlaceholder(fields, starting)
	query = " WHERE " + query
	return
}

func pqUpdateSet(fvs []fieldValue, starting int) (query string, args []interface{}, next int) {
	w := make([]string, len(fvs))
	for i, fv := range fvs {
		w[i] = fv.field + " = $" + strconv.Itoa(starting)
		args = append(args, fv.value)
		starting++
	}
	query = strings.Join(w, " AND ")
	next = starting
	query = " SET " + query
	return
}

func pqFieldsWithPlaceholder(fields []string, starting int) (query string, next int) {
	w := make([]string, len(fields))
	for i, field := range fields {
		w[i] = field + " = $" + strconv.Itoa(starting)
		starting++
	}
	query = strings.Join(w, " AND ")
	next = starting
	return
}

func pqFilter(filters []filter, starting int) (where string, args []interface{}, next int) {
	if len(filters) == 0 {
		return
	}
	args = make([]interface{}, len(filters))
	w := make([]string, len(filters))
	for i, filter := range filters {
		w[i] = filter.field + " " + filter.op + " $" + strconv.Itoa(starting)
		args[i] = filter.value
		starting++
	}
	next = starting
	where = " WHERE " + strings.Join(w, " AND ")
	return where, args, next
}

func pqMakePlaceholder(n int) string {
	p := make([]string, n)
	for i := 0; i < n; i++ {
		p[i] = "$" + strconv.Itoa(i+1)
	}
	return strings.Join(p, ",")
}
