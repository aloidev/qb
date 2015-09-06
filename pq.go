package qb

import "fmt"

var pqPlaceholder = "$"

func pqWhere(fields []string) string {
	where := fmt.Sprintf("WHERE %s = $1", fields[0])
	if len(fields) == 1 {
		return where
	}
	n := 2
	for _, field := range fields[1:] {
		where += fmt.Sprintf(" AND %s = $%d", field, n)
		n++
	}
	return where
}

func pqFilter(filters []filter) (where string, args []interface{}) {
	if len(filters) == 0 {
		return "", nil
	}

	where = fmt.Sprintf("WHERE %s %s $1", filters[0].field, filters[0].op)
	args = append(args, filters[0].value)
	n := 2
	for _, filter := range filters[1:] {
		where += fmt.Sprintf(" AND %s %s $%d", filter.field, filter.op, n)
		args = append(args, filter.value)
		n++
	}
	return where, args
}
