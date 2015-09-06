package qb

import "fmt"

var pqPlaceholder = "$"

func pqWhere(fields []string) string {
	where := fmt.Sprintf("WHERE %s = %s1", fields[0], pqPlaceholder)
	if len(fields) == 1 {
		return where
	}
	n := 2
	for _, field := range fields[1:] {
		where += fmt.Sprintf(" AND %s = %s%d", field, pqPlaceholder, n)
		n++
	}
	return where
}
