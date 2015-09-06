package qb

import "testing"

func BenchmarkPqFilter(b *testing.B) {
	var fields = []filter{
		{field: "id", op: "=", value: "id1"},
		{field: "name", op: "=", value: "id1"},
		{field: "joindate", op: ">=", value: "01/10/2010"},
		{field: "age", op: "=", value: "10"},
		{field: "descr", op: "=", value: "desc"},
	}
	for i := 0; i < b.N; i++ {
		pqFilter(fields)
	}
}
