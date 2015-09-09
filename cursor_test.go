package qb

import (
	"reflect"
	"testing"
)

func TestCursor(t *testing.T) {
	var tests = []Cursor{
		{
			offset: 50,
		},
		{
			limit:  10,
			offset: 50,
		},
		{
			orderBy: []string{"id", "data"},
			limit:   10,
			offset:  50,
		},
		{
			filters: []filter{
				{field: "amount", op: "=", value: "120.50"},
			},
			orderBy: []string{"id", "data"},
			limit:   10,
			offset:  50,
		},
		{
			fields: []string{"id", "amount", "date"},
			filters: []filter{
				{field: "amount", op: "=", value: "120.50"},
			},
			orderBy: []string{"id", "data"},
			limit:   10,
			offset:  50,
		},
		{
			fields: []string{"id", "amount", "date"},
			filters: []filter{
				{field: "amount", op: "=", value: "120.50"},
				// {field: "date", op: "=", value: time.Now().UTC()}, //TODO:failed on this.
			},
			limit:  10,
			offset: 50,
		},
	}
	for _, c := range tests {
		s := c.String()
		dc := Cursor{}
		got, err := dc.Decode(s)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(got, c) {
			if !reflect.DeepEqual(got.limit, c.limit) {
				t.Errorf("got limit: %#v \n    want %#v", got.orderBy, c.orderBy)
			}
			if !reflect.DeepEqual(got.offset, c.offset) {
				t.Errorf("got offset: %#v \n    want %#v", got.offset, c.offset)
			}
			if !reflect.DeepEqual(got.fields, c.fields) {
				t.Errorf("got fields: %#v want %#v", got.fields, c.fields)
			}
			if !reflect.DeepEqual(got.filters, c.filters) {
				t.Errorf("got filters: %#v \n    want %#v", got.filters, c.filters)
			}
			if !reflect.DeepEqual(got.orderBy, c.orderBy) {
				t.Errorf("got orderBy: %#v \n    want %#v", got.orderBy, c.orderBy)
			}
		}
	}
}
