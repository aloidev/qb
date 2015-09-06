//Package qb is simple library to construct SQL Query.
//Supported filter of are :
//	"=" 	equal
//	">"		greater than
//	"<"		less than
//	">=" 	greater than and equal
//	"<="	less than and equal
package qb

import (
	"fmt"
	"strings"
)

//Tabler is an interface that represent table for sql.
type Tabler interface {
	TableName() string
	//Fields return a fields for the Table.
	Fields() []string
	PrimaryKeys() []string
}

type filter struct {
	field string
	op    string
	value interface{}
}

//Builder is
type Builder struct {
	explicit bool
	t        Tabler
	driver   string
	fields   []string
	orderBy  []string
	filters  []filter
}

//NewPQ create a builder for PostgreSQL database.
//If explicit, when fields is not specified the builder will return query with the fields,
//instead of *.
func NewPQ(t Tabler, explicit bool) *Builder {
	return &Builder{explicit: explicit, t: t, driver: "pq"}
}

//SetFields set the fields to retrieve from table.
func (b *Builder) SetFields(fieldName ...string) *Builder {
	for _, field := range fieldName {
		b.fields = append(b.fields, strings.ToLower(field))
	}
	return b
}

//SetFilter set the where clause for the query, filter will be AND with other filter.
func (b *Builder) SetFilter(fieldName string, op string, value interface{}) *Builder {
	f := filter{
		field: strings.ToLower(fieldName),
		op:    op,
		value: value,
	}
	b.filters = append(b.filters, f)
	return b
}

//SetRange is a convenient wrapper for setfilter.
//SetRange is equivalent with 2 SetFilter call:
//	SetFilter(field,">=",starting)
//	SetFilter(field,"<=",ending)
func (b *Builder) SetRange(fieldName string, starting, ending interface{}) *Builder {
	b.SetFilter(fieldName, ">=", starting)
	b.SetFilter(fieldName, "<=", ending)
	return b
}

//OrderBy set the order by for the query.
//If OrderBy is not called the query will orderBy primaryKey fields.
func (b *Builder) OrderBy(fieldName ...string) *Builder {
	b.orderBy = fieldName
	return b
}

//SelectAll return a query to select all data from sql.
func (b *Builder) SelectAll() string {
	query := b.initialQuery() + " " + b.orderByQuery()
	return query
}

//SelectByPK return a query with the where clause from PrimaryKey.
func (b *Builder) SelectByPK() string {
	query := b.initialQuery() + " " + b.pkWhereQuery()
	return query
}

func (b *Builder) initialQuery() string {
	if len(b.fields) == 0 {
		if !b.explicit {
			return "SELECT * FROM " + b.t.TableName()
		}
		b.fields = b.t.Fields()
	}
	query := "SELECT " + strings.Join(b.fields, ",") + " FROM " + b.t.TableName()
	return query
}

func (b *Builder) orderByQuery() string {
	b.orderBy = append(b.orderBy, b.t.PrimaryKeys()...)
	orderBy := "ORDER BY " + strings.Join(b.orderBy, ",")
	return orderBy
}

func (b *Builder) pkWhereQuery() string {
	if b.driver == "pq" {
		return pqWhere(b.t.PrimaryKeys())
	}
	return "pkWhereQuery: should not happen"
}

func (b *Builder) filterQuery() (where string, args []interface{}) {
	if b.driver == "pq" {
		return pqFilter(b.filters)
	}
	return "filterQuery: should not happen", nil
}

//Error check the query
func (b *Builder) Error() error {
	if err := b.fieldError(); err != nil {
		return err
	}
	if err := b.orderByError(); err != nil {
		return err
	}
	if err := b.filterError(); err != nil {
		return err
	}
	return nil
}

//Query return a query without checking the error.
func (b *Builder) Query() (query string, args []interface{}) {
	where, args := b.filterQuery()
	query = b.initialQuery() + " " + where
	return query, args
}

//Reset zeroes the fields,filters and orderBy so the builder can be reuse to construct new query
//without include the old fields,filters and orderBy.
func (b *Builder) Reset() *Builder {
	b.fields = []string{}
	b.filters = []filter{}
	b.orderBy = []string{}
	return b
}

func (b *Builder) fieldError() error {
	for _, field := range b.fields {
		if !b.fieldExist(field) {
			return fmt.Errorf("field %s doesn't exist", field)
		}
	}
	return nil
}

func (b *Builder) orderByError() error {
	for _, field := range b.orderBy {
		if !b.fieldExist(field) {
			return fmt.Errorf("orderBy field %s doesn't exist", field)
		}
	}
	return nil
}

func (b *Builder) filterError() error {
	for _, filter := range b.filters {
		if !b.fieldExist(filter.field) {
			return fmt.Errorf("field %s doesn't exist", filter.field)
		}
		if err := b.isValidOp(filter.op); err != nil {
			return err
		}
	}
	return nil
}

func (b *Builder) fieldExist(field string) bool {
	for _, val := range b.t.Fields() {
		if val == field {
			return true
		}
	}
	return false
}

func (b *Builder) isValidOp(op string) error {
	if op == "=" || op == "<" || op == ">" || op == ">=" || op == "<=" {
		return nil
	}
	return fmt.Errorf("filter op %s is not supported", op)
}

//QueryMust will panic when builder return an error.
func (b *Builder) QueryMust() (query string, args []interface{}) {
	if err := b.Error(); err != nil {
		panic(err)
	}
	return b.Query()
}
