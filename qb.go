//Package qb is simple library to construct SQL Query.
//Supported filter of are :
//	"=" 	equal
//	">=" 	greater than
//	"<="	less than
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
	explicit    bool
	t           Tabler
	placeholder string
	fields      []string
	orderBy     []string
	filters     []filter
}

//NewPQ create a builder for PostgreSQL database.
//If explicit, when fields is not specified the builder will return query with the fields,
//instead of *.
func NewPQ(t Tabler, explicit bool) *Builder {
	return &Builder{explicit: explicit, t: t, placeholder: "$"}
}

//SetFilter set the where clause for the query, filter will be AND with other filter.
func (b *Builder) SetFilter(fieldName string, op string, value interface{}) *Builder {
	f := filter{
		field: fieldName,
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
	query := b.initialQuery() + " " + b.whereQuery()
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

func (b *Builder) whereQuery() string {
	pk := b.t.PrimaryKeys()
	where := fmt.Sprintf("WHERE %s = %s1", pk[0], b.placeholder)
	if len(pk) == 1 {
		return where
	}
	n := 2
	for _, field := range pk[1:] {
		where += fmt.Sprintf(" AND %s = %s%d", field, b.placeholder, n)
		n++
	}
	return where
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
func (b *Builder) Query() string {
	return ""
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

//QueryMust will panic when builder return an error.
func (b *Builder) QueryMust() string {
	if err := b.Error(); err != nil {
		panic(err)
	}
	query := b.Query()
	return query
}
