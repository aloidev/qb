//Package qb is simple library to construct SQL Query.
//Supported filter of are :
//	"="  equal
//	">"  greater than
//	"<"  less than
//	">=" greater than and equal
//	"<=" less than and equal
package qb

import (
	"errors"
	"fmt"
	"strconv"
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

//Select is builder to construct Select Query.
type Select struct {
	explicit bool
	t        Tabler
	driver   string
	fields   []string
	orderBy  []string
	filters  []filter
	limit    int
	offset   int
}

//NewPQSelect create a builder for PostgreSQL database.
//If explicit, when fields is not specified the builder will return query with the fields,
//instead of *.
func NewPQSelect(t Tabler, explicit bool) *Select {
	return &Select{explicit: explicit, t: t, driver: "pq"}
}

//GetByPK execute the query using qe with aargs and save the result to dst.
//dst must be pointer to struct.
func (s *Select) GetByPK(qe QueryExecer, dst interface{}, args ...interface{}) error {
	if len(args) == 0 {
		return errors.New("args is invalid")
	}
	row := qe.QueryRow(s.SelectByPK(), args...)
	err := scanWithReflection(s.t.Fields(), row, dst)
	return err
}

//GetByPKWithCursor execute the query using qe with aargs and save the result to dst.
//dst must be pointer to struct.
func (s *Select) GetByPKWithCursor(qe QueryExecer, dst interface{}, args ...interface{}) (cursor string, err error) {
	err = s.GetByPK(qe, dst, args...)
	if err != nil {
		return cursor, err
	}
	cursor, err = s.byPKCursor(qe, args...)
	return cursor, err
}

func (s *Select) byPKCursor(qe QueryExecer, args ...interface{}) (cursor string, err error) {
	c := Cursor{limit: 1, fields: s.fields, orderBy: s.orderBy}
	orderBy := s.orderByQuery()
	where, _ := s.pkWhereQuery(1)

	q := "SELECT row_number FROM (SELECT " + strings.Join(s.t.PrimaryKeys(), ",") +
		",row_number() OVER (" + orderBy + " ) FROM " + s.t.TableName() + ") as xxrn" + where
	// fmt.Println("query:", q)
	err = qe.QueryRow(q, args...).Scan(&c.offset)
	if err != nil {
		return cursor, err
	}
	c.offset--
	cursor = c.String()
	return cursor, err
}

func (s *Select) Get(qe QueryExecer) (*List, error) {
	//TODO:this error check could be skipped, as driver or the database would check.
	if err := s.Error(); err != nil {
		return nil, err
	}
	query, args := s.Query()
	rows, err := qe.Query(query, args...)
	if err != nil {
		return nil, err
	}
	l := &List{s: s, rows: rows}
	return l, nil
}

//GetNext get the next row based on the cursor save it to dst.
func (s *Select) GetNext(qe QueryExecer, dst interface{}, cursor string) error {
	if err := s.setCursor(cursor); err != nil {
		return err
	}
	s.offset += s.limit
	query, args := s.Query()
	row := qe.QueryRow(query, args...)
	err := scanWithReflection(s.t.Fields(), row, dst)
	return err
}

//GetNext get the next row based on the cursor save it to dst.
func (s *Select) GetPrevious(qe QueryExecer, dst interface{}, cursor string) error {
	if err := s.setCursor(cursor); err != nil {
		return err
	}
	s.offset -= s.limit
	query, args := s.Query()
	row := qe.QueryRow(query, args...)
	err := scanWithReflection(s.t.Fields(), row, dst)
	return err
}

func (s *Select) GetLast(qe QueryExecer, dst interface{}, cursor string) error {
	if err := s.getLast(qe, cursor); err != nil {
		return err
	}
	query, args := s.Query()
	row := qe.QueryRow(query, args...)
	err := scanWithReflection(s.t.Fields(), row, dst)
	return err
}

func (s *Select) getLast(qe QueryExecer, cursor string) error {
	if err := s.setCursor(cursor); err != nil {
		return err
	}
	count, err := s.getCount(qe)
	if err != nil {
		return err
	}
	if count-s.limit <= s.offset {
		return ErrDone
	}
	s.offset = count - s.limit
	return nil
}

func (s *Select) getCount(qe QueryExecer) (int, error) {
	where, args, _ := s.filterQuery(1)
	query := "SELECT count(*) FROM " + s.t.TableName() + where
	var count int
	err := qe.QueryRow(query, args...).Scan(&count)
	return count, err
}

func (s *Select) setCursor(cursor string) error {
	c := Cursor{}
	c, err := c.Decode(cursor)
	if err != nil {
		return err
	}
	if len(c.fields) != 0 {
		s.fields = c.fields
	}
	if len(c.filters) != 0 {
		s.filters = c.filters
	}
	if len(c.orderBy) != 0 {
		s.orderBy = c.orderBy
	}
	s.limit = c.limit
	s.offset = c.offset
	return nil
}

func (s *Select) Cursor() string {
	if len(s.fields) == 0 && len(s.filters) == 0 && len(s.orderBy) == 0 &&
		s.limit <= 0 && s.offset == 0 {
		return ""
	}
	c := Cursor{
		fields:  s.fields,
		filters: s.filters,
		orderBy: s.orderBy,
		limit:   s.limit,
		offset:  s.offset,
	}
	return c.String()
}

//SetFields set the fields to retrieve from table.
func (s *Select) SetFields(fieldName ...string) *Select {
	for _, field := range fieldName {
		s.fields = append(s.fields, strings.ToLower(field))
	}
	return s
}

//SetFilter set the where clause for the query, filter will be AND with other filter.
func (s *Select) SetFilter(fieldName string, op string, value interface{}) *Select {
	f := filter{
		field: strings.ToLower(fieldName),
		op:    op,
		value: value,
	}
	s.filters = append(s.filters, f)
	return s
}

//SetRange is a convenient wrapper for setfilter.
//SetRange is equivalent with 2 SetFilter call:
//	SetFilter(field,">=",starting)
//	SetFilter(field,"<=",ending)
func (s *Select) SetRange(fieldName string, starting, ending interface{}) *Select {
	s.SetFilter(fieldName, ">=", starting)
	s.SetFilter(fieldName, "<=", ending)
	return s
}

//OrderBy set the order by for the query.
//If OrderBy is not called the query will orderBy primaryKey fields.
func (s *Select) OrderBy(fieldName ...string) *Select {
	s.orderBy = fieldName
	return s
}

//SetLimit set the limit for the query, if the limit is not specified select
//will produce a query that get all the records that match with query.
func (s *Select) SetLimit(n int) *Select {
	s.limit = n
	return s
}

//SetOffset set the offset for the query.
func (s *Select) SetOffset(n int) *Select {
	s.offset = n
	return s
}

//SelectAll return a query to select all data from sql.
func (s *Select) SelectAll() string {
	query := s.initialQuery() + s.orderByQuery()
	return query
}

//SelectByPK return a query with the where clause from PrimaryKey.
func (s *Select) SelectByPK() string {
	where, _ := s.pkWhereQuery(1)
	query := s.initialQuery() + where
	return query
}

func (s *Select) initialQuery() string {
	if len(s.fields) == 0 {
		if !s.explicit {
			return "SELECT * FROM " + s.t.TableName()
		}
		s.fields = s.t.Fields()
	}
	query := "SELECT " + strings.Join(s.fields, ",") + " FROM " + s.t.TableName()
	return query
}

func (s *Select) orderByQuery() string {
	if len(s.orderBy) == 0 {
		s.orderBy = append(s.orderBy, s.t.PrimaryKeys()...)
	} else {
		for _, v := range s.t.PrimaryKeys() {
			if !s.isOrderByExist(v) {
				s.orderBy = append(s.orderBy, v)
			}
		}
	}
	orderBy := " ORDER BY " + strings.Join(s.orderBy, ",")
	return orderBy
}

func (s *Select) isOrderByExist(field string) bool {
	for _, v := range s.orderBy {
		if field == v {
			return true
		}
	}
	return false
}

func (s *Select) pkWhereQuery(starting int) (string, int) {
	if s.driver == "pq" {
		return pqWhere(s.t.PrimaryKeys(), starting)
	}
	return "pkWhereQuery: unreacheable", 0
}

func (s *Select) filterQuery(starting int) (where string, args []interface{}, next int) {
	if s.driver == "pq" {
		return pqFilter(s.filters, starting)
	}
	return
}

//Error check the query
func (s *Select) Error() error {
	if err := s.fieldError(); err != nil {
		return err
	}
	if err := s.orderByError(); err != nil {
		return err
	}
	if err := s.filterError(); err != nil {
		return err
	}
	return nil
}

//Query return a query without checking the error.
func (s *Select) Query() (query string, args []interface{}) {
	where, args, _ := s.filterQuery(1)
	query = s.initialQuery() + where + s.orderByQuery()
	if s.limit > 0 {
		query += " LIMIT " + strconv.Itoa(s.limit)
	}
	if s.offset > 0 {
		query += " OFFSET " + strconv.Itoa(s.offset)
	}
	return query, args
}

//Reset zeroes the fields,filters and orderBy so the builder can be reuse to construct new query
//without include the old fields,filters and orderBy.
func (s *Select) Reset() *Select {
	s.fields = []string{}
	s.filters = []filter{}
	s.orderBy = []string{}
	s.limit = 0
	s.offset = 0
	return s
}

func (s *Select) makePlaceholder(n int) string {
	if s.driver == "pq" {
		return pqMakePlaceholder(n)
	}
	return "makePlaceholder: should not happen"
}

func (s *Select) fieldError() error {
	for _, field := range s.fields {
		if !s.fieldExist(field) {
			return fmt.Errorf("field %s doesn't exist", field)
		}
	}
	return nil
}

func (s *Select) orderByError() error {
	for _, field := range s.orderBy {
		if !s.fieldExist(field) {
			return fmt.Errorf("orderBy field %s doesn't exist", field)
		}
	}
	return nil
}

func (s *Select) filterError() error {
	for _, filter := range s.filters {
		if !s.fieldExist(filter.field) {
			return fmt.Errorf("field %s doesn't exist", filter.field)
		}
		if err := s.isValidOp(filter.op); err != nil {
			return err
		}
	}
	return nil
}

func (s *Select) fieldExist(field string) bool {
	for _, val := range s.t.Fields() {
		if val == field {
			return true
		}
	}
	return false
}

func (s *Select) isValidOp(op string) error {
	if op == "=" || op == "<" || op == ">" || op == ">=" || op == "<=" {
		return nil
	}
	return fmt.Errorf("filter op %s is not supported", op)
}

//QueryMust will panic when builder return an error.
func (s *Select) QueryMust() (query string, args []interface{}) {
	if err := s.Error(); err != nil {
		panic(err)
	}
	return s.Query()
}
