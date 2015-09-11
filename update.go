package qb

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

//DBExecer is an interface to execute query againts database.
//sql.DB and sql.Tx implement this interface.
type DBExecer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type fieldValue struct {
	field string
	value interface{}
}

//Update use track updated field and to construct the update query.
type Update struct {
	t       Tabler
	driver  string
	updated []fieldValue
	filters []filter
}

//NewPQUpdate return and update to use
func NewPQUpdate(t Tabler) *Update {
	return &Update{t: t, driver: "pq"}
}

func (u *Update) getArgs(src interface{}) []interface{} {
	args := make([]interface{}, len(u.t.Fields()))
	if tbl, ok := u.t.(Table); ok {
		rt := reflect.ValueOf(src)
		for i, idx := range tbl.fieldsIndex {
			args[i] = rt.Field(idx).Interface()
		}
		return args
	}
	//TODO:use reflection
	return args
}

//Set set the field to be updated, the field to be update can't be part of Primary keys.
func (u *Update) Set(field string, value interface{}) error {
	field = strings.ToLower(field)
	if !isFieldExist(u.t, field) {
		return fmt.Errorf("field %s doesn't exist on table %s", field, u.t.TableName())
	}
	if isPK(u.t, field) {
		return fmt.Errorf("field %s is pimary key", field)
	}
	u.set(field, value)
	return nil
}

func (u *Update) set(field string, value interface{}) {
	for i, v := range u.updated {
		if v.field == field {
			v.value = value
			u.updated[i] = v
			return
		}
	}
	fv := fieldValue{field: field, value: value}
	u.updated = append(u.updated, fv)
}

//SetFilter set filter for the query, multiple filter will be AND together.
func (u *Update) SetFilter(field, op string, value interface{}) *Update {
	f := filter{
		field: strings.ToLower(field),
		op:    op,
		value: value,
	}
	u.filters = append(u.filters, f)
	return u
}

//Update update data on the database where the value is come from call to Set method.
func (u *Update) Update(dbe DBExecer) error {
	q, args := u.UpdateQuery()
	_, err := dbe.Exec(q, args...)
	return err
}

//UpdateQuery return the query and the args to execute again the database.
//After query create, the update will do a reset it's state to be reuse.
func (u *Update) UpdateQuery() (query string, args []interface{}) {
	if len(u.updated) == 0 {
		return "", nil
	}
	next := 1
	query, args, next = u.updateSetQuery(next)
	where, argsW, _ := u.filterQuery(next)
	if len(argsW) > 0 {
		args = append(args, argsW...)
	}
	query = "UPDATE " + u.t.TableName() + query + where
	u.reset()
	return
}

//UpdateByPK update the data on the database that match with args.
func (u *Update) UpdateByPK(dbe DBExecer, args ...interface{}) error {
	query, qargs := u.UpdateByPKQuery()
	qargs = append(qargs, args...)
	_, err := dbe.Exec(query, qargs...)
	return err
}

//DeleteByPKQuery return a delete query with the where clause is set by the table Primary Keys.
func (u *Update) UpdateByPKQuery() (query string, args []interface{}) {
	if len(u.updated) == 0 {
		return "", nil
	}
	next := 1
	query, args, next = u.updateSetQuery(next)
	w, _ := u.pkWhereQuery(next)
	query = "UPDATE " + u.t.TableName() + query + w
	u.reset()
	return query, args
}

//Insert insert data to database where the value is come from src.
func (u *Update) Insert(dbe DBExecer, src interface{}) error {
	args := u.getArgs(src)
	_, err := dbe.Exec(u.InsertQuery(), args...)
	return err
}

//InsertQuery return a query to insert to the database.
func (u *Update) InsertQuery() string {
	n := len(u.t.Fields())
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		u.t.TableName(),
		strings.Join(u.t.Fields(), ","),
		u.makePlaceholder(n))
	return query
}

//DeleteByPK delete the data from database that match with the arrgs.
func (u *Update) DeleteByPK(dbe DBExecer, args ...interface{}) error {
	if len(args) != len(u.t.PrimaryKeys()) {
		return errors.New("len of args mismatch with len of Primary Keys")
	}
	_, err := dbe.Exec(u.DeleteByPKQuery(), args...)
	return err
}

//DeleteByPKQuery return a delete query with the where clause is set by the table Primary Keys.
func (u *Update) DeleteByPKQuery() string {
	w, _ := u.pkWhereQuery(1)
	query := "DELETE FROM " + u.t.TableName() + w
	return query
}

//Delete delete the data from the database that match with DeleteQuery.
func (u *Update) Delete(dbe DBExecer) error {
	query, args := u.DeleteQuery()
	_, err := dbe.Exec(query, args...)
	return err
}

//DeleteQuery return a query to delete data on the database that match the filter.
func (u *Update) DeleteQuery() (query string, args []interface{}) {
	query, args, _ = u.filterQuery(1)
	query = "DELETE FROM " + u.t.TableName() + query
	u.reset()
	return query, args
}

func (u *Update) makePlaceholder(n int) string {
	if u.driver == "pq" {
		return pqMakePlaceholder(n)
	}
	return "makePlaceholder: should not happen"
}

//starting is placeholder starting number..
func (u *Update) updateSetQuery(starting int) (query string, args []interface{}, next int) {
	if u.driver == "pq" {
		return pqUpdateSet(u.updated, starting)
	}
	return
}

func (u *Update) filterQuery(starting int) (where string, args []interface{}, next int) {
	if u.driver == "pq" {
		return pqFilter(u.filters, starting)
	}
	return
}

func (u *Update) reset() {
	u.updated = u.updated[:0]
	u.filters = u.filters[:0]
}

func isFieldExist(t Tabler, field string) bool {
	for _, v := range t.Fields() {
		if v == field {
			return true
		}
	}
	return false
}

func isPK(t Tabler, field string) bool {
	for _, v := range t.PrimaryKeys() {
		if v == field {
			return true
		}
	}
	return false
}

func (u *Update) pkWhereQuery(starting int) (string, int) {
	if u.driver == "pq" {
		return pqWhere(u.t.PrimaryKeys(), starting)
	}
	return "pkWhereQuery: unreacheable", 0
}
