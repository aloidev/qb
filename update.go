package qb

import (
	"fmt"
	"strings"
)

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

//Set set the field to be updated.
func (u *Update) Set(field string, value interface{}) error {
	field = strings.ToLower(field)
	if !isFieldExist(u.t, field) {
		return fmt.Errorf("field %s doesn't exist on table %s", field, u.t.TableName())
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

func (u *Update) SetFilter(field, op string, value interface{}) *Update {
	f := filter{
		field: strings.ToLower(field),
		op:    op,
		value: value,
	}
	u.filters = append(u.filters, f)
	return u
}

//Query return the query and the args to execute again the database.
//After query create, the update will do a reset it's state to be reuse.
func (u *Update) Query() (query string, args []interface{}) {
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

//InsertQuery Return a query to insert to the database.
func (u *Update) InsertQuery() string {
	n := len(u.t.Fields())
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		u.t.TableName(),
		strings.Join(u.t.Fields(), ","),
		u.makePlaceholder(n))
	return query
}

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
