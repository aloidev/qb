package qb

import (
	"reflect"
	"testing"
)

func TestUpdateQuery(t *testing.T) {
	type Emp struct {
		ID   string `pk:"1"`
		Name string
		Age  int
	}
	b := newUpdateBuilder(t, Emp{})
	b.Set("name", "al")
	wantQ := "UPDATE emp SET name = $1"
	wantA := []interface{}{"al"}
	testUpdateQuery(t, b, wantQ, wantA)
	b.Set("name", "al")
	b.SetFilter("id", "=", "i8")
	wantQ = "UPDATE emp SET name = $1 WHERE id = $2"
	wantA = []interface{}{"al", "i8"}
	testUpdateQuery(t, b, wantQ, wantA)
}

func TestInsertQuery(t *testing.T) {
	type Emp struct {
		ID   string `pk:"1"`
		Name string
		Age  int
	}
	b := newUpdateBuilder(t, Emp{})
	got := b.InsertQuery()
	want := "INSERT INTO emp (id,name,age) VALUES ($1,$2,$3)"
	if got != want {
		t.Errorf("got: %s want %s", got, want)
	}
}

func TestDeleteQuery(t *testing.T) {
	type Emp struct {
		ID   string `pk:"1"`
		Name string
		Age  int
	}
	b := newUpdateBuilder(t, Emp{})
	wantQ := "DELETE FROM emp"
	var wantA []interface{}
	testDeleteQuery(t, b, wantQ, wantA)
	b.SetFilter("id", "=", "i8")
	wantQ = "DELETE FROM emp WHERE id = $1"
	wantA = []interface{}{"i8"}
	testDeleteQuery(t, b, wantQ, wantA)
}

func testDeleteQuery(t *testing.T, b *Update, wantQ string, wantA []interface{}) {
	gotQ, gotA := b.DeleteQuery()
	if gotQ != wantQ {
		t.Errorf("got: %s\n        want %s", gotQ, wantQ)
	}
	if !reflect.DeepEqual(gotA, wantA) {
		t.Errorf("got: %v \n want %v", gotA, wantA)
	}
}

func testUpdateQuery(t *testing.T, b *Update, wantQ string, wantA []interface{}) {
	gotQ, gotA := b.Query()
	if gotQ != wantQ {
		t.Errorf("got: %s\n        want %s", gotQ, wantQ)
	}
	if !reflect.DeepEqual(gotA, wantA) {
		t.Errorf("got: %v \n want %v", gotA, wantA)
	}
}

func newUpdateBuilder(t *testing.T, tbl interface{}) *Update {
	ti, err := NewTable("", tbl)
	if err != nil {
		t.Errorf("got err : %v want nil", err)
	}

	b := NewPQUpdate(ti)
	return b
}
