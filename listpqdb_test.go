package qb

import (
	"reflect"
	"testing"
)

type pqEmpArger struct {
	emp pqEmp
}

func (pq *pqEmpArger) ScanArgs(fields []string) []interface{} {
	args := make([]interface{}, len(fields))
	for i, field := range fields {
		switch field {
		case "id":
			args[i] = &pq.emp.ID
		case "name":
			args[i] = &pq.emp.Name
		case "child":
			args[i] = &pq.emp.Child
		case "joindate":
			args[i] = &pq.emp.JoinDate
		}
	}
	return args
}

func TestListNextUsingScanArger(t *testing.T) {
	if !*pqtest {
		t.Skip("to run a test for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	tbl, err := NewTable("emp", pqEmp{})
	if err != nil {
		t.Errorf("newTable err: %v", err)
	}
	emp := NewPQSelect(tbl, true)
	emp.SetFilter("id", "=", "B2")
	data := preparePqTest(t)
	want := data[1]
	checkListNextUsingArger(t, emp, want)
	emp.Reset()
	emp.SetFilter("id", "=", "B2")
	emp.SetFields("id", "name")
	checkListNextUsingArger(t, emp, want)
}

func checkListNextUsingArger(t *testing.T, emp *Select, want pqEmp) {
	empList := NewList(emp)
	if err := empList.Get(db); err != nil {
		t.Errorf("list get err: %v", err)
	}
	sa := new(pqEmpArger)
	var err error
	for {
		err = empList.Next(sa)
		if err != nil {
			if err == ErrDone {
				err = nil
			}
			break
		}
	}
	sa.emp.JoinDate = sa.emp.JoinDate.UTC()
	got := sa.emp
	for _, field := range emp.fields {
		switch field {
		case "id":
			if !reflect.DeepEqual(got.ID, want.ID) {
				t.Errorf("got ID: %v want %v", got.ID, want.ID)
			}
		case "name":
			if !reflect.DeepEqual(got.Name, want.Name) {
				t.Errorf("got Name: %v want %v", got, want)
			}
		case "child":
			if !reflect.DeepEqual(got.Child, want.Child) {
				t.Errorf("got Child: %v want %v", got, want)
			}
		case "joindate":
			if !reflect.DeepEqual(got.JoinDate, want.JoinDate) {
				t.Errorf("got JoinDate: %v want %v", got, want)
			}
		}
	}
}
