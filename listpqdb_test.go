package qb

import (
	"reflect"
	"sync"
	"testing"
	"time"
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
	defer deleteAllPQData(t)
	want := data[1]
	checkListNextUsingArger(t, emp, want)
	emp.Reset()
	emp.SetFilter("id", "=", "B2")
	emp.SetFields("id", "name")
	checkListNextUsingArger(t, emp, want)
}

func TestListUsingReflect(t *testing.T) {
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
	defer deleteAllPQData(t)
	want := data[1]
	checkListNextUsingReflect(t, emp, want)
}

func checkListNextUsingReflect(t *testing.T, emp *Select, want pqEmp) {
	empList := NewList(emp)
	if err := empList.Get(db); err != nil {
		t.Errorf("list get err: %v", err)
	}
	got := new(pqEmp)
	var err error
	for {
		if err = empList.Next(got); err != nil {
			break
		}
	}
	if err != nil && err != ErrDone {
		t.Error(err)
	}
	got.JoinDate = got.JoinDate.UTC()
	checkResult(t, emp, *got, want)
}

type pqEmpPtr struct {
	ID       *string `pk:"1"`
	Name     string
	Child    *int
	JoinDate *time.Time
}

func TestListUsingReflecPtr(t *testing.T) {
	if !*pqtest {
		t.Skip("to run a test for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	tbl, err := NewTable("emp", pqEmpPtr{})
	if err != nil {
		t.Errorf("newTable err: %v", err)
	}
	emp := NewPQSelect(tbl, true)
	emp.SetFilter("id", "=", "B2")
	data := preparePqTest(t)
	defer deleteAllPQData(t)
	want := data[1]
	checkListNextUsingReflectPtr(t, emp, want)
}

func checkListNextUsingReflectPtr(t *testing.T, emp *Select, want pqEmp) {
	empList := NewList(emp)
	if err := empList.Get(db); err != nil {
		t.Errorf("list get err: %v", err)
	}
	got := new(pqEmpPtr)
	got.ID = new(string)
	got.Child = new(int)
	got.JoinDate = new(time.Time)
	var err error
	for {
		if err = empList.Next(got); err != nil {
			break
		}
	}
	if err != nil && err != ErrDone {
		t.Error(err)
	}

	*got.JoinDate = got.JoinDate.UTC()
	gotT := pqEmp{
		ID:       *got.ID,
		Name:     got.Name,
		Child:    *got.Child,
		JoinDate: *got.JoinDate,
	}
	checkResult(t, emp, gotT, want)
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
			break
		}
	}
	if err != nil && err != ErrDone {
		t.Error(err)
	}
	sa.emp.JoinDate = sa.emp.JoinDate.UTC()
	got := sa.emp
	checkResult(t, emp, got, want)
}

func checkResult(t *testing.T, emp *Select, got, want pqEmp) {
	for _, field := range emp.fields {
		switch field {
		case "id":
			if !reflect.DeepEqual(got.ID, want.ID) {
				t.Errorf("got ID: %v want %v", got.ID, want.ID)
			}
		case "name":
			if !reflect.DeepEqual(got.Name, want.Name) {
				t.Errorf("got Name: %v want %v", got.Name, want.Name)
			}
		case "child":
			if !reflect.DeepEqual(got.Child, want.Child) {
				t.Errorf("got Child: %v want %v", got.Child, want.Child)
			}
		case "joindate":
			if !reflect.DeepEqual(got.JoinDate, want.JoinDate) {
				t.Errorf("got JoinDate: %v want %v", got.JoinDate, want.JoinDate)
			}
		}
	}
}

func BenchmarkScanUsingArger(b *testing.B) {
	if !*pqtest {
		b.Skip("to run a benchmark for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	preparePQBench(b)
	b.ReportAllocs()
	tbl, err := NewTable("emp", pqEmp{})
	if err != nil {
		b.Errorf("newTable err: %v", err)
	}
	emp := NewPQSelect(tbl, true)
	emp.SetFilter("id", "=", "B2")
	// _ = preparePqTest(t)
	empList := NewList(emp)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkPQScanUsingArger(b, empList)
	}
}

func benchmarkPQScanUsingArger(b *testing.B, empList *List) {
	if err := empList.Get(db); err != nil {
		b.Errorf("list get err: %v", err)
	}
	sa := new(pqEmpArger)
	var err error
	for {
		if err = empList.Next(sa); err != nil {
			break
		}
	}
	if err != nil && err != ErrDone {
		b.Error(err)
	}
}

func BenchmarkScanUsingReflect(b *testing.B) {
	if !*pqtest {
		b.Skip("to run a benchmark for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	preparePQBench(b)
	b.ReportAllocs()
	tbl, err := NewTable("emp", pqEmp{})
	if err != nil {
		b.Errorf("newTable err: %v", err)
	}
	emp := NewPQSelect(tbl, true)
	emp.SetFilter("id", "=", "B2")
	// _ = preparePqTest(t)
	empList := NewList(emp)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkPQScanUsingArger(b, empList)
	}
}

func benchmarkPQScanUsingReflec(b *testing.B, empList *List) {
	if err := empList.Get(db); err != nil {
		b.Errorf("list get err: %v", err)
	}
	sa := new(pqEmp)
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
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkGetByPKDirectScan(b *testing.B) {
	if !*pqtest {
		b.Skip("to run a benchmark for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	preparePQBench(b)
	b.ReportAllocs()
	pqe := pqEmp{}
	tbl, err := NewTable("emp", pqEmp{})
	if err != nil {
		b.Errorf("newTable err: %v", err)
	}
	emp := NewPQSelect(tbl, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := db.QueryRow(emp.SelectByPK(), "B2")
		if err := row.Scan(&pqe.ID, &pqe.Name, &pqe.Child, &pqe.JoinDate); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkGetByPK(b *testing.B) {
	if !*pqtest {
		b.Skip("to run a benchmark for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	preparePQBench(b)
	b.ReportAllocs()
	pqe := pqEmp{}
	tbl, err := NewTable("emp", pqEmp{})
	if err != nil {
		b.Errorf("newTable err: %v", err)
	}
	emp := NewPQSelect(tbl, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := emp.GetByPK(db, &pqe, "B2"); err != nil {
			b.Error(err)
		}
	}
}

var onceB sync.Once

func preparePQBench(b *testing.B) {
	data := []pqEmp{
		{"A1", "AN1", 0, newTime(2010, time.January, 1)},
		{"B2", "BN2", 1, newTime(2010, time.February, 2)},
		{"C3", "CN3", 2, newTime(2010, time.March, 3)},
		{"C4", "DN4", 3, newTime(2010, time.April, 3)},
		{"D5", "DN5", 0, newTime(2010, time.January, 1)},
		{"E6", "EN6", 1, newTime(2010, time.February, 2)},
		{"F7", "FN7", 2, newTime(2010, time.March, 3)},
		{"G8", "GN8", 3, newTime(2010, time.April, 3)},
		{"H9", "HN9", 3, newTime(2010, time.April, 3)},
	}
	onceB.Do(func() {
		openPQDB()
		createTable()
		insertPQData(b, data)
	})
}
