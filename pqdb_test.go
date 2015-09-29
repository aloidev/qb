package qb

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

var (
	db       *sql.DB
	pqtest   = flag.Bool("pqtest", false, "Run test for pq database")
	dbname   = flag.String("dbname", "qb_test", "Database Name")
	dbuser   = flag.String("dbuser", "", "User for Database Login")
	dbpasswd = flag.String("dbpasswd", "", "Password for Database Login")
)

type pqEmp struct {
	ID       string `pk:"1"`
	Name     string
	Child    int
	JoinDate time.Time
}

func TestSelectWithPQDatabase(t *testing.T) {
	if !*pqtest {
		t.Skip("to run a test for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	data := preparePqTest(t)
	defer deleteAllPQData(t)
	tbl, err := NewTable("emp", data[0])
	if err != nil {
		t.Errorf("create table err: %v", err)
	}
	b := NewPQSelect(tbl, true)
	b.SetFilter("id", "=", "A1")
	want := data[0]
	testSetFilterPQ(t, b, want)
	b.Reset()
	b.SetFilter("name", "=", "BN2")
	want = data[1]
	testSetFilterPQ(t, b, want)
	b.Reset()
	b.SetFilter("child", "=", 2)
	want = data[2]
	testSetFilterPQ(t, b, want)
	b.Reset()
	b.SetFilter("joinDate", "=", newTime(2010, time.April, 3))
	want = data[3]
	testSetFilterPQ(t, b, want)
}

func testSetFilterPQ(t *testing.T, b *Select, want pqEmp) {
	query, args := b.Query()
	got := queryRowPQ(t, query, args...)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v want %v", got, want)
	}
}

func queryRowPQ(t *testing.T, query string, args ...interface{}) pqEmp {
	var res pqEmp
	row := db.QueryRow(query, args...)
	err := row.Scan(&res.ID, &res.Name, &res.Child, &res.JoinDate)
	res.JoinDate = res.JoinDate.UTC()
	if err != nil {
		t.Errorf("queryRow err: %v", err)
	}
	return res
}

func TestSelectGetByPK(t *testing.T) {
	if !*pqtest {
		t.Skip("to run a test for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	data := preparePqTest(t)
	defer deleteAllPQData(t)
	emp := newPQSelectTest(t)
	want := data[0]
	got := pqEmp{}
	err := emp.GetByPK(db, &got, "A1")
	if err != nil {
		t.Error(err)
	}
	checkResult(t, emp, got, want)
}

func TestSelectGetByPKWithCursor(t *testing.T) {
	if !*pqtest {
		t.Skip("to run a test for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	data := preparePqTest(t)
	defer deleteAllPQData(t)
	emp := newPQSelectTest(t)
	start := 2
	want := data[start]
	got := pqEmp{}
	cursor, err := emp.GetByPKWithCursor(db, &got, "C3")
	if err != nil {
		t.Error(err)
	}
	checkResult(t, emp, got, want)
	// fmt.Printf("cursor: %s", cursor)
	err = emp.GetNext(db, &got, cursor)
	if err != nil {
		t.Fatalf("getNext err : %v", err)
	}
	next := start + 1
	want = data[next]
	checkResult(t, emp, got, want)
	cursor = emp.Cursor()
	err = emp.GetPrevious(db, &got, cursor)
	if err != nil {
		t.Fatalf("getPrevious err : %v", err)
	}
	want = data[start]
	checkResult(t, emp, got, want)
	cursor = emp.Cursor()
	err = emp.GetLast(db, &got, cursor)
	if err != nil {
		t.Fatalf("getLast err : %v", err)
	}
	want = data[len(data)-1]
	checkResult(t, emp, got, want)
}

//TODO:refactor this
func TestGetNext(t *testing.T) {
	data := preparePqTest(t)
	defer deleteAllPQData(t)
	emp := newPQSelectTest(t)
	// list, err := emp.SetLimit(2).Get(db)
	emp.SetLimit(2)
	list := NewList(emp)
	err := list.Get(db)
	if err != nil {
		t.Fatalf("get list err: %v", err)
	}
	// q, args := emp.Query()
	// fmt.Printf("list q : %s and args: %v", q, args)
	wants := data[:2]
	var gots []pqEmp
	got := new(pqEmp)
	for {
		if err = list.Next(got); err != nil {
			break
		}
		gots = append(gots, *got)
	}
	if err != ErrDone {
		t.Fatalf("got err: %v want ErrDone", err)
	}
	if len(gots) != len(wants) {
		t.Fatalf("gots len: %d want len %d", len(gots), len(wants))
	}
	for i, want := range wants {
		got := gots[i]
		checkResult(t, emp, got, want)
	}
	cursor := emp.Cursor()
	err = list.GetNext(db, cursor)
	if err != nil {
		t.Fatalf("list GetNext err: %v", err)
	}
	wants = data[2:4]
	gots = gots[:0]
	for {
		if err = list.Next(got); err != nil {
			break
		}
		gots = append(gots, *got)
	}
	if err != ErrDone {
		t.Fatalf("got err: %v want ErrDone", err)
	}
	if len(gots) != len(wants) {
		t.Fatalf("gots len: %d want len %d", len(gots), len(wants))
	}
	for i, want := range wants {
		got := gots[i]
		checkResult(t, emp, got, want)
	}
}

//TODO:refactor this
func TestGetLast(t *testing.T) {
	data := preparePqTest(t)
	defer deleteAllPQData(t)
	emp := newPQSelectTest(t)
	// list, err := emp.SetLimit(2).Get(db)
	emp.SetLimit(2)
	list := NewList(emp)
	err := list.Get(db)
	if err != nil {
		t.Fatalf("get list err: %v", err)
	}
	// q, args := emp.Query()
	// fmt.Printf("list q : %s and args: %v", q, args)
	wants := data[:2]
	var gots []pqEmp
	got := new(pqEmp)
	for {
		if err = list.Next(got); err != nil {
			break
		}
		gots = append(gots, *got)
	}
	if err != ErrDone {
		t.Fatalf("got err: %v want ErrDone", err)
	}
	if len(gots) != len(wants) {
		t.Fatalf("gots len: %d want len %d", len(gots), len(wants))
	}
	for i, want := range wants {
		got := gots[i]
		checkResult(t, emp, got, want)
	}
	cursor := emp.Cursor()
	err = list.GetLast(db, cursor)
	if err != nil {
		t.Fatalf("list GetNext err: %v", err)
	}
	wants = data[len(data)-2:]
	gots = gots[:0]
	for {
		if err = list.Next(got); err != nil {
			break
		}
		gots = append(gots, *got)
	}
	if err != ErrDone {
		t.Fatalf("got err: %v want ErrDone", err)
	}
	if len(gots) != len(wants) {
		t.Fatalf("gots len: %d want len %d", len(gots), len(wants))
	}
	for i, want := range wants {
		got := gots[i]
		checkResult(t, emp, got, want)
	}
}

func TestDeleteByPK(t *testing.T) {
	preparePqTest(t)
	defer deleteAllPQData(t)
	empUpdate := newPqUpdateTest(t)
	err := empUpdate.DeleteByPK(db, "A1")
	if err != nil {
		t.Errorf("delete got err: %v want nil", err)
	}
	empSelect := newPQSelectTest(t)
	got := pqEmp{}
	err = empSelect.GetByPK(db, &got, "A1")
	if err != nil && err != sql.ErrNoRows {
		t.Errorf("got err: %v want err: %v", err, sql.ErrNoRows)
	}
	want := pqEmp{}
	checkResult(t, empSelect, got, want)
}

func TestDelete(t *testing.T) {
	data := preparePqTest(t)
	defer deleteAllPQData(t)
	empUpdate := newPqUpdateTest(t)
	err := empUpdate.Delete(db)
	if err != nil {
		t.Error(err)
	}
	empSelect := newPQSelectTest(t)
	got := pqEmp{}
	for _, v := range data {
		err = empSelect.GetByPK(db, &got, v.ID)
		if err != nil && err != sql.ErrNoRows {
			t.Error(err)
		}
		want := pqEmp{}
		checkResult(t, empSelect, got, want)
	}
}

func TestUpdateByPK(t *testing.T) {
	preparePqTest(t)
	defer deleteAllPQData(t)
	empUpdate := newPqUpdateTest(t)
	empUpdate.Set("Name", "UCN4")
	err := empUpdate.UpdateByPK(db, "C3")
	if err != nil {
		t.Error(err)
	}
	want := pqEmp{ID: "C3", Name: "UCN4", Child: 2, JoinDate: newTime(2010, time.March, 3)}
	empSelect := newPQSelectTest(t)
	got := pqEmp{}
	empSelect.GetByPK(db, &got, "C3")
	checkResult(t, empSelect, got, want)
}

func TestUpdate(t *testing.T) {
	data := preparePqTest(t)
	defer deleteAllPQData(t)
	empUpdate := newPqUpdateTest(t)
	empUpdate.Set("Name", "ALLSame")
	err := empUpdate.Update(db)
	if err != nil {
		t.Error(err)
	}
	empSelect := newPQSelectTest(t)
	got := pqEmp{}
	for _, v := range data {
		err = empSelect.GetByPK(db, &got, v.ID)
		if err != nil && err != sql.ErrNoRows {
			t.Error(err)
		}
		want := v
		want.Name = "ALLSame"
		checkResult(t, empSelect, got, want)
	}
	empUpdate.Set("Name", "xxxSame")
	empUpdate.Set("NaMe", "NotXXXSame")
	if err := empUpdate.Update(db); err != nil {
		t.Errorf("update twice err: %v", err)
	}
	for _, v := range data {
		err = empSelect.GetByPK(db, &got, v.ID)
		if err != nil && err != sql.ErrNoRows {
			t.Error(err)
		}
		want := v
		want.Name = "NotXXXSame"
		checkResult(t, empSelect, got, want)
	}
}

func newPQSelectTest(t testing.TB) *Select {
	tbl, err := NewTable("emp", pqEmp{})
	if err != nil {
		t.Errorf("create table err: %v", err)
	}
	emp := NewPQSelect(tbl, true)
	return emp
}
func newPqUpdateTest(t testing.TB) *Update {
	tbl, err := NewTable("emp", pqEmp{})
	if err != nil {
		t.Errorf("create table err: %v", err)
	}
	emp := NewPQUpdate(tbl)
	return emp
}

func BenchmarkGetLast(b *testing.B) {
	if !*pqtest {
		b.Skip("to run a benchmark for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	preparePQBench(b)
	b.ReportAllocs()
	emp := newPQSelectTest(b)
	got := &pqEmp{}
	cursor, err := emp.GetByPKWithCursor(db, got, "B2")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = emp.GetLast(db, got, cursor)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func init() {
	flag.Parse()
}

func openPQDB() {
	ds := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", *dbuser, *dbpasswd, *dbname)
	var err error
	db, err = sql.Open("postgres", ds)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
}

func createTable() {
	d := "DROP TABLE IF EXISTS emp"
	_, err := db.Exec(d)
	if err != nil {
		log.Fatalf("drop table err: %v", err)
	}
	q := "CREATE TABLE IF NOT EXISTS emp (ID varchar PRIMARY KEY,Name varchar,child numeric,joindate timestamp)"
	_, err = db.Exec(q)
	if err != nil {
		log.Fatalf("create table err: %v", err)
	}
}

func insertPQData(t testing.TB, data []pqEmp) {
	name := "emp"
	tbl, err := NewTable(name, data[0])
	if err != nil {
		t.Fatalf("insert NewTable %s err: %v", name, err)
	}
	u := NewPQUpdate(tbl)
	for i, v := range data {
		if err := u.Insert(db, v); err != nil {
			t.Fatalf("insert data: %d err: %v", i, err)
		}
	}
}

func deleteAllPQData(t *testing.T) {
	name := "emp"
	tbl, err := NewTable("emp", pqEmp{})
	if err != nil {
		t.Errorf("Delete NewTable %s err: %v", name, err)
	}
	u := NewPQUpdate(tbl)
	err = u.Delete(db)
	if err != nil {
		t.Fatalf("delete data: %s err: %v", name, err)
	}
}

func newTime(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

var onceT sync.Once

func preparePqTest(t *testing.T) []pqEmp {
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
	onceT.Do(func() {
		openPQDB()
		createTable()
	})
	insertPQData(t, data)
	return data
}
