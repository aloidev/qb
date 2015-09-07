package qb

import (
	"database/sql"
	"flag"
	"fmt"
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

func TestWithPQDatabase(t *testing.T) {
	if !*pqtest {
		t.Skip("to run a test for pq database run the test with pqtest,dbuser and dbpasswd flag.")
	}
	// openPQDB(t)
	// createTable(t)
	data := preparePqTest(t)
	tbl, err := NewTable("emp", data[0])
	if err != nil {
		t.Errorf("create table err: %v", err)
	}
	b := NewPQ(tbl, true)
	// b.SetFields("id", "name", "child")
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

var once sync.Once

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
	once.Do(func() {
		openPQDB(t)
		createTable(t)
		populateData(t, data)
	})
	return data
}

func testSetFilterPQ(t *testing.T, b *Builder, want pqEmp) {
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

func init() {
	flag.Parse()
}

func openPQDB(t *testing.T) {
	ds := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", *dbuser, *dbpasswd, *dbname)
	var err error
	db, err = sql.Open("postgres", ds)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func createTable(t *testing.T) {
	d := "DROP TABLE IF EXISTS emp"
	_, err := db.Exec(d)
	if err != nil {
		t.Errorf("drop table err: %v", err)
	}
	q := "CREATE TABLE IF NOT EXISTS emp (ID varchar PRIMARY KEY,Name varchar,child numeric,joindate timestamp)"
	_, err = db.Exec(q)
	if err != nil {
		t.Errorf("create table err: %v", err)
	}
}

func populateData(t *testing.T, data []pqEmp) {
	tbl, err := NewTable("emp", pqEmp{})
	if err != nil {
		t.Fatal(err)
	}
	b := NewPQUpdate(tbl)
	insertQ := b.InsertQuery()
	for i, v := range data {
		_, err := db.Exec(insertQ, v.ID, v.Name, v.Child, v.JoinDate)
		if err != nil {
			t.Errorf("insert data: %d failed err: %v", i, err)
		}
	}
}

func newTime(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
