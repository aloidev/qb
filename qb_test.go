package qb

import (
	"reflect"
	"testing"
)

type simple struct {
	ID   string `pk:"1"`
	Name string
}

type twoPK struct {
	DocNo string `pk:"1"`
	Rev   string `pk:"2"`
	Name  string
}

type threePK struct {
	DocNo string `pk:"1"`
	Rev   string `pk:"3"`
	Name  string
	By    string `pk:"5"`
}

func TestSelectAll(t *testing.T) {
	tbl, err := NewTable("", simple{})
	if err != nil {
		t.Errorf("got err = %v want nil", err)
	}

	b := NewPQ(tbl, false)
	got := b.SelectAll()
	want := "SELECT * FROM simple ORDER BY id"
	if got != want {
		t.Errorf("got query = %s want %s", got, want)
	}
	b = NewPQ(tbl, true)
	got = b.SelectAll()
	want = "SELECT id,name FROM simple ORDER BY id"
	if got != want {
		t.Errorf("got query = %s want %s", got, want)
	}
}

func TestSelectByPK(t *testing.T) {
	var tests = []struct {
		tbl      interface{}
		explicit bool
		want     string
	}{
		{simple{}, false, "SELECT * FROM simple WHERE id = $1"},
		{simple{}, true, "SELECT id,name FROM simple WHERE id = $1"},
		{twoPK{}, false,
			"SELECT * FROM twopk WHERE docno = $1 AND rev = $2"},
		{twoPK{}, true,
			"SELECT docno,rev,name FROM twopk WHERE docno = $1 AND rev = $2"},
		{threePK{}, false,
			"SELECT * FROM threepk WHERE docno = $1 AND rev = $2 AND by = $3"},
		{threePK{}, true,
			"SELECT docno,rev,name,by FROM threepk WHERE docno = $1 AND rev = $2 AND by = $3"},
	}
	for _, test := range tests {
		testSelectByPK(t, test.tbl, test.explicit, test.want)
	}
}

func testSelectByPK(t *testing.T, tbl interface{}, explicit bool, want string) {
	b := newBuilder(t, tbl, explicit)
	got := b.SelectByPK()
	if got != want {
		t.Errorf("got query: %s \n             want %s", got, want)
	}
}

func TestSetFilterQuery(t *testing.T) {
	type Emp struct {
		ID   string `pk:"1"`
		Name string
		Age  int
	}
	b := newBuilder(t, Emp{}, false)
	b.SetFilter("age", "=", 20)
	wantQ := "SELECT * FROM emp WHERE age = $1"
	wantA := []interface{}{20}
	testQuery(t, b, wantQ, wantA)
	b.SetFilter("Name", "=", "me")
	wantQ = "SELECT * FROM emp WHERE age = $1 AND name = $2"
	wantA = []interface{}{20, "me"}
	testQuery(t, b, wantQ, wantA)
}

func TestSetRangeQuery(t *testing.T) {
	type Emp struct {
		ID   string `pk:"1"`
		Name string
		Age  int
	}
	b := newBuilder(t, Emp{}, false)
	b.SetRange("Name", "a", "b")
	wantQ := "SELECT * FROM emp WHERE name >= $1 AND name <= $2"
	wantA := []interface{}{"a", "b"}
	testQuery(t, b, wantQ, wantA)
}

func testQuery(t *testing.T, b *Builder, wantQ string, wantA []interface{}) {
	gotQ, gotA := b.Query()
	if gotQ != wantQ {
		t.Errorf("got query: %s \n             want %s", gotQ, wantQ)
	}
	if !reflect.DeepEqual(gotA, wantA) {
		t.Errorf("got args: %v want %v", gotA, wantA)
	}
}

func TestError(t *testing.T) {
	type Emp struct {
		ID   string `pk:"1"`
		Name string
		Age  int
	}
	b := newBuilder(t, Emp{}, false)
	b.SetFilter("NamE", "=", "me")
	testError(t, b, false)
	b.SetFilter("FieldNotExist", "=", "b")
	testError(t, b, true)
}

func TestReset(t *testing.T) {
	type Emp struct {
		ID   string `pk:"1"`
		Name string
		Age  int
	}
	b := newBuilder(t, Emp{}, false)
	b.SetFilter("age", "=", 20)
	b.SetFields("ID")
	wantQ := "SELECT id FROM emp WHERE age = $1"
	wantA := []interface{}{20}
	testQuery(t, b, wantQ, wantA)
	b.Reset()
	b.SetFields("name")
	b.SetFilter("Name", "=", "me")
	wantQ = "SELECT name FROM emp WHERE name = $1"
	wantA = []interface{}{"me"}
	testQuery(t, b, wantQ, wantA)
}

func testError(t *testing.T, b *Builder, err bool) {
	got := b.Error()
	if err {
		if got == nil {
			t.Errorf("got: nil want an error")
		}
		return
	}
	if got != nil {
		t.Errorf("got err: %v want nil", got)
	}
}

func newBuilder(t *testing.T, tbl interface{}, explicit bool) *Builder {
	ti, err := NewTable("", tbl)
	if err != nil {
		t.Errorf("got err : %v want nil", err)
	}

	b := NewPQ(ti, explicit)
	return b
}
