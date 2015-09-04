package qb

import "testing"

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
		{simple{}, false, "SELECT * FROM simple WHERE id = $1 ORDER BY id"},
		{simple{}, true, "SELECT id,name FROM simple WHERE id = $1 ORDER BY id"},
		{twoPK{}, false,
			"SELECT * FROM twopk WHERE docno = $1 AND rev = $2 ORDER BY docno,rev"},
		{twoPK{}, true,
			"SELECT docno,rev,name FROM twopk WHERE docno = $1 AND rev = $2 ORDER BY docno,rev"},
		{threePK{}, false,
			"SELECT * FROM threepk WHERE docno = $1 AND rev = $2 AND by = $3 ORDER BY docno,rev,by"},
		{threePK{}, true,
			"SELECT docno,rev,name,by FROM threepk WHERE docno = $1 AND rev = $2 AND by = $3 ORDER BY docno,rev,by"},
	}
	for _, test := range tests {
		testSelectByPK(t, test.tbl, test.explicit, test.want)
	}
}

func testSelectByPK(t *testing.T, tbl interface{}, explicit bool, want string) {
	ti, err := NewTable("", tbl)
	if err != nil {
		t.Errorf("got err : %v want nil", err)
	}

	b := NewPQ(ti, explicit)
	got := b.SelectByPK()
	if got != want {
		t.Errorf("got query: %s \n             want %s", got, want)
	}
}
