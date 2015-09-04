package qb

import (
	"reflect"
	"testing"
)

type testCase struct {
	tableName  string
	in         interface{}
	wantFields []string
	wantPK     []string
}

func TestNewTableSimple(t *testing.T) {
	type simple struct {
		ID   string `pk:"1"`
		Name string
	}
	tc := testCase{
		in:         simple{},
		tableName:  "simple",
		wantFields: []string{"id", "name"},
		wantPK:     []string{"id"},
	}
	testNewTable(t, tc)
}

func TestNewTableMultiplePK(t *testing.T) {
	type mpk struct {
		DocNo string `pk:"1"`
		Name  string `pk:"2"`
	}
	tc := testCase{
		in:         mpk{},
		tableName:  "mpk",
		wantFields: []string{"docno", "name"},
		wantPK:     []string{"docno", "name"},
	}
	testNewTable(t, tc)
}

func testNewTable(t *testing.T, tc testCase) {
	st, err := NewTable(tc.tableName, tc.in)
	if err != nil {
		t.Errorf("create table err:%v", err)
	}
	gotFields := st.Fields()
	if !reflect.DeepEqual(gotFields, tc.wantFields) {
		t.Errorf("got fields = %v want %v", gotFields, tc.wantFields)
	}
	gotPK := st.PrimaryKeys()
	if !reflect.DeepEqual(gotPK, tc.wantPK) {
		t.Errorf("got fields = %v want %v", gotPK, tc.wantPK)
	}
}

func TestUseStructName(t *testing.T) {
	type mpk struct {
		DocNo string `pk:"1"`
		Name  string `pk:"2"`
	}
	st, err := NewTable("", mpk{})
	if err != nil {
		t.Errorf("create table err:%v", err)
	}
	got := st.TableName()
	if got != "mpk" {
		t.Errorf("got = %s want %s", got, "mpk")
	}
}

func TestNewTableShouldFail(t *testing.T) {
	type conflictPK struct {
		DocNo string `pk:"1"`
		Name  string `pk:"1"`
	}
	testNewTableShouldFail(t, conflictPK{})

	type mpk struct {
		DocNo string `pk:"1"`
		Name  string `pk:"2"`
	}
	pointerToStruct := &mpk{"d1", "n1"}
	testNewTableShouldFail(t, pointerToStruct)
	emptyStruct := struct{}{}
	testNewTableShouldFail(t, emptyStruct)

	type allPrivate struct {
		docNo string
		name  string
	}

	noFields := allPrivate{}
	testNewTableShouldFail(t, noFields)
}

func testNewTableShouldFail(t *testing.T, invalid interface{}) {
	if _, err := NewTable("name", invalid); err == nil {
		t.Errorf("expected error when create the table.")
	}
}
