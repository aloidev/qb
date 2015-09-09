package qb

import (
	"database/sql"
	"errors"
)

//ErrDone is an error to signal no more rows.
var ErrDone = errors.New("no more rows")

//QueryExecer is an
//sql.DB and sq.Tx from GO standard library implement QueryExecer
type QueryExecer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

//Queryer is an interface that can produce query string and the arruments to execute the query.
type Queryer interface {
	Query() (query string, args []interface{})
}

//ScanArger is an interface that use to store the data when scan the executed query.
type ScanArger interface {
	ScanArgs(fields []string) []interface{}
}

//List is an iterator to save the result of the query to the struct.
type List struct {
	s    *Select
	rows *sql.Rows
}

//NewList return a list that ready to use for the querying data.
func NewList(s *Select) *List {
	return &List{s: s}
}

//Get execute the query using the QueryExecer, use Next method to save the result of the query.
func (l *List) Get(qe QueryExecer) error {
	//TODO:this error check could be skipped, as driver or the database would check.
	if err := l.s.Error(); err != nil {
		return err
	}
	query, args := l.s.Query()
	rows, err := qe.Query(query, args...)
	if err != nil {
		return err
	}
	l.rows = rows
	return nil
}

//Next scan the rows and save the result to the dst.
//dst must be pointer to struct or implement ScanArger.
func (l *List) Next(dst interface{}) error {
	//TODO: if no records match the query, list will return ErrDone instead of sql.ErrNoRows
	if l.rows.Next() {
		if sa, ok := dst.(ScanArger); ok {
			return l.scanWithArger(sa)
		}
		return l.scanWithReflect(dst)
	}
	if err := l.rows.Err(); err != nil {
		return err
	}
	return ErrDone
}

func (l *List) scanWithArger(dst ScanArger) error {
	if err := l.rows.Scan(dst.ScanArgs(l.s.fields)...); err != nil {
		l.rows.Close()
		return err
	}
	return nil
}

func (l *List) scanWithReflect(dst interface{}) error {
	return nil
}

//Close is to call close on the sql.Rows,
//rows on the list follow the rule on standard database sql package.
func (l *List) Close() error {
	return l.rows.Close()
}
