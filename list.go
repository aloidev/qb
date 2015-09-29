package qb

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

//ErrDone is an error to signal no more rows.
var ErrDone = errors.New("no more rows")

//QueryExecer is an
//sql.DB and sq.Tx from GO standard library implement QueryExecer
type QueryExecer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

//Queryer is an interface that can produce query string and the arguments to execute the query.
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

//GetAll execute the query and store the result to the dst.
//Dst should be pointer to slice.
func (l *List) GetAll(qe QueryExecer, dstx interface{}) error {
	if err := l.Get(qe); err != nil {
		return err
	}
	vo := reflect.ValueOf(dstx)
	if vo.Kind() != reflect.Ptr || vo.Elem().Kind() != reflect.Slice {
		return errors.New("dst must be pointer to slice")
	}
	dst := reflect.Indirect(vo)
	n := dst.Len()
	kind := dst.Type().Elem().Kind()
	var v reflect.Value
	if kind == reflect.Struct {
		v = reflect.New(dst.Type().Elem())
	}
	i := 0
	var err error
	for l.rows.Next() {
		if err = scanReflectValue(l.s.fields, l.rows, v); err != nil {
			return err
		}
		if i < n {
			dst.Index(i).Set(v.Elem())
		} else {
			dst = reflect.Append(dst, v.Elem())
		}
		i++
	}

	err = l.rows.Err()
	if err != nil {
		return err
	}
	dst.SetLen(i)
	vo.Elem().Set(dst)
	return nil
}

func (l *List) GetNext(qe QueryExecer, cursor string) error {
	err := l.s.setCursor(cursor)
	if err != nil {
		return err
	}
	l.s.offset += l.s.limit
	query, args := l.s.Query()
	rows, err := qe.Query(query, args...)
	if err != nil {
		return err
	}
	l.rows = rows
	return nil
}

func (l *List) GetLast(qe QueryExecer, cursor string) error {
	if err := l.s.getLast(qe, cursor); err != nil {
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
	//TODO: SelectByPK is not filled l.s.fields
	if l.rows.Next() {
		if sa, ok := dst.(ScanArger); ok {
			return l.scanWithArger(sa)
		}
		// return l.scanWithReflect(dst)
		v := reflect.ValueOf(dst)
		if err := scanReflectValue(l.s.fields, l.rows, v); err != nil {
			// if err := scanWithReflection(l.s.fields, l.rows, dst); err != nil {
			l.rows.Close()
			return err
		}
		return nil
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

//Close is to call close on the sql.Rows,
//rows on the list follow the rule on standard database sql package.
func (l *List) Close() error {
	return l.rows.Close()
}

type rowScanner interface {
	Scan(dst ...interface{}) error
}

func scanWithReflection(fields []string, r rowScanner, dst interface{}) error {
	v := reflect.ValueOf(dst)
	return scanReflectValue(fields, r, v)
	// if v.Kind() != reflect.Ptr || v.IsNil() {
	// 	return errors.New("dst must be pointer to struct")
	// }
	// ve := v.Elem()
	// n := ve.NumField()
	// if n <= 0 || n < len(fields) {
	// 	return errors.New("destination field not enough")
	// }
	//
	// var args []interface{}
	// for _, field := range fields {
	// 	dstF := ve.FieldByNameFunc(func(dstName string) bool {
	// 		if strings.ToLower(dstName) == field {
	// 			return true
	// 		}
	// 		return false
	// 	})
	// 	if dstS, ok := dstF.Interface().(sql.Scanner); ok {
	// 		args = append(args, dstS)
	// 		continue
	// 	}
	// 	if dstF.Kind() == reflect.Ptr {
	// 		args = append(args, dstF.Interface())
	// 		continue
	// 	}
	// 	if dstF.IsValid() {
	// 		args = append(args, fieldScanner{dv: dstF})
	// 	}
	// }
	// if len(args) != len(fields) {
	// 	return errors.New("destination field not enough")
	// }
	// if err := r.Scan(args...); err != nil {
	// 	return err
	// }
	// return nil
}

func scanReflectValue(fields []string, r rowScanner, v reflect.Value) error {
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.New("dst must be pointer to struct")
	}
	ve := v.Elem()
	n := ve.NumField()
	if n <= 0 || n < len(fields) {
		return errors.New("destination field not enough")
	}

	var args []interface{}
	for _, field := range fields {
		dstF := ve.FieldByNameFunc(func(dstName string) bool {
			if strings.ToLower(dstName) == field {
				return true
			}
			return false
		})
		if dstS, ok := dstF.Interface().(sql.Scanner); ok {
			args = append(args, dstS)
			continue
		}
		if dstF.Kind() == reflect.Ptr {
			args = append(args, dstF.Interface())
			continue
		}
		if dstF.IsValid() {
			args = append(args, fieldScanner{dv: dstF})
		}
	}
	if len(args) != len(fields) {
		return errors.New("destination field not enough")
	}
	if err := r.Scan(args...); err != nil {
		return err
	}
	return nil
}

type fieldScanner struct {
	dv reflect.Value
}

const pqTime = "2006-01-02 15:04:05 +0000 +0000"

func (sc fieldScanner) Scan(src interface{}) error {
	if !sc.dv.CanSet() {
		return errors.New("field is not settable")
	}
	switch sc.dv.Kind() {
	case reflect.String:
		s := asString(src)
		sc.dv.SetString(s)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := asString(src)
		i64, err := strconv.ParseInt(s, 10, sc.dv.Type().Bits())
		if err != nil {
			return fmt.Errorf("converting string %q to a %s: %v", s, sc.dv.Kind(), err)
		}
		sc.dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s := asString(src)
		u64, err := strconv.ParseUint(s, 10, sc.dv.Type().Bits())
		if err != nil {
			return fmt.Errorf("converting string %q to a %s: %v", s, sc.dv.Kind(), err)
		}
		sc.dv.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		s := asString(src)
		f64, err := strconv.ParseFloat(s, sc.dv.Type().Bits())
		if err != nil {
			return fmt.Errorf("converting string %q to a %s: %v", s, sc.dv.Kind(), err)
		}
		sc.dv.SetFloat(f64)
		return nil
	case reflect.Struct:
		if _, ok := sc.dv.Interface().(time.Time); ok {
			s := asString(src)
			dtt, err := time.Parse(pqTime, s)
			sc.dv.Set(reflect.ValueOf(dtt))
			return err
		}
	}
	return fmt.Errorf("unsupported driver -> Scan pair: %T -> %T", src, sc.dv)
}

//copied from GO standard library database.sql package
func asString(src interface{}) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return fmt.Sprintf("%v", src)
}

//copied from GO standard library database.sql package
func asBytes(buf []byte, rv reflect.Value) (b []byte, ok bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(buf, rv.Uint(), 10), true
	case reflect.Float32:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 32), true
	case reflect.Float64:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 64), true
	case reflect.Bool:
		return strconv.AppendBool(buf, rv.Bool()), true
	case reflect.String:
		s := rv.String()
		return append(buf, s...), true
	}
	return
}
