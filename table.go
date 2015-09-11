package qb

import (
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

//Table implement Tabler interface to help using builder without implements the tabler interface.
type Table struct {
	name   string
	fields []string
	//fieldsIndex is an index of the field on the struct.
	fieldsIndex []int
	primaryKey  []string
}

//NewTaable create Tabler implementation using reflection.
func NewTable(name string, s interface{}) (t Table, err error) {
	st := reflect.TypeOf(s)
	if st.Kind() == reflect.Struct {
		return fromStruct(name, st)
	}
	return Table{}, errors.New("unsupported type.")
}

func fromStruct(name string, s reflect.Type) (t Table, err error) {
	if name == "" {
		if s.Name() == "" {
			err = errors.New("please specify a name or use name type.")
			return t, err
		}
		name = s.Name()
	}

	t.name = strings.ToLower(name)
	n := s.NumField()
	if n <= 0 {
		return t, errors.New("struct doesn't have a field.")
	}

	for i := 0; i < n; i++ {
		field := s.Field(i)
		if !field.Anonymous {
			t.fields = append(t.fields, strings.ToLower(field.Name))
			t.fieldsIndex = append(t.fieldsIndex, i)
		}
	}
	t.primaryKey, err = primaryKeys(s)
	return t, err
}

func primaryKeys(s reflect.Type) ([]string, error) {
	pk := make(map[int]string, 0)
	var pkNum sort.IntSlice
	for i := 0; i < s.NumField(); i++ {
		field := s.Field(i)
		pkTag := field.Tag.Get("pk")
		if pkTag != "" {
			num, err := strconv.ParseInt(pkTag, 10, 8)
			if err != nil {
				return nil, err
			}
			fieldName := strings.ToLower(field.Name)
			n := int(num)
			if v, exist := pk[n]; exist {
				err = errors.New("key number conflict field " + v + "with " + fieldName)
				return nil, err
			}
			pk[n] = fieldName
			pkNum = append(pkNum, n)
		}
	}
	n := len(pk)
	if n == 0 {
		return nil, errors.New("struct doesn't have a primary key.")
	}
	pkNum.Sort()
	result := make([]string, n)
	for i, v := range pkNum {
		result[i] = pk[v]
	}
	return result, nil
}

//TableName return the table name.
func (t Table) TableName() string {
	return t.name
}

//Fields return all the fields for the table.
func (t Table) Fields() []string {
	return t.fields
}

//primaryKey return primary keys for the table.
func (t Table) PrimaryKeys() []string {
	return t.primaryKey
}
