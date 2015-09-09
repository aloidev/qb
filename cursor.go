package qb

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

//Cursor represent current state of the select query, that can be use for paging operation.
type Cursor struct {
	fields  []string
	filters []filter
	orderBy []string
	limit   int
	offset  int
}

var sep = []byte("\n")

//Decode decodes the string.
func (c Cursor) Decode(s string) (Cursor, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	ds := bytes.Split(data, sep)
	c.fields = c.decodeToSS(ds[0])
	c.filters = c.decodeFilters(ds[1])
	c.orderBy = c.decodeToSS(ds[2])

	limit, err := strconv.Atoi(string(ds[3]))
	if err != nil {
		return c, err
	}
	c.limit = limit

	offset, err := strconv.Atoi(string(ds[4]))
	if err != nil {
		return c, err
	}
	c.offset = offset
	return c, nil
}

//Decode to slice string
func (c Cursor) decodeToSS(data []byte) []string {
	var result []string
	if len(data) == 0 {
		return result
	}
	result = strings.Split(string(data), ",")
	return result
}

func (c Cursor) decodeFilters(data []byte) []filter {
	var filters []filter
	if len(data) == 0 {
		return filters
	}
	ds := bytes.Split(data, []byte(";"))
	for _, v := range ds {
		if len(v) != 0 {
			vs := bytes.Split(v, []byte(","))
			filters = append(filters,
				filter{
					field: string(vs[0]),
					op:    string(vs[1]),
					value: string(vs[2]),
				})
		}
	}
	return filters
}

//String return base64 string representation of the cursor.
func (c Cursor) String() string {
	buf := &bytes.Buffer{}
	if len(c.fields) != 0 {
		buf.WriteString(strings.Join(c.fields, ","))
	}
	buf.Write(sep)
	for _, filter := range c.filters {
		fmt.Fprintf(buf, "%s,%s,%v;", filter.field, filter.op, filter.value)
	}
	buf.Write(sep)
	buf.WriteString(strings.Join(c.orderBy, ","))
	buf.Write(sep)
	fmt.Fprintf(buf, "%d", c.limit)
	buf.Write(sep)
	fmt.Fprintf(buf, "%d", c.offset)
	buf.Write(sep)
	// fmt.Fprintf(buf, "%d", c.count)
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}
