package pkg

import (
	"fmt"
	"strings"
)

const (
	Unknown = iota
	Int
	Float
	String
	DateTime
	ElasticDateTime
	IntArray
	FloatArray
	StringArray
	DateTimeArray
)

type typeInfo struct {
	typ      int
	nullable bool
}

var (
	info map[string]typeInfo
)

func whichType(typ string) (dataType int, nullable bool) {
	ti, ok := info[typ]
	if ok {
		dataType, nullable = ti.typ, ti.nullable
		return
	}
	nullable = strings.HasPrefix(typ, "Nullable(")
	if nullable {
		typ = typ[len("Nullable(") : len(typ)-1]
	}
	if strings.HasPrefix(typ, "DateTime64") {
		dataType = DateTime
	} else if strings.HasPrefix(typ, "Array(DateTime64") {
		dataType = DateTimeArray
		nullable = false
	} else if strings.HasPrefix(typ, "Decimal") {
		dataType = Float
	} else if strings.HasPrefix(typ, "Array(Decimal") {
		dataType = FloatArray
		nullable = false
	} else if strings.HasPrefix(typ, "FixedString") {
		dataType = String
	} else if strings.HasPrefix(typ, "Array(FixedString") {
		dataType = StringArray
		nullable = false
	} else if strings.HasPrefix(typ, "Enum8(") {
		dataType = String
	} else if strings.HasPrefix(typ, "Enum16(") {
		dataType = String
	} else {
		logger.Fatal(fmt.Sprintf("LOGIC ERROR: unsupported ClickHouse data type %v", typ))
	}
	info[typ] = typeInfo{typ: dataType, nullable: nullable}
	return
}
