package bq

import (
	"fmt"
	"reflect"

	"google.golang.org/api/bigquery/v2"
)

// Encode takes a struct and returns a BigQuery compatible encoded map.
func Encode(v interface{}) (map[string]bigquery.JsonValue, error) {
	rv := reflect.ValueOf(v)

	if rv.Kind() != reflect.Struct {
		if rv.Kind() == reflect.Ptr {
			return Encode(rv.Elem().Interface())
		}
		return nil, fmt.Errorf("bq: unsupported type: %T (%v)", v, rv.Kind())
	}

	m := make(map[string]bigquery.JsonValue)

	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)

		name := fieldName(field)
		switch {
		case !fv.CanInterface(),
			name == "-":
			continue
		}

		m[name] = bigquery.JsonValue(fv.Interface())
	}

	return m, nil
}

func fieldName(field reflect.StructField) string {
	if name := field.Tag.Get("msg"); name != "" {
		return name
	}
	if name := field.Tag.Get("bq"); name != "" {
		return name
	}
	return field.Name
}
