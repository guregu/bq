package bq

import (
	"fmt"
	"reflect"
	"strings"

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

		name, special := fieldInfo(field)
		switch {
		case !fv.CanInterface(),
			name == "-":
			continue
		}

		if special == "omitempty" && isZero(fv) {
			continue
		}

		m[name] = bigquery.JsonValue(fv.Interface())
	}

	return m, nil
}

func fieldInfo(field reflect.StructField) (name, special string) {
	if tag := field.Tag.Get("bq"); tag != "" {
		return splitTag(tag, field.Name)
	}
	if tag := field.Tag.Get("msg"); tag != "" {
		return splitTag(tag, field.Name)
	}
	return field.Name, ""
}

func splitTag(tag, fieldName string) (name, special string) {
	if idx := strings.IndexRune(tag, ','); idx != -1 {
		if idx == 0 {
			return fieldName, tag[idx+1:]
		}
		return tag[:idx], tag[idx+1:]
	}
	if len(tag) > 0 {
		return tag, ""
	}
	return fieldName, ""
}

func isZero(rv reflect.Value) bool {
	switch rv.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return rv.IsNil()
	}

	z := reflect.Zero(rv.Type())
	return rv.Interface() == z.Interface()
}
