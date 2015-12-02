package bq

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"google.golang.org/api/bigquery/v2"
)

// Schema infers a BigQuery schema from the given struct v.
func Schema(v interface{}) (*bigquery.TableSchema, error) {
	rv := reflect.ValueOf(v)

	if rv.Kind() != reflect.Struct {
		if rv.Kind() == reflect.Ptr {
			return Schema(rv.Elem().Interface())
		}
		return nil, fmt.Errorf("bq schema: unsupported type: %T (%v)", v, rv.Kind())
	}

	schema := &bigquery.TableSchema{}

	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)

		name, _ := fieldInfo(field)
		switch {
		case !fv.CanInterface(),
			name == "-":
			continue
		}

		schema.Fields = append(schema.Fields, &bigquery.TableFieldSchema{
			Name: name,
			Type: fieldType(field, fv),
		})
	}

	return schema, nil
}

// SchemaFromJSON infers a BigQuery schema from the given JSON map.
func SchemaFromJSON(m map[string]bigquery.JsonValue) (*bigquery.TableSchema, error) {
	schema := &bigquery.TableSchema{}
	for name, v := range m {
		fv := reflect.ValueOf(v)
		schema.Fields = append(schema.Fields, &bigquery.TableFieldSchema{
			Name: name,
			Type: typeOf(fv),
		})
	}
	return schema, nil
}

var timeType = reflect.TypeOf(time.Time{})

// STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP or RECORD
func fieldType(field reflect.StructField, fv reflect.Value) string {
	if t := field.Tag.Get("bqtype"); t != "" {
		return strings.ToUpper(t)
	}
	return typeOf(fv)
}

func typeOf(fv reflect.Value) string {
	if fv.Kind() == reflect.Ptr {
		return typeOf(fv.Elem())
	}

	if fv.Type() == timeType {
		return "TIMESTAMP"
	}
	switch fv.Kind() {
	case reflect.String:
		return "STRING"
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return "INTEGER"
	case reflect.Float64, reflect.Float32:
		return "FLOAT"
	case reflect.Bool:
		return "BOOLEAN"
	}

	// TODO: record support
	panic(fmt.Errorf("bq schema: unsupported type %T: %v", fv.Interface(), fv.Interface()))
}
