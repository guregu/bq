package bq

import (
	"reflect"
	"testing"

	"google.golang.org/api/bigquery/v2"
)

type testStruct struct {
	A string `bq:",omitempty"`
	B string `bq:"foo"`
	C string `bq:"bar,omitempty"`
	D string `msg:"baz,omitempty"`
}

func TestEncodeOmitEmpty(t *testing.T) {
	in := testStruct{
		A: "hello",
		B: "",
		C: "",
		D: "",
	}
	expect := map[string]bigquery.JsonValue{
		"A":   bigquery.JsonValue("hello"),
		"foo": bigquery.JsonValue(""),
	}

	out, err := Encode(in)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(out, expect) {
		t.Error("bad encode. %v ≠ %v", out, expect)
	}
}
