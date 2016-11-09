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
	E int    `msg:"qux,omitempty"`
	F *int   `msg:",omitempty"`
	G *int   `msg:",omitempty"`
}

func TestEncodeOmitEmpty(t *testing.T) {
	zero := new(int)
	in := testStruct{
		A: "hello",
		B: "",
		C: "",
		D: "",
		E: 1337,
		F: zero,
	}
	expect := map[string]bigquery.JsonValue{
		"A":   bigquery.JsonValue("hello"),
		"foo": bigquery.JsonValue(""),
		"qux": bigquery.JsonValue(1337),
		"F":   zero,
	}

	out, err := Encode(in)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(out, expect) {
		t.Errorf("bad encode. %v ≠ %v", out, expect)
	}
}
