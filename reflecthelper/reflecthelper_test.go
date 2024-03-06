package reflecthelper

import (
	"reflect"
	"testing"
)

func TestGetTypeValue(t *testing.T) {
	t.Parallel()

	type Type1 struct{}
	expectTypeName := "Type1"

	t.Run("concrete struct type", func(t *testing.T) {
		t.Parallel()
		v := GetTypevalue(Type1{})

		if v.Type().Name() != expectTypeName {
			t.Fatalf("invalid type name, got %s but expect %s", v.Type().Name(), expectTypeName)
		}
	})

	t.Run("pointer struct type", func(t *testing.T) {
		t.Parallel()
		v := GetTypevalue(&Type1{})

		if v.Type().Name() != expectTypeName {
			t.Fatalf("invalid type name, got %s but expect %s", v.Type().Name(), expectTypeName)
		}
	})

	t.Run("using reflect value", func(t *testing.T) {
		t.Parallel()
		v := GetTypevalue(reflect.ValueOf(Type1{}))

		if v.Type().Name() != expectTypeName {
			t.Fatalf("invalid type name, got %s but expect %s", v.Type().Name(), expectTypeName)
		}
	})

	// TODO: add interface to interface type test.
}
