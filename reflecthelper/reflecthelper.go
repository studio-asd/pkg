package reflecthelper

import (
	"reflect"
)

// GetTypeValue returns the origin value of a type. It doesn't matter if a value is an interface
// or if a value is pointer, the function will always returns the origin of its type.
//
// For example:
//
//	type Itf interface {
//		DoSomething()
//	}
//
//	// Implements Itf.
//	type A struct {}
//
//	func(a A) DoSomething() {}
//
//	var a Itf = &A{}
//
//	value := getTypeValue(a) // The value is struct A{}
func GetTypevalue(obj any) reflect.Value {
	var (
		kind  reflect.Kind
		value reflect.Value
		itf   any
	)

	switch v := obj.(type) {
	case reflect.Value:
		value = v
		kind = v.Kind()
	default:
		value = reflect.ValueOf(obj)
		kind = value.Kind()
	}

	switch kind {
	case reflect.Interface:
		itf = value.Interface()
		value = reflect.ValueOf(itf)
		// If the type of the value is an interface, then we should extract the value recursively.
		// This is because we want to extract the origin of the type to retrieve the package path
		// and the name of the type.
		//
		// For example:
		//
		// 	type Frontend struct {
		//		Auth api.APIGroup // We need to extract this into the origin object, which is *auth.AuthAPIGroup(api/auth).
		//	}
		//
		// But, since the *auth.AuthAPIGroup is a pointer, we need to extract the type using value.Elem which adding
		// more logic into the switch case of reflect.Interface. To avoid this, we call the extractAPIGroupInfo once
		// again. Since the functino already handle the reflect.Pointer type, we should be able to retrieve the origin
		// type.
		return GetTypevalue(value)

	case reflect.Pointer:
		value = value.Elem()
		if value.Kind() == reflect.Interface {
			return GetTypevalue(value)
		}
	}
	return value
}

// StaticTypeOf returns the reflection Type that represents the static type of T.
func StaticTypeOf[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}
