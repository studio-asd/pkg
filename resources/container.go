package resources

import (
	"fmt"
	"reflect"
	"sync"
)

type ResourcesContainer struct {
	mu sync.Mutex
	// resourcess is a container that stores every single resource map inside the package.
	// The resources is grouped by its each package name and can be retrieved through the
	// generic Get function by passing both the package name and the resource name. For example
	// we can retrieve the postgres resources by using "postgres.[resource_name]". We will automatically
	// split the "." to separate the pacakge and resource name.
	resources map[string]map[string]any
}

func (r *ResourcesContainer) setResources(res map[string]any) {
	if res == nil {
		panic("cannot set an empty resource")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(res) == 0 {
		panic("cannot register zero length resources")
	}
	var t reflect.Type
	for _, v := range res {
		t = reflect.TypeOf(v)
		break
	}
	// Set the resource key using the package path and the type name. We might have the same type name for
	// different packages.
	resourceKey := t.PkgPath() + "." + t.Name()
	r.resources[resourceKey] = res
}

func (r *ResourcesContainer) getResources(resourceKey string) (map[string]any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	res, ok := r.resources[resourceKey]
	if !ok {
		return nil, fmt.Errorf("resource key%s not found", resourceKey)
	}
	return res, nil
}

func Get[T any](container *ResourcesContainer, name string) (T, error) {
	var (
		v  T
		ok bool
	)

	tt := reflect.TypeOf(v)
	resourceKey := tt.PkgPath() + "." + tt.Name()
	res, err := container.getResources(resourceKey)
	if err != nil {
		return v, err
	}

	t, ok := res[name]
	if !ok {
		return v, fmt.Errorf("resource with name %s is not exist within resource key %s", name, resourceKey)
	}

	v, ok = t.(T)
	if !ok {
		return v, fmt.Errorf("invalid type %T for resources in package %s", v, name)
	}
	return v, nil
}
