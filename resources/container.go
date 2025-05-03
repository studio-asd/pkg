package resources

import (
	"errors"
	"fmt"
	"strings"
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
	// GRPC client connections.
	grpc *grpcResources
	// Redis connections.
}

func (r *ResourcesContainer) setResources(pkgName string, res map[string]any) {
	if res == nil {
		panic("cannot set an empty resource")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resources[pkgName] = res
}

func (r *ResourcesContainer) getResources(pkgName string) (map[string]any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	res, ok := r.resources[pkgName]
	if !ok {
		return nil, fmt.Errorf("package name %s not found", pkgName)
	}
	return res, nil
}

func (r *ResourcesContainer) GRPC() *grpcResources {
	return r.grpc
}

func Get[T any](container *ResourcesContainer, name string) (T, error) {
	var (
		v  T
		ok bool
	)
	out := strings.Split(name, ".")
	if len(out) != 2 {
		return v, errors.New("need both package and resource name to retrieve a resource")
	}
	res, err := container.getResources(out[0])
	if err != nil {
		return v, err
	}

	t, ok := res[out[1]]
	if !ok {
		return v, fmt.Errorf("resource with name %s is not exist within package %s", out[1], out[0])
	}

	v, ok = t.(T)
	if !ok {
		return v, fmt.Errorf("invalid type %T for resources in package %s", v, out[0])
	}
	return v, nil
}
