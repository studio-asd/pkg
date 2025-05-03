package resources

import (
	"context"
	"fmt"
	"log/slog"
)

type Package interface {
	Name() string
	SetLogger(logger *slog.Logger)
	Close(ctx context.Context) error
}

type PostgresPackage interface {
	Connect(ctx context.Context, config *PostgresResourcesConfig) (map[string]any, error)
}

var pkgs = make(map[string]Package)

const (
	postgresPackage = "postgres"
)

// RegisterPackage registers a package to the resources manager to be used to create the resources based on the provided configuration..
func RegisterPackage(pkg Package) {
	switch pkg.(type) {
	case PostgresPackage:
		if _, ok := pkgs[postgresPackage]; ok {
			panic(fmt.Sprintf("package %s already registered", postgresPackage))
		}
		pkgs[postgresPackage] = pkg
	default:
		panic(fmt.Sprintf("unsupported package type %T", pkg))
	}
}
