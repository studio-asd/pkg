package resources

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/studio-asd/pkg/srun"
)

type Package interface {
	SetLogger(logger *slog.Logger)
	Init(ctx srun.Context) error
	Close(ctx context.Context) error
}

type PostgresPackage interface {
	Connect(ctx context.Context, config *PostgresResourcesConfig) (map[string]any, error)
}

type GRPCClientPackage interface {
	Connect(ctx context.Context, config []GRPCClientResourceConfig) (map[string]any, error)
}

type GRPCServerPackage interface {
	Create(ctx context.Context, config []GRPCServerResourceConfig) (map[string]any, error)
	Run(ctx context.Context) error
}

var pkgs = make(map[string]Package)

const (
	postgresPackage   = "postgresql"
	grpcClientPackage = "grpc.client"
	grpcServerPackage = "grpc.server"
)

// RegisterPackage registers a package to the resources manager to be used to create the resources based on the provided configuration..
//
// This function allows the user of the resources package to import only the package they need and used by the configurations.
// For example, if the client only need the postgresql package for the resources, the client can do:
/*
	package main

	import (
		"github.com/studio-asd/pkg/resources
		"github.com/studio-asd/pkg/resources/postgres
	)
*/
//
// The "postgresql" package will automatically register itself when its imported by using init().
func RegisterPackage(pkg Package) {
	packagePath := reflect.TypeOf(pkg).PkgPath()
	switch pkg.(type) {
	case PostgresPackage:
		if _, ok := pkgs[postgresPackage]; ok {
			panic(fmt.Sprintf("package %s already registered with package %s", postgresPackage, packagePath))
		}
		pkgs[postgresPackage] = pkg
	case GRPCServerPackage:
		if _, ok := pkgs[grpcServerPackage]; ok {
			panic(fmt.Sprintf("package %s already registered with package %s", grpcServerPackage, packagePath))
		}
		pkgs[grpcServerPackage] = pkg
	default:
		panic(fmt.Sprintf("unsupported package type %T", pkg))
	}
}
