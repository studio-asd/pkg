package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/albertwidi/pkg/postgres"
)

func TestMigrate(t *testing.T) {
	t.Parallel()

	pgtest, err := New(ConnConfig{
		Driver:   testDriver,
		Username: "postgres",
		Password: "postgres",
		Host:     "localhost",
		Port:     "5432",
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		pgtest.Cleanup()
	})

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	migrationPath := filepath.Join(dir, "testdata")

	if err := pgtest.MigrateUP("testing_1", WithGoose(migrationPath)); err != nil {
		t.Fatal(err)
	}

	// Check baseline schema.
	var schemaName string
	baselineSchemaQuery := "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'baseline';"
	row := pgtest.QueryRow(context.Background(), baselineSchemaQuery)
	if err := row.Scan(&schemaName); err != nil {
		t.Fatal(err)
	}
	if schemaName != "baseline" {
		t.Fatalf("expecting baseline schema name but got %s", schemaName)
	}

	var publicTables []string
	var baselineTables []string

	// Compare table list in public schema and baseline schema.
	getTablesQuery := "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN('public', 'baseline');"
	if err := pgtest.RunQuery(context.Background(), getTablesQuery, func(rc *postgres.RowsCompat) error {
		var schemaName string
		var tableName string

		if err := rc.Scan(&schemaName, &tableName); err != nil {
			return err
		}

		switch schemaName {
		case "public":
			publicTables = append(publicTables, tableName)
		case "baseline":
			baselineTables = append(baselineTables, tableName)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	slices.Sort[[]string](publicTables)
	slices.Sort[[]string](baselineTables)

	if diff := cmp.Diff(baselineTables, publicTables); diff != "" {
		t.Fatalf("(-want/+got)\n%s", diff)
	}
}

func TestAcquireConnWithNewSchema(t *testing.T) {
	t.Parallel()

	pgtest, err := New(ConnConfig{
		Driver:   testDriver,
		Username: "postgres",
		Password: "postgres",
		Host:     "localhost",
		Port:     "5432",
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		pgtest.Cleanup()
	})

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	migrationPath := filepath.Join(dir, "testdata")

	if err := pgtest.MigrateUP("testing_2", WithGoose(migrationPath)); err != nil {
		t.Fatal(err)
	}

	schemaConn, err := pgtest.AcquireConnWithNewSchema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		schemaConn.Cleanup()
	})

	var newChemaTables []string
	var baselineTables []string

	// Compare table list in public schema and baseline schema.
	getTablesQuery := fmt.Sprintf(`SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN('public', '%s')`, schemaConn.SchemaName())
	if err := pgtest.RunQuery(context.Background(), getTablesQuery, func(rc *postgres.RowsCompat) error {
		var schemaName string
		var tableName string

		if err := rc.Scan(&schemaName, &tableName); err != nil {
			return err
		}

		switch schemaName {
		case schemaConn.SchemaName():
			newChemaTables = append(newChemaTables, tableName)
		case "baseline":
			baselineTables = append(baselineTables, tableName)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	slices.Sort[[]string](newChemaTables)
	slices.Sort[[]string](baselineTables)

	if diff := cmp.Diff(baselineTables, newChemaTables); diff != "" {
		t.Fatalf("(-want/+got)\n%s", diff)
	}
}

// TestRandomSchemaName tests whether the schema name is random after 100_000 iteration.
func TestRandomSchemaName(t *testing.T) {
	t.Skip()

	pgtest, err := New(ConnConfig{
		Driver:   testDriver,
		Username: "postgres",
		Password: "postgres",
		Host:     "localhost",
		Port:     "5432",
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := make(map[string]struct{})
	for i := 0; i < 100_000; i++ {
		schemaName := randomSchemaName()
		if _, ok := entries[schemaName]; ok {
			t.Fatalf("schema name %s already exist", schemaName)
		}
		entries[schemaName] = struct{}{}

		if err := createSchema(pgtest.origin, schemaName); err != nil {
			t.Log(schemaName)
			t.Fatal(err)
		}
	}
}
