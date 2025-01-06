package pgtest

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/studio-asd/pkg/postgres"
)

var (
	pgt    *PGTest
	pgconn *postgres.Postgres
	dbName = "pgtest"
	schema = `
DROP TABLE IF EXISTS testing;

CREATE TABLE IF NOT EXISTS testing (
	testing_id varchar PRIMARY KEY,
	created_at timestamptz NOT NULL
);
`
)

func TestMain(m *testing.M) {
	dsn := "postgres://postgres:postgres@localhost:5432/"
	if err := CreateDatabase(context.Background(), dsn, dbName, true); err != nil {
		log.Fatal(err)
	}

	pgconn, err := postgres.Connect(
		context.Background(),
		postgres.ConnectConfig{
			Driver:   "postgres",
			Username: "postgres",
			Password: "postgres",
			Host:     "localhost",
			Port:     "5432",
			DBName:   dbName,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	pgt = New()
	// Apply the schema for the test.
	if _, err := pgconn.Exec(context.Background(), schema); err != nil {
		log.Fatal(err)
	}

	code := m.Run()
	if err := pgt.Close(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestForkShcema(t *testing.T) {
	var conns []*postgres.Postgres
	t.Run("create fork schema", func(t *testing.T) {
		for i := 1; i <= 30; i++ {
			conn, err := pgt.ForkSchema(context.Background(), pgconn, "public")
			if err != nil {
				t.Fatal(err)
			}
			conns = append(conns, conn)
		}
	})

	t.Run("compare_fork_with_source", func(t *testing.T) {
		var newChemaTables []string
		var sourceTables []string

		for _, conn := range conns {
			searchPath := conn.SearchPath()[0]

			// Compare table list in public schema and baseline schema.
			getTablesQuery := fmt.Sprintf(`SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN('public', '%s')`, searchPath)
			if err := pgconn.RunQuery(context.Background(), getTablesQuery, func(rc *postgres.RowsCompat) error {
				var schemaName string
				var tableName string

				if err := rc.Scan(&schemaName, &tableName); err != nil {
					return err
				}

				switch schemaName {
				case searchPath:
					newChemaTables = append(newChemaTables, tableName)
				case "public":
					sourceTables = append(sourceTables, tableName)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(newChemaTables) == 0 {
				t.Fatal("new schema tables is empty")
			}
			if len(sourceTables) == 0 {
				t.Fatal("source tables is empty")
			}

			slices.Sort[[]string](newChemaTables)
			slices.Sort[[]string](sourceTables)

			if diff := cmp.Diff(sourceTables, newChemaTables); diff != "" {
				t.Fatalf("(-want/+got)\n%s", diff)
			}
		}
	})
}
