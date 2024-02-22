package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/go-cmp/cmp"
)

func TestCreateDropDatabase(t *testing.T) {
	t.Parallel()

	t.Run("create_drop_create", func(t *testing.T) {
		t.Parallel()

		conf := *testPG.config
		conf.DBName = ""
		originConn, err := Connect(context.Background(), conf)
		if err != nil {
			t.Fatal(err)
		}
		defer originConn.Close()

		pg, err := CreateDatabase(context.Background(), originConn, "testing_1")
		if err != nil {
			t.Fatal(err)
		}
		// Dropping the database with the connection that creates the database. The connection should be closed as database
		// is dropped.
		if err := DropDatabase(context.Background(), pg, "testing_1"); err != nil {
			t.Fatal(err)
		}
		if !pg.closed {
			t.Fatal("expecting connection to be closed")
		}
	})

	t.Run("create_create", func(t *testing.T) {
		t.Parallel()

		conf := *testPG.config
		conf.DBName = ""
		originConn, err := Connect(context.Background(), conf)
		if err != nil {
			t.Fatal(err)
		}
		defer originConn.Close()

		pg, err := CreateDatabase(context.Background(), originConn, "testing_2")
		if err != nil {
			t.Fatal(err)
		}
		if err := pg.Close(); err != nil {
			t.Fatal(err)
		}
		pg, err = CreateDatabase(context.Background(), originConn, "testing_2")
		if err != nil {
			t.Fatal(err)
		}
		if err := pg.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestApplySchemaFromFile(t *testing.T) {
	t.Parallel()

	schema := `
DROP TABLE IF EXISTS testing;

CREATE TABLE IF NOT EXISTS testing (
	testing_id varchar PRIMARY KEY,
	created_at timestamptz NOT NULL
);
`

	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	f, err := os.Create(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte(schema))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(schemaPath)

	if err := ApplySchemaFile(context.Background(), testPG, schemaPath); err != nil {
		t.Fatal(err)
	}
	// Do a simple select.
	query := "SELECT testing_id, created_at FROM testing"

	row := testPG.QueryRow(context.Background(), query)
	var (
		id      string
		created time.Time
	)
	if err := row.Scan(&id, &created); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
}

func TestForkConnWithNewSchema(t *testing.T) {
	t.Parallel()

	dbName := "testing_fork_schema"
	schema := `
DROP TABLE IF EXISTS testing;

CREATE TABLE IF NOT EXISTS testing (
	testing_id varchar PRIMARY KEY,
	created_at timestamptz NOT NULL
);
`

	conf := testPG.config.copy()
	conf.DBName = ""
	originConn, err := Connect(context.Background(), conf)
	if err != nil {
		t.Fatal(err)
	}

	pg, err := CreateDatabase(context.Background(), originConn, dbName)
	if err != nil {
		t.Fatal(err)
	}
	if err := originConn.Close(); err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	f, err := os.Create(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte(schema))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(schemaPath)

	if err := ApplySchemaFile(context.Background(), pg, schemaPath); err != nil {
		t.Fatal(err)
	}

	var forks []*Postgres
	for i := 0; i < 1; i++ {
		forkConn, err := ForkConnWithNewSchema(context.Background(), pg, "")
		if err != nil {
			t.Fatal(err)
		}
		forks = append(forks, forkConn)
	}
	// Even if we got an error, we still need to close all connections.
	t.Cleanup(func() {
		for _, forkConn := range forks {
			forkConn.Close()
		}
	})

	t.Run("compare_public_baseline", func(t *testing.T) {
		var publicTables []string
		var baselineTables []string

		// Compare table list in public schema and baseline schema.
		getTablesQuery := "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN('public', 'baseline');"
		if err := pg.RunQuery(context.Background(), getTablesQuery, func(rc *RowsCompat) error {
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

		if len(publicTables) == 0 {
			t.Fatal("public tables is empty")
		}
		if len(baselineTables) == 0 {
			t.Fatal("baseline tables is empty")
		}

		slices.Sort[[]string](publicTables)
		slices.Sort[[]string](baselineTables)

		if diff := cmp.Diff(baselineTables, publicTables); diff != "" {
			t.Fatalf("(-want/+got)\n%s", diff)
		}
	})

	for _, forkConn := range forks {
		fc := forkConn
		t.Run(fmt.Sprintf("compare_baseline_%s", forkConn.searchPath), func(t *testing.T) {
			var newChemaTables []string
			var baselineTables []string

			// Compare table list in public schema and baseline schema.
			getTablesQuery := fmt.Sprintf(`SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN('baseline', '%s')`, fc.searchPath)
			if err := pg.RunQuery(context.Background(), getTablesQuery, func(rc *RowsCompat) error {
				var schemaName string
				var tableName string

				if err := rc.Scan(&schemaName, &tableName); err != nil {
					return err
				}

				switch schemaName {
				case fc.searchPath:
					newChemaTables = append(newChemaTables, tableName)
				case "baseline":
					baselineTables = append(baselineTables, tableName)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(newChemaTables) == 0 {
				t.Fatal("new schema tables is empty")
			}
			if len(baselineTables) == 0 {
				t.Fatal("baseline tables is empty")
			}

			slices.Sort[[]string](newChemaTables)
			slices.Sort[[]string](baselineTables)

			if diff := cmp.Diff(baselineTables, newChemaTables); diff != "" {
				t.Fatalf("(-want/+got)\n%s", diff)
			}
		})
	}

	t.Run("close_all_forks", func(t *testing.T) {
		var schemaNames []string
		for _, fork := range forks {
			if err := fork.Close(); err != nil {
				t.Fatal(err)
			}
			schemaNames = append(schemaNames, fork.searchPath)
		}
		// Check further whether all schemas are deleted. We delete the forked conn schema when closing the connection.
		query, args, err := squirrel.Select("schema_name").
			From("information_schema.schemata").
			Where(squirrel.Eq{"schema_name": schemaNames}).
			PlaceholderFormat(squirrel.Dollar).
			ToSql()
		if err != nil {
			t.Fatal(err)
		}

		var got []string
		err = pg.RunQuery(context.Background(), query, func(rc *RowsCompat) error {
			var schemaName string
			if err := rc.Scan(&schemaName); err != nil {
				return err
			}
			got = append(got, schemaName)
			return nil
		}, args...)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatal("expecting 0 schemas left")
		}
	})
}
