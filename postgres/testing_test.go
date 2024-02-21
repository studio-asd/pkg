package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
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

}
