package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pressly/goose/v3"

	"github.com/albertwidi/pkg/postgres"
)

type MigrationFunc func(ctx context.Context, conn *postgres.Postgres) error

func WithGoose(migrationPath string) MigrationFunc {
	return func(ctx context.Context, pg *postgres.Postgres) error {
		stat, err := os.Stat(migrationPath)
		if err != nil {
			return err
		}
		if !stat.IsDir() {
			return fmt.Errorf("%s is not a directory", migrationPath)
		}

		sqlDBConn := pg.StdlibDB()
		if err := goose.Up(sqlDBConn, migrationPath, goose.WithAllowMissing()); err != nil {
			err = fmt.Errorf("gooseUp: %w", err)
			return err
		}
		// If pgx is used, then it will create a new connection. Since we don't need it anymore we can close it.
		if pg.IsPgx() {
			sqlDBConn.Close()
		}
		return nil
	}
}

func WithSQLSchema(sqlSchemaPath string) MigrationFunc {
	return func(ctx context.Context, pg *postgres.Postgres) error {
		stat, err := os.Stat(sqlSchemaPath)
		if err != nil {
			return err
		}
		if stat.IsDir() {
			return fmt.Errorf("%s is a directory, please provide a .sql file", sqlSchemaPath)
		}

		extension := filepath.Ext(sqlSchemaPath)
		if extension != ".sql" {
			return fmt.Errorf("expecting .sql extension but got %s", extension)
		}

		out, err := os.ReadFile(sqlSchemaPath)
		if err != nil {
			return err
		}
		_, err = pg.Exec(ctx, string(out))
		return err
	}
}
