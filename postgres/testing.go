package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
)

//go:embed  clone_schema.sql
var cloneSQL embed.FS

const letters = "abcdefghijklmnopqrstuvwxyz"

// randString returns a random string with length of n.
func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))] // #nosec
	}
	return "pgtesting_" + string(b)
}

// CreateDatabase automatically and forcefully creates database using the current connection and creates a new connection
// to the new database.
//
// This function can only be used inside test.
func CreateDatabase(ctx context.Context, pg *Postgres, name string) (*Postgres, error) {
	if !testing.Testing() {
		panic("creating a new database is not allowed outside of test")
	}
	// Forcefully create the database by creating-dropping-creating database if error happen. We are not using IF NOT EXIST
	// because for test we usualy want a fresh database.
	query := fmt.Sprintf("CREATE DATABASE %s", name)
	_, err := pg.Exec(ctx, query)
	if err != nil && err != context.Canceled {
		// If we got an error it might be because the database is still exist.
		errDrop := DropDatabase(ctx, pg, name)
		if errDrop != nil {
			return nil, errors.Join(err, errDrop)
		}
		_, err = pg.Exec(ctx, query)
	}
	if err != nil {
		err = fmt.Errorf("failed to create database: %w", err)
		return nil, err
	}

	// Copy the configuration to the new configuration and change the database name to
	// connect to the newly created database.
	conf := *pg.config
	conf.DBName = name
	return Connect(ctx, conf)
}

func DropDatabase(ctx context.Context, pg *Postgres, name string) error {
	if !testing.Testing() {
		panic("dropping database is not allowed outside of test")
	}
	// If we use the same connection to connect to the database, then we should switch the connection first.
	if pg.config.DBName == name {
		if err := pg.Close(); err != nil {
			return err
		}
		var err error
		conf := *pg.config
		conf.DBName = ""
		pg, err = Connect(ctx, conf)
		if err != nil {
			return err
		}
	}
	query := fmt.Sprintf("DROP DATABASE %s", name)
	_, err := pg.Exec(ctx, query)
	if err != nil {
		err = fmt.Errorf("failed to drop database: %w", err)
	}
	return err
}

func ApplySchemaFile(ctx context.Context, pg *Postgres, sqlFile string) error {
	if !testing.Testing() {
		panic("applying schema can only be done inside testing")
	}

	stat, err := os.Stat(sqlFile)
	if err != nil {
		return err
	}
	if stat.IsDir() {
		return fmt.Errorf("%s is a directory", sqlFile)
	}

	f, err := os.Open(sqlFile)
	if err != nil {
		return err
	}
	out, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	_, err = pg.Exec(ctx, string(out))
	return err
}

// ForkConnWithNewSchema fork the connection to a new cloned schema. This operation doesn't close the original connection.
//
// This function can only be used inside test.
func ForkConnWithNewSchema(ctx context.Context, pg *Postgres, schemaName string) (*Postgres, error) {
	if !testing.Testing() {
		panic("forking connection with new schema can only be done inside testing")
	}
	if pg.originConn != nil {
		return nil, errors.New("cannot fork a new connection from a fork. Please use the origin connection")
	}
	if pg.config.DBName == "" {
		return nil, errors.New("can only fork connection to a new schema when connected to a database")
	}
	// If the schema name is empty, then we will use a random schema name.
	if schemaName == "" {
		schemaName = randString(10)
	}
	if schemaName == "public" || schemaName == "baseline" {
		return nil, errors.New("cannot use either public and baseline as the schema name")
	}

	// Create the clone schema function and clone the 'public' schema to 'baseline' schema. The 'baseline' schema will be
	// the source of truth of schema cloning.
	var errDo error
	pg.cloneOnce.Do(func() {
		// Create the clone schema function using embedded cloneSQL.
		out, err := cloneSQL.ReadFile("clone_schema.sql")
		if err != nil {
			errDo = err
			return
		}
		_, err = pg.Exec(ctx, string(out))
		if err != nil {
			errDo = err
			return
		}
		err = cloneSchema(pg, "public", "baseline")
		if err != nil {
			errDo = err
		}
	})
	if errDo != nil {
		return nil, errDo
	}

	// Clone the baseline schema to the designated schema. The function will automatically create the schema.
	if err := cloneSchema(pg, "baseline", schemaName); err != nil {
		return nil, err
	}

	newConn, err := Connect(ctx, pg.config.copy())
	if err != nil {
		return nil, err
	}
	if err := newConn.setDefaultSearchPath(ctx, schemaName); err != nil {
		return nil, err
	}
	// Set the origin connection, so we know this is a fork.
	newConn.originConn = pg
	// Set the new connection clone once with the origin clone once so we won't do it again in the same connection.
	newConn.cloneOnce = pg.cloneOnce
	return newConn, nil
}

// cloneSchema clones a new schema using clone_schema SQL function.
func cloneSchema(conn *Postgres, fromSchema, toSchema string) error {
	if !testing.Testing() {
		panic("clone schema can only be done inside testing")
	}
	query := fmt.Sprintf("SELECT clone_schema('%s', '%s', FALSE)", fromSchema, toSchema)
	_, err := conn.Exec(context.Background(), query)
	if err != nil {
		err = fmt.Errorf("cloneSchema: %w", err)
	}
	return err
}
