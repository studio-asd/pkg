package pgtest

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/studio-asd/pkg/postgres"
)

//go:embed  clone_schema.sql
var cloneSQL embed.FS

// skipDrop skips drop database when CreateDatabase function is invoked. By default, the function will drop the database if it is exists.
var skipDrop = os.Getenv("PGTEST_SKIP_DROP")

const letters = "abcdefghijklmnopqrstuvwxyz"

// randString returns a random string with length of n.
func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))] // #nosec
	}
	return "pgtesting_" + string(b)
}

func checkTesting() {
	if !testing.Testing() {
		panic("pg test can only be used inside testing.")
	}
}

// CreateDatabase automatically and forcefully creates a new database and 'clone_schema' function to clone one schema to another.
// The function is not protected by testing check because this might be used outside of the test itself. For example to create a
// test preparation for several services.
//
// WARNING: Please use this function carefully as by default, this will automatically drops the database if the database is exists.
// Please don't use this function out fo the test code. To not drop the database, PGTEST_SKIP_DROP environment variable need to be set.
func CreateDatabase(ctx context.Context, dsn, name string, recreateIfExists bool) error {
	pgDSN, err := postgres.ParseDSN(dsn)
	if err != nil {
		return err
	}
	pg, err := postgres.Connect(ctx, postgres.ConnectConfig{
		Driver:   "pgx",
		Username: pgDSN.Username,
		Password: pgDSN.Password,
		Host:     pgDSN.Host,
		Port:     pgDSN.Port,
		SSLMode:  pgDSN.SSLMode,
	})
	if err != nil {
		return err
	}
	// recreateIfExists forcefully create the database if the database is already exists, otherwise it will just ignore the
	// fact that the database is exists and continue.
	if !recreateIfExists {
		ok, err := isDatabaseExists(ctx, pg, name)
		if err != nil {
			fmt.Println("LAH")
			return err
		}
		if ok {
			return nil
		}
	}

	query := fmt.Sprintf("CREATE DATABASE %s", name)
	_, err = pg.Exec(ctx, query)
	if err != nil && err != context.Canceled {
		// Skip to drop the database because we might don't want to drop the database for the whole test.
		if skipDrop != "" {
			return nil
		}
		// If we got an error it might be because the database is still exist.
		errDrop := DropDatabase(ctx, pg, name)
		if errDrop != nil {
			return errors.Join(err, errDrop)
		}
		_, err = pg.Exec(ctx, query)
	}
	if err != nil {
		err = fmt.Errorf("failed to create database: %w", err)
		return err
	}

	newConnConfig := pg.Config()
	newConnConfig.DBName = name
	newConn, err := postgres.Connect(ctx, newConnConfig)
	if err != nil {
		return err
	}
	defer newConn.Close()

	// Create the clone schema function using embedded cloneSQL.
	out, err := cloneSQL.ReadFile("clone_schema.sql")
	if err != nil {
		return err
	}
	_, err = newConn.Exec(ctx, string(out))
	if err != nil {
		return err
	}
	return nil
}

func DropDatabase(ctx context.Context, pg *postgres.Postgres, name string) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", name)
	_, err := pg.Exec(ctx, query)
	if err != nil {
		err = fmt.Errorf("failed to drop database: %w", err)
	}
	return err
}

func isDatabaseExists(ctx context.Context, pg *postgres.Postgres, name string) (bool, error) {
	var gotName string
	query := fmt.Sprintf("SELECT datname FROM pg_database WHERE datname='%s'", name)
	row := pg.QueryRow(ctx, query)
	if err := row.Scan(&gotName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return gotName == name, nil
}

type PGTest struct {
	forkedConns []*postgres.Postgres
}

func New() *PGTest {
	checkTesting()
	return &PGTest{}
}

// Close closes all forked connections and original postgres connection of PGTest.
func (pg *PGTest) Close() error {
	var err error
	for _, conn := range pg.forkedConns {
		if errClose := conn.Close(); errClose != nil {
			err = errors.Join(err, errClose)
		}
	}
	return err
}

// ForkSchema forks a schema and fork it into another schema.
func (pg *PGTest) ForkSchema(ctx context.Context, conn *postgres.Postgres, sourceSchema string) (*postgres.Postgres, error) {
	checkTesting()
	if sourceSchema == "" {
		return nil, errors.New("source schema name cannot be empty")
	}
	// Reject the fork if its coming from a fork schema.
	if strings.HasPrefix(sourceSchema, "pgtesting") {
		return nil, errors.New("cannot fork a test schema")
	}

	forkName := randString(15)
	if err := cloneSchema(conn, sourceSchema, forkName); err != nil {
		return nil, err
	}

	config := conn.Config()
	config.SearchPath = forkName
	return postgres.Connect(ctx, config)
}

// cloneSchema clones a new schema using clone_schema SQL function.
func cloneSchema(conn *postgres.Postgres, fromSchema, toSchema string) error {
	checkTesting()
	query := fmt.Sprintf("SELECT clone_schema('%s', '%s', FALSE)", fromSchema, toSchema)
	_, err := conn.Exec(context.Background(), query)
	if err != nil {
		err = fmt.Errorf("cloneSchema: %w", err)
	}
	return err
}
