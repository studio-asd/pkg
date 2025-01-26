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
func CreateDatabase(ctx context.Context, dsn string, recreateIfExists bool) error {
	checkTesting()

	pgDSN, err := postgres.ParseDSN(dsn)
	if err != nil {
		return err
	}
	if pgDSN.DatabaseName == "" {
		return errors.New("pg_test: database name cannot be empty inside the data source name")
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
		ok, err := isDatabaseExists(ctx, pg, pgDSN.DatabaseName)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}

	query := fmt.Sprintf("CREATE DATABASE %s", pgDSN.DatabaseName)
	_, err = pg.Exec(ctx, query)
	if err != nil && err != context.Canceled {
		// Skip to drop the database because we might don't want to drop the database for the whole test.
		if skipDrop != "" {
			return nil
		}
		// If we got an error it might be because the database is still exist.
		errDrop := dropDatabase(ctx, pg, pgDSN.DatabaseName)
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
	newConnConfig.DBName = pgDSN.DatabaseName
	newConn, err := postgres.Connect(ctx, newConnConfig)
	if err != nil {
		return err
	}
	defer newConn.Close()

	return createCloneSchemaFunc(ctx, newConn)
}

func CreateCloneSchemaFunc(ctx context.Context, dsn string) error {
	checkTesting()

	config, err := postgres.NewConfigFromDSN(dsn)
	if err != nil {
		return err
	}
	conn, err := postgres.Connect(ctx, config)
	if err != nil {
		return err
	}
	defer conn.Close()
	return createCloneSchemaFunc(ctx, conn)
}

func createCloneSchemaFunc(ctx context.Context, conn *postgres.Postgres) error {
	var exists bool
	// Check whether the clone schema function is already exists.
	query := "SELECT EXISTS(SELECT * FROM pg_proc WHERE proname = 'clone_schema');"
	row := conn.QueryRow(ctx, query)
	if err := row.Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}

	// Create the clone schema function using embedded cloneSQL.
	out, err := cloneSQL.ReadFile("clone_schema.sql")
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, string(out))
	if err != nil {
		return err
	}
	return nil
}

func DropDatabase(ctx context.Context, dsn string) error {
	checkTesting()

	pgDSN, err := postgres.ParseDSN(dsn)
	if err != nil {
		return err
	}
	if pgDSN.DatabaseName == "" {
		return errors.New("pg_test: database name is required in the data source name")
	}
	dbName := pgDSN.DatabaseName
	pgDSN.DatabaseName = ""
	config, err := pgDSN.BuildConfig()
	if err != nil {
		return err
	}
	pg, err := postgres.Connect(ctx, config)
	if err != nil {
		return err
	}
	defer pg.Close()
	return dropDatabase(ctx, pg, dbName)
}

func dropDatabase(ctx context.Context, pg *postgres.Postgres, name string) error {
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
