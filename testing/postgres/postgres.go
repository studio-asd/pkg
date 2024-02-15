package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/albertwidi/pkg/postgres"
)

//go:embed  clone_schema.sql
var cloneSQL embed.FS

type ConnConfig struct {
	Driver   string
	Username string
	Password string
	Host     string
	Port     string
	SSLMode  string
}

type PGTest struct {
	*postgres.Postgres
	origin *postgres.Postgres

	schemaName string

	mu       sync.Mutex
	migrated bool
	dbName   string
}

func NewFromDSN(dsn string) (*PGTest, error) {
	kv, err := postgres.PostgresDSN(dsn)
	if err != nil {
		return nil, err
	}
	if kv["dbname"] != "" {
		return nil, errors.New("require empty database name for testing purpose")
	}

	pgConfig := ConnConfig{
		Driver:   "postgres",
		Username: kv["user"],
		Password: kv["password"],
		Host:     kv["host"],
		Port:     kv["port"],
		SSLMode:  kv["sslmode"],
	}
	return New(pgConfig)
}

// New creates a new instance of PGTest for testing purposes.
func New(config ConnConfig) (*PGTest, error) {
	pgConfig := postgres.ConnectConfig{
		Driver:   config.Driver,
		Username: config.Username,
		Password: config.Password,
		Host:     config.Host,
		Port:     config.Port,
		SSLMode:  config.SSLMode,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pg, err := postgres.Connect(ctx, pgConfig)
	if err != nil {
		return nil, err
	}
	return &PGTest{
		origin: pg,
		// Set the schema name to public because we will always use public schema in initial connection.
		schemaName: "public",
	}, nil
}

func (p *PGTest) SchemaName() string {
	return p.schemaName
}

func (p *PGTest) Cleanup() error {
	// If the origin connection is not exist then something must go wrong, so we can exit early.
	if p.origin == nil {
		return nil
	}
	// Drop the database whatever happen.
	defer func() {
		dropDatabase(p.origin, p.dbName)
		p.origin.Close()
	}()

	// This means we fail to migrate the schema, so we can just exit.
	if p.Postgres == nil {
		return nil
	}
	if err := p.Postgres.Close(); err != nil {
		return err
	}
	if err := terminateAllConnections(p.origin); err != nil {
		slog.Error("failed to terminate all connections", slog.String("error", err.Error()))
	}
	return nil
}

// MigrateUP create database and migrates all the schema for the database. While the migration is being executed for the designated database
// a specific schema called 'baseline' will be created as the schema baseline.
//
// Please note that the database connection will automatically switched to the new database.
//
// The steps are:
//  1. Create a new database if not exists.
//  2. Create a new connection to the new database based on the previous configuration.
//  3. Execute the migration using goose.
//  4. Apply the clone_schema function.
//  5. Create a new schema called baseline.
func (p *PGTest) MigrateUP(dbName string, migrationFn MigrationFunc) error {
	if dbName == "" {
		return errors.New("database name cannot be empty")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.migrated {
		return errors.New("migration is already running")
	}

	// Create the database as we will always running from clean slate.
	if err := createDatabase(p.origin, dbName); err != nil {
		return err
	}

	copyConf := p.origin.Config()
	copyConf.DBName = dbName
	newConn, err := postgres.Connect(context.Background(), copyConf)
	if err != nil {
		err = fmt.Errorf("failed to create new connection: %w", err)
		return err
	}

	// Do the migration through migration function.
	if err := migrationFn(context.Background(), newConn); err != nil {
		return fmt.Errorf("migrateUP: %w", err)
	}

	// Create the clone schema function using embedded cloneSQL.
	out, err := cloneSQL.ReadFile("clone_schema.sql")
	if err != nil {
		return err
	}
	_, err = newConn.Exec(context.Background(), string(out))
	if err != nil {
		return err
	}
	err = cloneSchema(newConn, "public", "baseline")
	if err != nil {
		return err
	}

	p.Postgres = newConn
	p.dbName = dbName
	return nil
}

// AcquireConnWithNewSchema acquires new connection and clones the 'baseline' schema to the new 'schema'.
func (p *PGTest) AcquireConnWithNewSchema(ctx context.Context) (*PGTest, error) {
	schemaName := randomSchemaName()
	if err := createSchema(p.Postgres, schemaName); err != nil {
		return nil, err
	}

	// Clone the baseline schema to the new schema name.
	if err := cloneSchema(p.Postgres, "baseline", schemaName); err != nil {
		return nil, err
	}

	conf := p.Postgres.Config()
	newConn, err := postgres.Connect(context.Background(), conf)
	if err != nil {
		return nil, err
	}
	// Change the default search path for the current connection to the new schema.
	if err := newConn.SetDefaultSearchPath(ctx, schemaName); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return &PGTest{
		Postgres: newConn,
		origin:   p.origin,
		migrated: p.migrated,
		dbName:   p.dbName,
	}, nil
}

// createDatabase creates a new database. If the database is still exist, the function will try to drop the database
// and re-create the database.
func createDatabase(conn *postgres.Postgres, dbName string) (err error) {
	defer func() {
		if err != nil {
			return
		}
		slog.Info(fmt.Sprintf("database %s created successfully", dbName))
	}()
	slog.Info(fmt.Sprintf("creating database %s", dbName))

	query := fmt.Sprintf("CREATE DATABASE %s;", dbName)
	_, err = conn.Exec(context.Background(), query)
	if err != nil {
		slog.Info(fmt.Sprintf("failed to create database %s with error %v, retryting", dbName, err))
		// The error might be coming from the database is still exist, we should try to drop
		// the database and recreate.
		errDrop := dropDatabase(conn, dbName)
		if errDrop != nil {
			return errDrop
		}
		_, err = conn.Exec(context.Background(), query)
	}
	// If we still facing an error then we need to enrich the error string.
	if err != nil {
		err = fmt.Errorf("createDatabase: %w", err)
	}
	return err
}

// dropDatabase drops the entire database using DROP DATABASE. If the current database connnection is the same
// with the database we want to drop, the function will automatically switch the connection to the 'postgres' database
// and drop the designated table.
func dropDatabase(conn *postgres.Postgres, dbName string) error {
	var err error
	if dbName == "" {
		return errors.New("database name cannot be empty")
	}

	// The database connection is the same as the database name, this means we reuse the
	// connection to drop the database. We need to switch to another connection and
	// close the current connection to succeed.
	if conn.Config().DBName == dbName {
		// To drop a databse, all connections must be terminated.
		if err := terminateAllConnections(conn); err != nil {
			return err
		}

		// Close the initial connection because we no-longer needed it.
		if err := conn.Close(); err != nil {
			return err
		}

		conf := conn.Config()
		conf.DBName = ""
		conn, err = postgres.Connect(context.Background(), conf)
		if err != nil {
			return err
		}
	}

	slog.Info(fmt.Sprintf("dropping database %s", dbName))
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s;", dbName)
	_, err = conn.Exec(context.Background(), query)
	if err != nil {
		err = fmt.Errorf("dropDatabase: %w. Query: %s", err, query)
	}
	return err
}

func createSchema(conn *postgres.Postgres, schemaName string) error {
	query := fmt.Sprintf("CREATE SCHEMA %s;", schemaName)
	_, err := conn.Exec(context.Background(), query)
	if err != nil {
		err = fmt.Errorf("createSchema: %w", err)
	}
	return err
}

// dropSchema drops a schema based on the schema name.
func dropSchema(conn *postgres.Postgres, schemaName string) error {
	// When dropping schema, it is possible our search path is still on the same schema. Just to be safe,
	// we need to switch to the public schema.
	if slices.Contains[[]string, string](conn.SearchPath(), schemaName) && schemaName != "public" {
		if err := conn.SetDefaultSearchPath(context.Background(), "public"); err != nil {
			return err
		}
	}

	query := fmt.Sprintf("DROP SCHEMA %s CASCADE;", schemaName)
	_, err := conn.Exec(context.Background(), query)
	if err != nil {
		err = fmt.Errorf("dropSchema: %w", err)
	}
	return err
}

// cloneSchema clones a new schema using clone_schema SQL function.
func cloneSchema(conn *postgres.Postgres, fromSchema, toSchema string) error {
	query := fmt.Sprintf("SELECT clone_schema('%s', '%s', FALSE)", fromSchema, toSchema)
	_, err := conn.Exec(context.Background(), query)
	if err != nil {
		err = fmt.Errorf("cloneSchema: %w", err)
	}
	return err
}

// terminateAllConnections terminates connections in the current database based on the session in the connection.
func terminateAllConnections(conn *postgres.Postgres) error {
	query := `
	SELECT pid, pg_terminate_backend(pid)
	FROM pg_stat_activity
	WHERE datname = current_database() AND pid <> pg_backend_pid();
	`

	_, err := conn.Exec(context.Background(), query)
	return err
}

// randomSchemaName returns short and random schema name using base64 raw-url-encoding and removing (-,_).
func randomSchemaName() string {
	return randString(10)
}
