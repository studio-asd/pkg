package postgres

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

const (
	testTimeout = time.Second * 10
	testPGName  = "postgres_test"
)

var testPG *Postgres
var testDriver string

func TestMain(m *testing.M) {
	for _, driver := range []string{"postgres", "pgx"} {
		fmt.Println("==========")
		fmt.Printf("Driver: %s\n", driver)
		fmt.Println("==========")
		testDriver = driver

		var err error
		originConn, err := Connect(context.Background(), ConnectConfig{
			Driver:   driver,
			Username: "postgres",
			Password: "postgres",
			Host:     "localhost",
			Port:     "5432",
		})
		if err != nil {
			log.Fatal(err)
		}
		testPG, err = CreateDatabase(context.Background(), originConn, testPGName)
		if err != nil {
			log.Fatal(err)
		}
		if err := originConn.Close(); err != nil {
			log.Fatal(err)
		}

		code := m.Run()
		if err := testPG.Close(); err != nil {
			log.Fatal(err)
		}
		if code != 0 {
			os.Exit(code)
		}
	}
}

func TestGenerateURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config ConnectConfig
		expect string
	}{
		{
			name: "simple configuration",
			config: ConnectConfig{
				Driver:   "postgres",
				Username: "username",
				Password: "password",
				Host:     "localhost",
				Port:     "5432",
				DBName:   "testing",
				SSLMode:  "disable",
			},
			expect: "postgres://username:password@localhost:5432/testing?sslmode=disable",
		},
		{
			name: "with search path",
			config: ConnectConfig{
				Driver:     "postgres",
				Username:   "username",
				Password:   "password",
				Host:       "localhost",
				Port:       "5432",
				DBName:     "testing",
				SSLMode:    "disable",
				SearchPath: "public",
			},
			expect: "postgres://username:password@localhost:5432/testing?sslmode=disable&searchPath=public",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			url, _, err := test.config.DSN()
			if err != nil {
				t.Fatal(err)
			}
			if url != test.expect {
				t.Fatalf("expecting:\n%s\nbut got\n%s", test.expect, url)
			}
		})
	}
}

func TestConnect(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config ConnectConfig
		dbName string
		setup  func(context.Context, string, *testing.T)
	}{
		{
			name: "without schema",
			config: ConnectConfig{
				Driver:   testDriver,
				Username: "postgres",
				Password: "postgres",
				Host:     "localhost",
				Port:     "5432",
			},
			dbName: "test_connect_1",
			setup: func(ctx context.Context, name string, t *testing.T) {
				t.Helper()
				pg, err := CreateDatabase(ctx, testPG, name)
				if err != nil {
					t.Fatal(err)
				}
				if err := pg.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "with schema",
			config: ConnectConfig{
				Driver:     testDriver,
				Username:   "postgres",
				Password:   "postgres",
				Host:       "localhost",
				Port:       "5432",
				SearchPath: "testing2",
			},
			dbName: "test_connect_2",
			setup: func(ctx context.Context, name string, t *testing.T) {
				t.Helper()
				pg, err := CreateDatabase(ctx, testPG, name)
				if err != nil {
					t.Fatal(err)
				}
				_, err = pg.Exec(ctx, "CREATE SCHEMA testing2")
				if err != nil {
					t.Fatal(err)
				}
				if err := pg.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := test.config
			config.DBName = test.dbName

			test.setup(context.Background(), test.dbName, t)
			pg, err := Connect(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := pg.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
