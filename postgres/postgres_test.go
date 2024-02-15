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

func TestMain(m *testing.M) {
	for _, driver := range []string{"postgres", "pgx"} {
		fmt.Println("==========")
		fmt.Printf("Driver: %s\n", driver)
		fmt.Println("==========")

		var err error
		testPG, err = Connect(context.Background(), ConnectConfig{
			Driver:   driver,
			Username: "postgres",
			Password: "postgres",
			Host:     "localhost",
			Port:     "5432",
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := createDB(testPG); err != nil {
			log.Fatal(err)
		}

		code := m.Run()
		if code != 0 {
			os.Exit(code)
		}

		if err := testPG.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func createDB(pg *Postgres) error {
	query := fmt.Sprintf("CREATE DATABASE %s;", testPGName)
	_, err := pg.Exec(context.Background(), query)
	if err != nil {
		// The error might be coming from the database is still exist, we should try to drop
		// the database and recreate.
		dropQuery := fmt.Sprintf("DROP DATABASE IF EXISTS %s;", testPGName)
		_, errDrop := pg.Exec(context.Background(), dropQuery)
		if err != nil {
			err = fmt.Errorf("dropDatabase: %w. Query: %s", err, query)
		}
		if errDrop != nil {
			return errDrop
		}
		_, err = pg.Exec(context.Background(), query)
	}
	// If we still facing an error then we need to enrich the error string.
	if err != nil {
		err = fmt.Errorf("createDatabase: %w", err)
	}
	return err
}
