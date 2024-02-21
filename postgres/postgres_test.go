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
