package postgres

import (
	"fmt"
	"os"
	"testing"
)

var testDriver string

func TestMain(t *testing.M) {
	drivers := []string{
		"postgres",
		"pgx",
	}

	for _, driver := range drivers {
		dr := driver
		testDriver = dr

		fmt.Println("==========")
		fmt.Printf("Driver: %s\n", dr)
		fmt.Println("==========")

		exitCode := t.Run()
		if exitCode > 0 {
			os.Exit(1)
		}
	}
}
