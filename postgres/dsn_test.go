package postgres

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPostgres(t *testing.T) {
	t.Parallel()

	expected := DSN{
		Username:     "bob",
		Password:     "secret",
		Host:         "1.2.3.4",
		Port:         "5432",
		DatabaseName: "mydb",
		SSLMode:      "verify-full",
		SearchPath:   "public",
	}
	m, err := ParseDSN("postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full&search_path=public")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(expected, m); diff != "" {
		t.Fatalf("(-want/+got)\n%s", diff)
	}

	expected = DSN{
		Username:        "dog",
		Password:        "zMWmQz26GORmgVVKEbEl",
		Host:            "master-db-master-active.postgres.service.consul",
		Port:            "5433",
		DatabaseName:    "dogdatastaging",
		ApplicationName: "trace-api",
	}
	dsn := "password=zMWmQz26GORmgVVKEbEl dbname=dogdatastaging application_name=trace-api port=5433 host=master-db-master-active.postgres.service.consul user=dog"
	m, err = ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(expected, m); diff != "" {
		t.Fatalf("(-want/+got)\n%s", diff)
	}
}
