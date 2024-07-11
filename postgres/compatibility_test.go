package postgres

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTxPreapredStatement(t *testing.T) {
	t.Parallel()

	type data struct {
		A int
		B int
	}
	expect := data{
		A: 1,
		B: 2,
	}
	got := data{}

	tableQuery := "CREATE TABLE transact_prepared(a INT, b INT);"
	if _, err := testPG.Exec(context.Background(), tableQuery); err != nil {
		t.Fatal(err)
	}

	err := testPG.Transact(context.Background(), sql.LevelReadCommitted, func(ctx context.Context, p *Postgres) error {
		sc, err := p.Prepare(
			context.Background(),
			"INSERT INTO transact_prepared VALUES($1,$2)",
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sc.Exec(context.Background(), 1, 2); err != nil {
			t.Fatal(err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r := testPG.QueryRow(context.Background(), "SELECT a,b FROM transact_prepared")
	if err := r.Scan(&got.A, &got.B); err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(expect, got); diff != "" {
		t.Fatalf("(-want/+got)\n%s", diff)
	}
}
