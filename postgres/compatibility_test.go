package postgres

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestTxPreparedStatmeent tests the prepared statement inside a session/connection to the PostgreSQL. Please note
// that prepared statments lives inside a session. So its doesn't matter if it actually being triggered inside the
// the transaction or not. Both prepared statements and transaction live inside a session/connection, so we can use
// prepared statements that not prepared inside the transaction.
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

	t.Run("in_transact", func(t *testing.T) {
		t.Parallel()

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
	})

	t.Run("outside_transact", func(t *testing.T) {
		t.Parallel()

		got := data{}
		tableQuery := "CREATE TABLE transact_prepared_2(a INT, b INT);"
		if _, err := testPG.Exec(context.Background(), tableQuery); err != nil {
			t.Fatal(err)
		}

		sc, err := testPG.Prepare(
			context.Background(),
			"INSERT INTO transact_prepared_2 VALUES($1,$2)",
		)
		if err != nil {
			t.Fatal(err)
		}

		err = testPG.Transact(context.Background(), sql.LevelReadCommitted, func(ctx context.Context, p *Postgres) error {
			if _, err := sc.Exec(context.Background(), 1, 2); err != nil {
				t.Fatal(err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		r := testPG.QueryRow(context.Background(), "SELECT a,b FROM transact_prepared_2")
		if err := r.Scan(&got.A, &got.B); err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(expect, got); diff != "" {
			t.Fatalf("(-want/+got)\n%s", diff)
		}
	})
}
