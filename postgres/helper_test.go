// Helper contains modified codes from Go's pkgsite.
//
// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBulkInsert(t *testing.T) {
	t.Parallel()
	table := "test_bulk_insert"

	for _, test := range []struct {
		name           string
		columns        []string
		values         []interface{}
		conflictAction string
		wantErr        bool
		wantCount      int
		wantReturned   []string
	}{
		{
			name:      "test-one-row",
			columns:   []string{"colA"},
			values:    []interface{}{"valueA"},
			wantCount: 1,
		},
		{
			name:      "test-multiple-rows",
			columns:   []string{"colA"},
			values:    []interface{}{"valueA1", "valueA2", "valueA3"},
			wantCount: 3,
		},
		{
			name:    "test-invalid-column-name",
			columns: []string{"invalid_col"},
			values:  []interface{}{"valueA"},
			wantErr: true,
		},
		{
			name:    "test-mismatch-num-cols-and-vals",
			columns: []string{"colA", "colB"},
			values:  []interface{}{"valueA1", "valueB1", "valueA2"},
			wantErr: true,
		},
		{
			name:         "insert-returning",
			columns:      []string{"colA", "colB"},
			values:       []interface{}{"valueA1", "valueB1", "valueA2", "valueB2"},
			wantCount:    2,
			wantReturned: []string{"valueA1", "valueA2"},
		},
		{
			name:    "test-conflict",
			columns: []string{"colA"},
			values:  []interface{}{"valueA", "valueA"},
			wantErr: true,
		},
		{
			name:           "test-conflict-do-nothing",
			columns:        []string{"colA"},
			values:         []interface{}{"valueA", "valueA"},
			conflictAction: OnConflictDoNothing,
			wantCount:      1,
		},
		{
			// This should execute the statement
			// INSERT INTO series (path) VALUES ('''); TRUNCATE series CASCADE;)');
			// which will insert a row with path value:
			// '); TRUNCATE series CASCADE;)
			// Rather than the statement
			// INSERT INTO series (path) VALUES (''); TRUNCATE series CASCADE;));
			// which would truncate most tables in the database.
			name:           "test-sql-injection",
			columns:        []string{"colA"},
			values:         []interface{}{fmt.Sprintf("''); TRUNCATE %s CASCADE;))", table)},
			conflictAction: OnConflictDoNothing,
			wantCount:      1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			createQuery := fmt.Sprintf(`CREATE TABLE %s (
					colA TEXT NOT NULL,
					colB TEXT,
					PRIMARY KEY (colA)
				);`, table)

			_, err := testPG.Exec(ctx, createQuery)
			if err != nil {
				t.Fatal(err)
			}

			defer func() {
				dropTableQuery := fmt.Sprintf("DROP TABLE %s;", table)
				if _, err := testPG.Exec(ctx, dropTableQuery); err != nil {
					t.Fatal(err)
				}
			}()

			var returned []string
			if test.wantReturned == nil {
				err = testPG.BulkInsert(ctx, table, test.columns, test.values, test.conflictAction)
			} else {
				err = testPG.BulkInsertReturning(ctx, table, test.columns, test.values, test.conflictAction,
					[]string{"colA"}, func(rows *RowsCompat) error {
						var r string
						if err := rows.Scan(&r); err != nil {
							return err
						}
						returned = append(returned, r)
						return nil
					})
			}

			if test.wantErr && err == nil || !test.wantErr && err != nil {
				t.Errorf("got error %v, wantErr %t", err, test.wantErr)
			}
			if err != nil {
				return
			}

			if test.wantCount != 0 {
				var count int
				query := "SELECT COUNT(*) FROM " + table
				row := testPG.QueryRow(ctx, query)
				err := row.Scan(&count)
				if err != nil {
					t.Fatalf("testPG.queryRow(%q): %v", query, err)
				}
				if count != test.wantCount {
					t.Errorf("testPG.queryRow(%q) = %d; want = %d", query, count, test.wantCount)
				}
			}
			if test.wantReturned != nil {
				sort.Strings(returned)
				if !cmp.Equal(returned, test.wantReturned) {
					t.Errorf("returned: got %v, want %v", returned, test.wantReturned)
				}
			}
		})
	}
}

func TestBulkUpdate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	defer func(old int) { maxBulkUpdateArrayLen = old }(maxBulkUpdateArrayLen)
	maxBulkUpdateArrayLen = 5

	if _, err := testPG.Exec(ctx, `CREATE TABLE bulk_update (a INT, b INT)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := testPG.Exec(ctx, `DROP TABLE bulk_update`); err != nil {
			t.Fatal(err)
		}
	}()

	cols := []string{"a", "b"}
	var values []any
	for i := 0; i < 50; i++ {
		values = append(values, i, i)
	}
	err := testPG.Transact(ctx, sql.LevelDefault, func(ctx context.Context, tx *Postgres) error {
		return tx.BulkInsert(ctx, "bulk_update", cols, values, "")
	})
	if err != nil {
		t.Fatalf("failed to bulk_insert into bulk_update table with error: %v", err)
	}

	// Update all even values of column a.
	updateVals := make([][]any, 2)
	for i := 0; i < len(values)/2; i += 2 {
		updateVals[0] = append(updateVals[0], i)
		updateVals[1] = append(updateVals[1], -i)
	}

	err = testPG.Transact(ctx, sql.LevelDefault, func(ctx context.Context, tx *Postgres) error {
		return tx.BulkUpdate(ctx, "bulk_update", cols, []string{"INT", "INT"}, updateVals)
	})
	if err != nil {
		t.Fatalf("failed to bulk_update into bulk_udpate table with error: %v`", err)
	}

	err = testPG.RunQuery(ctx, `SELECT a, b FROM bulk_update`, func(rows *RowsCompat) error {
		var a, b int
		if err := rows.Scan(&a, &b); err != nil {
			return err
		}
		want := a
		if a%2 == 0 {
			want = -a
		}
		if b != want {
			t.Fatalf("a=%d: got %d, want %d", a, b, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
