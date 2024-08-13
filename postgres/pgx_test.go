package postgres

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCopyFrom(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	// Dont' test the feature if pgx driver is not used.
	if !testPG.IsPgx() {
		t.SkipNow()
	}

	sqlTempTable := "CREATE TABLE IF NOT EXISTS copy_pgx(a int NOT NULL, b text NOT NULL);"
	sqlSelectAllData := "SELECT a,b FROM copy_pgx ORDER BY a"
	sqlTruncate := "TRUNCATE TABLE copy_pgx"
	sqlDrop := "DROP TABLE copy_pgx"

	type tableStruct struct {
		A int
		B string
	}

	_, err := testPG.Exec(context.Background(), sqlTempTable)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		testPG.Exec(context.Background(), sqlDrop)
	})

	tests := []struct {
		name        string
		columns     []string
		values      [][]interface{}
		expectDatas []tableStruct
	}{
		{
			"one row",
			[]string{"a", "b"},
			[][]interface{}{
				{1, "one"},
			},
			[]tableStruct{
				{
					A: 1,
					B: "one",
				},
			},
		},
		{
			"multiple rows",
			[]string{"a", "b"},
			[][]interface{}{
				{1, "one"},
				{2, "two"},
				{3, "three"},
				{4, "four"},
				{5, "five"},
			},
			[]tableStruct{
				{
					A: 1,
					B: "one",
				},
				{
					A: 2,
					B: "two",
				},
				{
					A: 3,
					B: "three",
				},
				{
					A: 4,
					B: "four",
				},
				{
					A: 5,
					B: "five",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows, err := testPG.CopyFromRows(context.Background(), "copy_pgx", test.columns, test.values)
			if err != nil {
				t.Fatal(err)
			}

			expectRows := int64(len(test.values))
			if rows != expectRows {
				t.Fatalf("expecting %d rows but got %d", expectRows, rows)
			}

			data := make([]tableStruct, 0)
			err = testPG.RunQuery(context.Background(), sqlSelectAllData, func(r *RowsCompat) error {
				d := tableStruct{}
				err := r.Scan(
					&d.A,
					&d.B,
				)
				if err == nil {
					data = append(data, d)
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.expectDatas, data); diff != "" {
				t.Fatalf("(-want/+got)\n%s", diff)
			}

			if _, err := testPG.Exec(context.Background(), sqlTruncate); err != nil {
				t.Fatal(err)
			}
		})
	}
}
