package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
)

func TestErrorToPostgresError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errs   []error
		expect error
	}{
		{
			errs: []error{
				&pgconn.PgError{
					Code: "22012",
				},
				&pq.Error{
					Code: "22012",
				},
			},
			expect: ErrDivisionByZero,
		},
		{
			errs: []error{
				&pgconn.PgError{
					Code: "23505",
				},
				&pq.Error{
					Code: "23505",
				},
			},
			expect: ErrUniqueViolation,
		},
	}

	for _, test := range tests {
		t.Run(test.expect.Error(), func(t *testing.T) {
			t.Parallel()
			for _, err := range test.errs {
				gotErr := ErrToPostgresError(err)
				if !errors.Is(gotErr, test.expect) {
					t.Fatalf("expecting error %v but got %v", test.expect, gotErr)
				}
			}
		})
	}
}

// TestPostgreSQLError test the integration testing with the PostgreSQL database and check whether the library
// successfully give the expected error.
func TestPostgreSQLError(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
}
