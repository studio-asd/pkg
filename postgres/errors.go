package postgres

import (
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
)

// PostgreSQL error codes and definition taken from https://www.postgresql.org/docs/current/errcodes-appendix.html.
var (
	ErrConnDone = sql.ErrConnDone
	ErrTxDone   = sql.ErrTxDone
	ErrNoRows   = sql.ErrNoRows
	// Class 22 - Data Exception.
	ErrDivisionByZero            = errors.New("[code: 22012] divison by zero error")
	ErrBadCopyFileFormat         = errors.New("[code: 22P04] bad_copy_file_format error")
	ErrInvalidJSONText           = errors.New("[code: 22032] invalid json text error")
	ErrInvalidTextRepresentation = errors.New("[code: 22P02] invalid text representation error")
	// Class 23 - Integrity Constraint Violation.
	ErrNotNullViolation        = errors.New("[code: 23502] not null violation error")
	ErrForeignKeyViolation     = errors.New("[code: 23503] foreign key violation error")
	ErrUniqueViolation         = errors.New("[code: 23505] unique violation error")
	ErrReadOnlySQLTransaction  = errors.New("[code: 25006] read only sql transaction error")
	ErrInvalidSQLStatementName = errors.New("[code: 26000] invalid sql statement name error")
	ErrTransactionRollback     = errors.New("[code: 40000] transaction rollback error")
	ErrSerializatoinFailure    = errors.New("[code: 40001] serialization failure error")
	ErrDeadlockDetected        = errors.New("[code: 40P01] deadlock detected error")
)

// postgresErrCodeToError converts the error code we got from the PostgreSQL database to our internal error.
func postgresErrCodeToError(code string) (error, bool) {
	switch code {
	case "22012":
		return ErrDivisionByZero, true
	case "22P04":
		return ErrBadCopyFileFormat, true
	case "22032":
		return ErrInvalidJSONText, true
	case "23502":
		return ErrNotNullViolation, true
	case "23503":
		return ErrForeignKeyViolation, true
	case "23505":
		return ErrUniqueViolation, true
	case "25006":
		return ErrReadOnlySQLTransaction, true
	case "26000":
		return ErrInvalidSQLStatementName, true
	case "40000":
		return ErrTransactionRollback, true
	case "40001":
		return ErrSerializatoinFailure, true
	case "40P01":
		return ErrDeadlockDetected, true
	default:
		return nil, false
	}
}

// IsPQError returns whether the error is a PostgreSQL internal error or not.
func IsPQError(err error) (string, bool) {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		return pgerr.Code, true
	}
	var pqerr *pq.Error
	if errors.As(err, &pqerr) {
		return string(pqerr.Code), true
	}
	return "", false
}

// isPQError is the internal version of IsPQError. This function won't check both pgx and libpq error as the packge
// already know what package is being used.
func isPQError(err error, isPgx bool) (string, bool) {
	if isPgx {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) {
			return pgerr.Code, true
		}
		return "", false
	}
	var pqerr *pq.Error
	if errors.As(err, &pqerr) {
		return string(pqerr.Code), true
	}
	return "", false
}

// tryErrToPostgresError converts PostgreSQL error with codes to the internal error type.
func tryErrToPostgresError(err error, isPgx bool) (string, error) {
	if err == nil {
		return "", nil
	}
	code, ok := isPQError(err, isPgx)
	if !ok {
		return "", err
	}
	pgErr, ok := postgresErrCodeToError(code)
	if !ok {
		return code, err
	}
	// Join the errors so errors.Is and errors.As behave the same with the first error while keeping the internal
	// type to be checked via errors.Is.
	// //
	// This have drawbacks where we have a duplicated error text. For example:
	//
	// [code: 23505] unique violation error
	// [code: 23505] unique violation error
	// ERROR: duplicate key value violates unique constraint "accounts_pkey" (SQLSTATE 23505)
	//
	// The first one is from the package's internal error, and the second one is from the PostgreSQL driver error.
	pgErr = errors.Join(pgErr, err)
	return code, pgErr
}
