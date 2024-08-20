package resources

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPostgresConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config PostgresResourcesConfig
		expect PostgresResourcesConfig
		error  bool
	}{
		{
			name:   "Postgres default config",
			config: PostgresResourcesConfig{},
			expect: PostgresResourcesConfig{
				PostgresOverrideableConfig: PostgresOverrideableConfig{
					ConnMaxLifetime: Duration(defaultPostgresConnMaxLifetime),
					ConnMaxIdleTime: Duration(defaultPostgresConnMaxIdleTime),
					MaxOpenConns:    defaultPostgresMaxOpenConnections,
					MaxIdleConns:    defaultPostgresMaxIdleConnections,
					MaxRetry:        defaultPostgresRetry,
					RetryDelay:      Duration(defaultPostgresRetryDelay),
				},
			},
			error: false,
		},
		{
			name: "Postgres config with no name",
			config: PostgresResourcesConfig{
				PostgresConnections: []PostgresConnConfig{{}},
			},
			expect: PostgresResourcesConfig{},
			error:  true,
		},
		{
			name: "Postgres overridable config",
			config: PostgresResourcesConfig{
				PostgresConnections: []PostgresConnConfig{
					{
						Name: "testdb1",
						PrimaryDB: PostgresConfig{
							DSN: "postgres://postgres:postgres@localhost:5432?sslmode=disable",
						},
						SecondaryDB: PostgresConfig{
							DSN: "postgres://postgres:postgres@localhost:5432?sslmode=disable",
						},
					},
				},
			},
			expect: PostgresResourcesConfig{
				PostgresOverrideableConfig: PostgresOverrideableConfig{
					ConnMaxLifetime: Duration(defaultPostgresConnMaxLifetime),
					ConnMaxIdleTime: Duration(defaultPostgresConnMaxIdleTime),
					MaxOpenConns:    defaultPostgresMaxOpenConnections,
					MaxIdleConns:    defaultPostgresMaxIdleConnections,
					MaxRetry:        defaultPostgresRetry,
					RetryDelay:      Duration(defaultPostgresRetryDelay),
				},
				PostgresConnections: []PostgresConnConfig{
					{
						Name: "testdb1",
						PostgresOverrideableConfig: PostgresOverrideableConfig{
							ConnMaxLifetime: Duration(defaultPostgresConnMaxLifetime),
							ConnMaxIdleTime: Duration(defaultPostgresConnMaxIdleTime),
							MaxOpenConns:    defaultPostgresMaxOpenConnections,
							MaxIdleConns:    defaultPostgresMaxIdleConnections,
							MaxRetry:        defaultPostgresRetry,
							RetryDelay:      Duration(defaultPostgresRetryDelay),
						},
						PrimaryDB: PostgresConfig{
							DSN: "postgres://postgres:postgres@localhost:5432?sslmode=disable",
							PostgresOverrideableConfig: PostgresOverrideableConfig{
								ConnMaxLifetime: Duration(defaultPostgresConnMaxLifetime),
								ConnMaxIdleTime: Duration(defaultPostgresConnMaxIdleTime),
								MaxOpenConns:    defaultPostgresMaxOpenConnections,
								MaxIdleConns:    defaultPostgresMaxIdleConnections,
								MaxRetry:        defaultPostgresRetry,
								RetryDelay:      Duration(defaultPostgresRetryDelay),
							},
						},
						SecondaryDB: PostgresConfig{
							DSN: "postgres://postgres:postgres@localhost:5432?sslmode=disable",
							PostgresOverrideableConfig: PostgresOverrideableConfig{
								ConnMaxLifetime: Duration(defaultPostgresConnMaxLifetime),
								ConnMaxIdleTime: Duration(defaultPostgresConnMaxIdleTime),
								MaxOpenConns:    defaultPostgresMaxOpenConnections,
								MaxIdleConns:    defaultPostgresMaxIdleConnections,
								MaxRetry:        defaultPostgresRetry,
								RetryDelay:      Duration(defaultPostgresRetryDelay),
							},
						},
					},
				},
			},
			error: false,
		},
	}

	for _, test := range tests {
		tt := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.config.Validate(); err != nil && !tt.error {
				t.Fatal(err)
			}

			if tt.error {
				return
			}

			if diff := cmp.Diff(tt.config, tt.expect); diff != "" {
				t.Fatalf("config is different from expected (+want/-got):\n%s", diff)
			}
		})
	}
}
