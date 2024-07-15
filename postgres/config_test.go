package postgres

import "testing"

func TestConfigvalidate(t *testing.T) {
	t.Parallel()

	t.Run("must_have", func(t *testing.T) {
		t.Parallel()

		c := ConnectConfig{}
		if err := c.validate(); err.Error() != "postgres: username cannot be empty" {
			t.Fatal("expecting username error")
		}

		c.Username = "test"
		if err := c.validate(); err.Error() != "postgres: password cannot be empty" {
			t.Fatal("expecting username error")
		}

		c.Password = "test"
		if err := c.validate(); err.Error() != "postgres: host cannot be empty" {
			t.Fatal("expecting username error")
		}

		c.Host = "localhost"
		if err := c.validate(); err.Error() != "postgres: port cannot be empty" {
			t.Fatal("expecting username error")
		}

		c.Port = "5432"
		if err := c.validate(); err.Error() != "postgres: driver <empty> is not supported. Please choose: postgres, libpq, pgx" {
			t.Fatal("expecting username error")
		}
	})

	t.Run("override_values", func(t *testing.T) {
		t.Parallel()

		c := ConnectConfig{
			Username: "test",
			Password: "test",
			Host:     "localhost",
			Port:     "5432",
			Driver:   "pgx",
		}
		if err := c.validate(); err != nil {
			t.Fatal(err)
		}
		if c.MaxOpenConns != defaultMaxOpenConns {
			t.Fatalf("expecting max open conns %d but got %d", defaultMaxOpenConns, c.MaxOpenConns)
		}
		if c.ConnMaxLifetime != defaultConnMaxLifetime {
			t.Fatalf("expecting conn max lifetime %d but got %d", defaultConnMaxLifetime, c.ConnMaxLifetime)
		}
		if c.ConnMaxIdletime != defaultConnMaxIdletime {
			t.Fatalf("expecting max idle time %d but got %d", defaultConnMaxIdletime, c.ConnMaxIdletime)
		}
	})
}
