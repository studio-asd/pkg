// The database DSN parser codes is taken from:
// https://github.com/DataDog/dd-trace-go/blob/v0.6.1/contrib/database/sql/parsedsn/parsedsn.go.

package postgres

import (
	"fmt"
	"net"
	nurl "net/url"
	"sort"
	"strings"
	"unicode"
)

type values map[string]string

// scanner implements a tokenizer for libpq-style option strings.
type scanner struct {
	s []rune
	i int
}

// newScanner returns a new scanner initialized with the option string s.
func newScanner(s string) *scanner {
	return &scanner{[]rune(s), 0}
}

// Next returns the next rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) Next() (rune, bool) {
	if s.i >= len(s.s) {
		return 0, false
	}
	r := s.s[s.i]
	s.i++
	return r, true
}

// SkipSpaces returns the next non-whitespace rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) SkipSpaces() (rune, bool) {
	r, ok := s.Next()
	for unicode.IsSpace(r) && ok {
		r, ok = s.Next()
	}
	return r, ok
}

type DSN struct {
	Username        string
	Password        string
	Host            string
	Port            string
	DatabaseName    string
	SSLMode         string
	ApplicationName string
}

func (d DSN) URL() string {
	return buildPostgresURL(d.Username, d.Password, d.Host, d.Port, d.DatabaseName, d.SSLMode)
}

// SafeURL generates a safe URL(without password) but not usable for the dsn for connecting to the database.
func (d DSN) SafeURL() string {
	return buildPostgresURL(d.Username, "[xxxx]", d.Host, d.Port, d.DatabaseName, d.SSLMode)
}

func (d DSN) BuildConfig() (ConnectConfig, error) {
	config := ConnectConfig{
		Driver:   "pgx",
		Username: d.Username,
		Password: d.Password,
		Host:     d.Host,
		Port:     d.Port,
		SSLMode:  d.SSLMode,
	}
	return config, config.validate()
}

// ParseDSN parses a postgres-type dsn into a map
func ParseDSN(dsn string) (DSN, error) {
	var err error
	meta := make(map[string]string)

	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		dsn, err = parsePostgresURL(dsn)
		if err != nil {
			return DSN{}, err
		}
	}

	if err := parsePogtresOpts(dsn, meta); err != nil {
		return DSN{}, err
	}
	return DSN{
		Username:        meta["user"],
		Password:        meta["password"],
		Host:            meta["host"],
		Port:            meta["port"],
		DatabaseName:    meta["dbname"],
		SSLMode:         meta["sslmode"],
		ApplicationName: meta["application_name"],
	}, nil
}

// parsePostgresURL no longer needs to be used by clients of this library since supplying a URL as a
// connection string to sql.Open() is now supported:
//
//	sql.Open("postgres", "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full")
//
// It remains exported here for backwards-compatibility.
//
// ParseURL converts a url to a connection string for driver.Open.
// Example:
//
//	"postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
//
// converts to:
//
//	"user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
//
// A minimal example:
//
//	"postgres://"
//
// This will be blank, causing driver.Open to use all of the defaults
func parsePostgresURL(url string) (string, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return "", err
	}

	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return "", fmt.Errorf("invalid connection protocol: %s", u.Scheme)
	}

	var kvs []string
	escaper := strings.NewReplacer(` `, `\ `, `'`, `\'`, `\`, `\\`)
	accrue := func(k, v string) {
		if v != "" {
			kvs = append(kvs, k+"="+escaper.Replace(v))
		}
	}

	if u.User != nil {
		v := u.User.Username()
		accrue("user", v)

		v, _ = u.User.Password()
		accrue("password", v)
	}

	if host, port, err := net.SplitHostPort(u.Host); err != nil {
		accrue("host", u.Host)
	} else {
		accrue("host", host)
		accrue("port", port)
	}

	if u.Path != "" {
		accrue("dbname", u.Path[1:])
	}

	q := u.Query()
	for k := range q {
		accrue(k, q.Get(k))
	}

	sort.Strings(kvs) // Makes testing easier (not a performance concern)
	return strings.Join(kvs, " "), nil
}

// parsePostgresOpts parses the options from name and adds them to the values.
// The parsing code is based on conninfo_parse from libpq's fe-connect.c
func parsePogtresOpts(name string, o values) error {
	s := newScanner(name)

	for {
		var (
			keyRunes, valRunes []rune
			r                  rune
			ok                 bool
		)

		if r, ok = s.SkipSpaces(); !ok {
			break
		}

		// Scan the key
		for !unicode.IsSpace(r) && r != '=' {
			keyRunes = append(keyRunes, r)
			if r, ok = s.Next(); !ok {
				break
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			r, ok = s.SkipSpaces()
		}

		// The current character should be =
		if r != '=' || !ok {
			return fmt.Errorf(`missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			// If we reach the end here, the last value is just an empty string as per libpq.
			o[string(keyRunes)] = ""
			break
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r == '\\' {
					if r, ok = s.Next(); !ok {
						return fmt.Errorf(`missing character after backslash`)
					}
				}
				valRunes = append(valRunes, r)

				if r, ok = s.Next(); !ok {
					break
				}
			}
		} else {
		quote:
			for {
				if r, ok = s.Next(); !ok {
					return fmt.Errorf(`unterminated quoted string literal in connection string`)
				}
				switch r {
				case '\'':
					break quote
				case '\\':
					r, _ = s.Next()
					fallthrough
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		o[string(keyRunes)] = string(valRunes)
	}

	return nil
}
