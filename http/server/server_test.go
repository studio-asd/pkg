package server

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestConfig(t *testing.T) {
	t.Parallel()
	t.Run("default config", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			config Config
			expect Config
			err    error
		}{
			{
				name: "test default",
				config: Config{
					Name:    "testing",
					Address: ":8080",
				},
				expect: Config{
					Name:              "testing",
					Address:           ":8080",
					ReadTimeout:       defaultReadTimeout,
					ReadHeaderTimeout: defaultReadHeaderTimeout,
					WriteTimeout:      defaultWriteTimeout,
				},
				err: nil,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := test.config
				if err := c.Validate(); err != test.err {
					t.Fatalf("expecting error %v but got %v", test.err, err)
				}

				if diff := cmp.Diff(test.expect, c); diff != "" {
					t.Fatalf("(-want/+got)\n%s", diff)
				}
			})
		}
	})
}
