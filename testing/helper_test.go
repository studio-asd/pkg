package testing

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type EventualTester struct {
	counter int
}

func (e *EventualTester) Do(expect string) (string, error) {
	e.counter += 1
	if e.counter < 3 {
		return "", errors.New("waiting for 3 times")
	}
	return expect, nil
}

func TestEventually(t *testing.T) {
	t.Parallel()
	expect := "OK"

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		tester := EventualTester{}

		if _, err := eventually(
			func() error {
				_, err := tester.Do(expect)
				return err
			},
			time.Millisecond*100,
			time.Second,
		); err != nil {
			t.Fatal(err)
		}
		if tester.counter < 3 {
			t.Fatal("expecting at least 3 times retry")
		}
	})

	t.Run("failed diff", func(t *testing.T) {
		t.Parallel()
		tester := EventualTester{}

		if _, err := eventually(
			func() error {
				got, err := tester.Do(expect)
				if diff := cmp.Diff("FAILED", got); diff != "" {
					return fmt.Errorf("(-want/+got)\n%s", diff)
				}
				return err
			},
			time.Millisecond*100,
			time.Second,
		); err == nil {
			t.Fatal("expecting err not nil")
		}
	})

	t.Run("failed eventual timeout", func(t *testing.T) {
		t.Parallel()
		tester := EventualTester{}

		if _, err := eventually(
			func() error {
				_, err := tester.Do(expect)
				return err
			},
			time.Millisecond*600,
			time.Second,
		); !errors.Is(err, ErrEventualTimeout) {
			t.Fatalf("expecting error %v but got %v", ErrEventualTimeout, err)
		}
	})
}
