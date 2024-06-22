package testing

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// ErrEventualTimeout defines whether the error comes from the eventual checks timeout.
var ErrEventualTimeout = errors.New("timeout on eventual evaluation")

// Eventually compare the result of the called 'fn' and retry the effort every T duration with timeout of X.
//
// As this function is only a helper, we don't want to inject testing.T into the function. If we do that then we need to add more parameters into the function to
// compare the test/error result.
func Eventually(t *testing.T, fn func() error, every, timeout time.Duration) {
	if every == 0 {
		t.Fatal("every cannot be 0")
	}
	retryCount, err := eventually(fn, every, timeout)
	if err == nil {
		return
	}

	// If we found some error then we should re-format the error into something nicer because this will eventually be shown in the terminal/output.
	//
	// Format:
	//	cause: [the cause]
	//	timeout: [duration, for example 10s]
	//
	//	error:
	//	[error format]
	cause := "evaluation function returns error"
	if errors.Is(err, ErrEventualTimeout) {
		cause = "check timeout"
	}
	// Wraps the error. Please note we still need to use %w here because we want to keep the original error types.
	err = fmt.Errorf("cause: %s\ntimeout: %s\nreties-count: %d\nerror:\n%w", cause, timeout.String(), retryCount, err)
	t.Error(err)
}

// eventually is our internal eventual evaluator function so we can test the function properly.
func eventually(fn func() error, every, timeout time.Duration) (retryCount int, err error) {
	tStop := time.Now().Add(timeout)
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		// Break if the current time is bigger than the time to stop. If this happen then we will just return
		// the latest error.
		if time.Now().After(tStop) {
			err = errors.Join(err, ErrEventualTimeout)
			break
		}
		// Respect the context error, return immediately if context is cancelled.
		if ctx.Err() != nil {
			err = errors.Join(err, ctx.Err())
			return
		}
		// Wait until the ticker come.
		<-ticker.C
		// This function call will always override 'result' and 'err'. This means whenever the loop breaks, it will
		// always return the latest result and error.
		err = fn()
		if err != nil {
			retryCount++
			continue
		}
		// If no error happens then we should return immediately and report the compare as a success.
		break
	}
	return
}

// RepositoryRoot returns the path of root repository using git command. The function returns error if the git command is not exist.
func RepositoryRoot() (string, error) {
	_, err := exec.LookPath("git")
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(context.Background(), "git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(string(out), "\n", ""), nil
}
