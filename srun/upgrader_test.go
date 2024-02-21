package srun

import (
	"context"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestUpgrader for testing, because we cannot use tableflip to ensure the upgrader is running correctly.
// Tableflip only allowed us to call tableflip.New() for once.
type TestUpgrader struct {
	mu      sync.Mutex
	running bool
}

func (tg *TestUpgrader) Name() string {
	return "test-upgrader"
}

func (tg *TestUpgrader) Listen(ctx context.Context) error {
	tg.mu.Lock()
	tg.running = true
	tg.mu.Unlock()

	for range ctx.Done() {
		break
	}
	return nil
}

func (tg *TestUpgrader) Listener(network, addr string) (net.Listener, error) {
	return nil, nil
}

func (tg *TestUpgrader) Running() bool {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return tg.running
}

func (tg *TestUpgrader) PIDFile() string {
	return ""
}

func TestUpgraderStop(t *testing.T) {
	t.Parallel()

	pidFile := "testdata.pid"
	t.Cleanup(func() {
		os.Remove(pidFile)
	})

	u, err := newUpgrader(pidFile, syscall.SIGHUP)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errC := make(chan error, 1)
	go func() {
		errC <- u.Run(ctx)
	}()

	maxCheckCount := 10
	checkCount := 0
	for {
		select {
		case err := <-errC:
			t.Fatal(err)
		default:
			time.Sleep(time.Millisecond * 100)
		}

		if u.Running() {
			break
		}
		checkCount++

		if maxCheckCount == checkCount {
			t.Fatal("runner is not running")
		}
	}
	cancel()

	time.Sleep(time.Millisecond * 300)

	if u.Running() {
		t.Fatal("upgrader is still running")
	}
}
