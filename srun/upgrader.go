package srun

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"

	"github.com/cloudflare/tableflip"
)

var _ ServiceRunnerAware = (*upgrader)(nil)

type UpgraderConfig struct {
	// SelfUpgrade defines whether the binary is allowed to be upgraded with SIGHUP or not.
	SelfUpgrade bool
	// PIDFileName is an optional name for the PIDFile to do a self-upgrade. By default we will use
	// the build information for the pid file.
	PIDFileName string
}

// upgrader provides the ability to self-upgrade the Go program.
type upgrader struct {
	// pidFile is the file that stores pid number for the program to hot-reload.
	pidFile string
	// upgrader is tableflip upgrader instance to invoke upgrade when upgrade signal
	// is invoked.
	upgrader *tableflip.Upgrader

	readyErr         error
	ctxUpgrade       context.Context
	ctxUpgradeCancel context.CancelCauseFunc

	sigC   chan os.Signal
	stopC  chan struct{}
	readyC chan struct{}

	mu      sync.Mutex
	running bool
}

// newUpgrader creates a new upgrader, the upgrader will create a PIDFILE based on
// the file passed to the upgrader.
//
// It is possible to use multiple signal for upgrade process, but usually
// SIGHUP is used for upgrade signal.
func newUpgrader(pidFile string, signals ...os.Signal) (*upgrader, error) {
	upg, err := tableflip.New(tableflip.Options{
		PIDFile: pidFile,
	})
	if err != nil {
		return nil, err
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)

	// Create a new cancellable context because we want to cancel the context when upgrade starts.
	// If other components are using this context, then we can propagate the context cancellation to
	// other components to indicates the context is cancelled because of an upgrade.
	ctx, cancel := context.WithCancelCause(context.Background())

	return &upgrader{
		upgrader:         upg,
		ctxUpgrade:       ctx,
		ctxUpgradeCancel: cancel,
		sigC:             c,
		stopC:            make(chan struct{}),
		readyC:           make(chan struct{}, 1),
	}, nil
}

// Context returns the upgrade context so other components can listen to the upgrader context as well.
func (u *upgrader) Context() context.Context {
	return u.ctxUpgrade
}

// Name returns the name of the upgrader.
func (*upgrader) Name() string {
	return "upgrader"
}

func (u *upgrader) PreRun(ctx context.Context) error {
	return nil
}

func (u *upgrader) setRunning(running bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.running = running
}

func (u *upgrader) Running() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.running
}

func (u *upgrader) Init(Context) error {
	return nil
}

func (u *upgrader) Run(ctx context.Context) error {
	if err := u.upgrader.Ready(); err != nil {
		// Mark the ready error as the same error as we might want to throw the error early.
		u.readyErr = err
		return err
	}
	u.setRunning(true)
	// Mark the upgrader as ready as we the tableflip upgrader is also ready.
	u.readyC <- struct{}{}

	defer func() {
		u.setRunning(false)
		// Ensure that the context is cancelled so it's not leaking. It's okay if the function
		// is invoked again since the second one will be ignored.
		u.ctxUpgradeCancel(nil)
	}()

	for {
		select {
		case <-u.upgrader.Exit():
			u.ctxUpgradeCancel(errUpgrade)
			return nil

		case <-u.sigC: // Wait until the signal is coming.
			err := u.upgrader.Upgrade()
			if err != nil {
				return err
			}
			<-u.upgrader.Exit()
			u.ctxUpgradeCancel(errUpgrade)
			return nil

		case <-ctx.Done():
			u.upgrader.Stop()
			return nil

		case <-u.stopC:
			u.upgrader.Stop()
			return nil
		}
	}
}

func (u *upgrader) Ready(ctx context.Context) error {
	if u.readyErr != nil {
		return u.readyErr
	}
	<-u.readyC
	return nil
}

func (u *upgrader) Stop(ctx context.Context) error {
	if !u.Running() {
		return nil
	}
	close(u.stopC)
	return nil
}

func (u *upgrader) PIDFile() string {
	return u.pidFile
}

func (u *upgrader) createListener(network, addr string) (net.Listener, error) {
	return u.upgrader.Listen(network, addr)
}
