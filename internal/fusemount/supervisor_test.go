package fusemount

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"testing"
	"time"
)

// fakeBackend is a scripted mount backend. serveResults is consumed one entry
// per Serve call; when it runs out, Serve blocks until the context is
// cancelled, which is what a healthy mount looks like.
type fakeBackend struct {
	mu           sync.Mutex
	serveResults []error
	mountErrs    []error
	serves       int
	mounts       int
	unmounts     int
	// served is signalled after each Serve call returns, so tests can key off
	// observed state rather than racing a timer against the supervisor loop.
	served chan struct{}
}

func newFakeBackend(serveResults ...error) *fakeBackend {
	return &fakeBackend{serveResults: serveResults, served: make(chan struct{}, 64)}
}

func (f *fakeBackend) Mount(ctx context.Context, mountPoint string) error {
	f.mu.Lock()
	f.mounts++
	var err error
	if len(f.mountErrs) > 0 {
		err = f.mountErrs[0]
		f.mountErrs = f.mountErrs[1:]
	}
	f.mu.Unlock()
	return err
}

func (f *fakeBackend) Serve(ctx context.Context) error {
	f.mu.Lock()
	f.serves++
	var (
		err  error
		have bool
	)
	if len(f.serveResults) > 0 {
		err, have = f.serveResults[0], true
		f.serveResults = f.serveResults[1:]
	}
	f.mu.Unlock()

	if !have {
		<-ctx.Done()
	}
	select {
	case f.served <- struct{}{}:
	default:
	}
	return err
}

func (f *fakeBackend) Unmount(mountPoint string) error {
	f.mu.Lock()
	f.unmounts++
	f.mu.Unlock()
	return nil
}

func (f *fakeBackend) counts() (mounts, serves, unmounts int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mounts, f.serves, f.unmounts
}

func quietLogger() *log.Logger { return log.New(io.Discard, "", 0) }

func testConfig(remounts int) Config {
	return Config{
		MountPoint:      "/tmp/test-mount",
		MountRetries:    1,
		MountRetryDelay: time.Millisecond,
		RemountAttempts: remounts,
		Logger:          quietLogger(),
		// Skip MountWithRetry's cleanup, which shells out to
		// fusermount/umount against a real path.
		mount: func(ctx context.Context, backend Backend, mountPoint string, _ int, _ time.Duration, _ *log.Logger) error {
			return backend.Mount(ctx, mountPoint)
		},
	}
}

// A serve loop ending while the process runs is a dead mount, even when it
// reports no error — go-fuse's Serve returns nil when the server stops. If a
// nil return were treated as success the supervisor would silently return and
// the node would keep advertising a mount nothing can use.
func TestServeExitingCleanlyIsNotSuccess(t *testing.T) {
	// One nil serve, then a mount that stays up.
	backend := newFakeBackend(nil)
	sup := NewSupervisor(backend, testConfig(1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	waitFor(t, func() bool {
		mounts, serves, _ := backend.counts()
		return mounts == 1 && serves == 2
	}, "remount after a clean serve exit")

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run after cancellation = %v, want nil", err)
	}
}

// Cancellation is normal shutdown, whatever Serve returns on its way out.
func TestShutdownIsNotAFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"serve returns nil", nil},
		{"serve returns an error", errors.New("interrupted")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeBackend{served: make(chan struct{}, 4)}
			// Serve blocks on ctx and then reports tc.err.
			backend.serveResults = nil
			sup := NewSupervisor(&cancelAwareBackend{fakeBackend: backend, exitErr: tc.err}, testConfig(3))

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- sup.Run(ctx) }()

			waitFor(t, func() bool { _, serves, _ := backend.counts(); return serves == 1 }, "first serve to start")
			cancel()

			if err := <-done; err != nil {
				t.Fatalf("Run = %v, want nil on shutdown", err)
			}
			if mounts, _, _ := backend.counts(); mounts != 0 {
				t.Fatalf("remounted %d times during shutdown, want 0", mounts)
			}
			if !sup.Healthy() {
				t.Fatal("mount reported unhealthy during a normal shutdown")
			}
		})
	}
}

// Repeated deaths must stop, not loop forever: a mount that will not stay up
// has to take the process down so the DaemonSet replaces the pod.
func TestGivesUpAfterConsecutiveFailures(t *testing.T) {
	deaths := []error{errors.New("d1"), errors.New("d2"), errors.New("d3"), errors.New("d4")}
	backend := newFakeBackend(deaths...)
	sup := NewSupervisor(backend, testConfig(3))

	err := sup.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil after the mount died repeatedly; the process would keep running with no mount")
	}
	// 3 remounts allowed → 4 serves, and the 4th death is fatal.
	mounts, serves, _ := backend.counts()
	if mounts != 3 || serves != 4 {
		t.Fatalf("mounts=%d serves=%d, want 3 and 4", mounts, serves)
	}
	if sup.Healthy() {
		t.Fatal("Healthy() true after giving up on the mount")
	}
	if !errors.Is(err, deaths[3]) {
		t.Fatalf("error %v does not wrap the last serve error %v", err, deaths[3])
	}
}

// A mount that served for a long stretch and then died once is a transient
// fault, not a flap: the consecutive-failure count resets so the node is not
// killed off by unrelated failures hours apart.
func TestStableServeResetsFailureCount(t *testing.T) {
	backend := newFakeBackend(errors.New("d1"), errors.New("d2"), errors.New("d3"), errors.New("d4"))
	cfg := testConfig(1)
	cfg.StableServeWindow = time.Minute

	// Each serve appears to last 10 minutes, so every death starts fresh and
	// the single allowed remount is never exhausted.
	clock := int64(0)
	cfg.now = func() time.Time {
		t := time.Unix(clock, 0)
		clock += 600
		return t
	}
	sup := NewSupervisor(backend, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	// All four scripted deaths are absorbed; the fifth serve blocks.
	waitFor(t, func() bool { _, serves, _ := backend.counts(); return serves == 5 }, "all deaths to be absorbed")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v, want nil (each death followed a stable serve)", err)
	}
}

// Without the reset, deaths spaced out over a long uptime would accumulate.
// This is the same script as above with a short stable window, and it must
// give up — otherwise the previous test proves nothing.
func TestShortServesAreTreatedAsAFlap(t *testing.T) {
	backend := newFakeBackend(errors.New("d1"), errors.New("d2"), errors.New("d3"))
	cfg := testConfig(1)
	cfg.StableServeWindow = time.Minute
	sup := NewSupervisor(backend, cfg)

	if err := sup.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil for a mount dying immediately on every attempt")
	}
}

// The health probe has to flip before the remount, not after: that window is
// exactly when the node must stop being routed to.
func TestHealthyGoesFalseWhileRemounting(t *testing.T) {
	backend := newFakeBackend(errors.New("died"))
	// Block the remount so the unhealthy window is observable.
	gate := make(chan struct{})
	gated := &gatedMountBackend{fakeBackend: backend, gate: gate}
	sup := NewSupervisor(gated, testConfig(2))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	waitFor(t, func() bool { return !sup.Healthy() }, "health to drop after the serve loop died")
	close(gate)
	waitFor(t, func() bool { return sup.Healthy() }, "health to return after the remount")

	cancel()
	<-done
}

// A remount that cannot re-establish the mount is fatal immediately — there is
// nothing left to retry against.
func TestUnrecoverableRemountEndsRun(t *testing.T) {
	backend := newFakeBackend(errors.New("died"))
	backend.mountErrs = []error{errors.New("no such device")}
	sup := NewSupervisor(backend, testConfig(5))

	err := sup.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil after the remount failed")
	}
	if sup.Healthy() {
		t.Fatal("Healthy() true after a failed remount")
	}
}

// Negative attempts disable remounting entirely, for deployments that would
// rather have the pod rescheduled than have a node retry in place.
func TestRemountDisabled(t *testing.T) {
	backend := newFakeBackend(errors.New("died"))
	sup := NewSupervisor(backend, testConfig(-1))

	if err := sup.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil with remounting disabled")
	}
	if mounts, _, _ := backend.counts(); mounts != 0 {
		t.Fatalf("mounted %d times with remounting disabled, want 0", mounts)
	}
}

// The stale mount is torn down before being rebuilt; leaving the kernel's
// record of the old connection in place makes the remount fail with EBUSY.
func TestRemountUnmountsFirst(t *testing.T) {
	backend := newFakeBackend(errors.New("died"))
	sup := NewSupervisor(backend, testConfig(1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	waitFor(t, func() bool { mounts, _, unmounts := backend.counts(); return mounts == 1 && unmounts == 1 }, "unmount before remount")
	cancel()
	<-done
}

// cancelAwareBackend blocks in Serve until the context is cancelled, then
// reports exitErr — the shape of a backend interrupted by shutdown.
type cancelAwareBackend struct {
	*fakeBackend
	exitErr error
}

func (c *cancelAwareBackend) Serve(ctx context.Context) error {
	c.mu.Lock()
	c.serves++
	c.mu.Unlock()
	<-ctx.Done()
	return c.exitErr
}

// gatedMountBackend holds Mount until gate is closed.
type gatedMountBackend struct {
	*fakeBackend
	gate chan struct{}
}

func (g *gatedMountBackend) Mount(ctx context.Context, mountPoint string) error {
	select {
	case <-g.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.fakeBackend.Mount(ctx, mountPoint)
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
