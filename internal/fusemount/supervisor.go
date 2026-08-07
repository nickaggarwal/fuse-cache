package fusemount

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// A FUSE serve loop can end while the process is otherwise perfectly healthy:
// the kernel connection is severed by an external `fusermount -u`, the device
// returns ENODEV, or the library hits an unrecoverable protocol error. The
// process keeps running, the API keeps answering, heartbeats keep arriving at
// the coordinator — and every pod on the node sees ENOTCONN on the mount while
// the node continues to advertise itself as a healthy holder.
//
// The Supervisor closes that gap. An unexpected serve exit marks the mount
// unhealthy (so the liveness probe stops passing) and triggers a remount. A
// mount that will not come back, or that keeps flapping, ends the process so
// the DaemonSet replaces it on a clean node state.

const (
	// DefaultRemountAttempts is how many consecutive remounts are tried before
	// the mount is declared unrecoverable.
	DefaultRemountAttempts = 3
	// DefaultStableServeWindow is how long a mount must serve without dying
	// for the consecutive-failure count to reset. A mount that served for an
	// hour and then died once is a transient fault; one that dies every few
	// seconds is a flap, and remounting it forever just hides the problem.
	DefaultStableServeWindow = 5 * time.Minute
)

// Config controls a Supervisor. The zero value of each field falls back to the
// documented default.
type Config struct {
	MountPoint string
	// MountRetries and MountRetryDelay are passed through to MountWithRetry for
	// each remount attempt.
	MountRetries    int
	MountRetryDelay time.Duration
	// RemountAttempts bounds consecutive remount cycles. Negative means never
	// remount: the first serve death is fatal.
	RemountAttempts int
	// StableServeWindow is how long a serve must last to be considered stable.
	StableServeWindow time.Duration
	Logger            *log.Logger
	// now and mount are injectable for tests: the real mount path shells out
	// to fusermount/umount, which a unit test has no business doing.
	now   func() time.Time
	mount func(ctx context.Context, backend Backend, mountPoint string, retries int, baseDelay time.Duration, logger *log.Logger) error
}

// Supervisor serves a FUSE mount and keeps it alive, or gives up loudly.
type Supervisor struct {
	backend Backend
	cfg     Config
	logger  *log.Logger
	now     func() time.Time
	mount   func(ctx context.Context, backend Backend, mountPoint string, retries int, baseDelay time.Duration, logger *log.Logger) error

	// healthy is read from the API health handler on every probe, so it is an
	// atomic rather than something guarded by the supervisor's own loop.
	healthy atomic.Bool
}

// NewSupervisor returns a supervisor for backend. The mount is reported healthy
// up front: Run is expected to be called on an already-mounted backend, which
// is the state MountWithRetry leaves it in.
func NewSupervisor(backend Backend, cfg Config) *Supervisor {
	if cfg.Logger == nil {
		cfg.Logger = log.New(log.Writer(), "[FUSE] ", log.LstdFlags)
	}
	if cfg.RemountAttempts == 0 {
		cfg.RemountAttempts = DefaultRemountAttempts
	}
	if cfg.StableServeWindow <= 0 {
		cfg.StableServeWindow = DefaultStableServeWindow
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	if cfg.mount == nil {
		cfg.mount = MountWithRetry
	}
	s := &Supervisor{backend: backend, cfg: cfg, logger: cfg.Logger, now: cfg.now, mount: cfg.mount}
	s.healthy.Store(true)
	return s
}

// Healthy reports whether the mount is currently being served. It is false
// from the moment a serve loop dies until a remount succeeds, and stays false
// once the mount is declared unrecoverable.
func (s *Supervisor) Healthy() bool { return s.healthy.Load() }

// Run serves the mount until ctx is cancelled, returning nil for that case.
// Any other outcome is a mount that could not be kept alive, and the error
// says why. The caller is expected to treat a non-nil return as fatal and shut
// the process down.
func (s *Supervisor) Run(ctx context.Context) error {
	failures := 0
	for {
		startedAt := s.now()
		err := s.backend.Serve(ctx)

		if ctx.Err() != nil {
			// Normal shutdown. A serve error raced with cancellation is still
			// just shutdown — the mount is going away either way.
			return nil
		}

		// A nil error here is not success: the serve loop returning while the
		// process is still running means the kernel connection went away
		// (an external unmount, ENODEV, a severed /dev/fuse handle).
		s.healthy.Store(false)
		served := s.now().Sub(startedAt)
		if served >= s.cfg.StableServeWindow {
			failures = 0
		}
		failures++
		if err != nil {
			s.logger.Printf("FUSE serve loop for %s exited after %v: %v (consecutive failures: %d)",
				s.cfg.MountPoint, served.Truncate(time.Millisecond), err, failures)
		} else {
			s.logger.Printf("FUSE serve loop for %s exited after %v with no error — the mount connection was lost (consecutive failures: %d)",
				s.cfg.MountPoint, served.Truncate(time.Millisecond), failures)
		}

		if s.cfg.RemountAttempts < 0 {
			return fmt.Errorf("FUSE serve loop for %s ended and remounting is disabled: %w",
				s.cfg.MountPoint, orLostConnection(err))
		}
		if failures > s.cfg.RemountAttempts {
			return fmt.Errorf("FUSE mount at %s died %d times in a row (last serve lasted %v): %w",
				s.cfg.MountPoint, failures, served.Truncate(time.Millisecond), orLostConnection(err))
		}

		// Tear the old mount down before rebuilding it. The connection is
		// already gone, so this is about clearing the kernel's record of it;
		// MountWithRetry's cleanup covers the same ground, and both are
		// best-effort.
		if uerr := s.backend.Unmount(s.cfg.MountPoint); uerr != nil {
			s.logger.Printf("Pre-remount unmount of %s failed (continuing): %v", s.cfg.MountPoint, uerr)
		}

		s.logger.Printf("Remounting %s (attempt %d/%d)", s.cfg.MountPoint, failures, s.cfg.RemountAttempts)
		if merr := s.mount(ctx, s.backend, s.cfg.MountPoint, s.cfg.MountRetries, s.cfg.MountRetryDelay, s.logger); merr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("remount of %s failed: %w", s.cfg.MountPoint, merr)
		}
		s.healthy.Store(true)
		s.logger.Printf("Remounted %s; resuming serve", s.cfg.MountPoint)
	}
}

// orLostConnection gives a nil serve error something to say in a wrapped
// message, so "exited cleanly but unexpectedly" does not surface as "<nil>".
func orLostConnection(err error) error {
	if err != nil {
		return err
	}
	return errLostConnection
}

var errLostConnection = fmt.Errorf("mount connection lost")
