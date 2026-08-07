// Package fusemount owns the lifecycle of the FUSE mount: establishing it,
// serving it, and deciding what to do when the serve loop dies. It is
// deliberately backend-agnostic (see Backend) so the recovery logic can be
// exercised in tests — the concrete bazil/go-fuse backends are Linux-only.
package fusemount

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Backend is the mount-lifecycle surface a FUSE implementation must provide.
type Backend interface {
	Mount(ctx context.Context, mountPoint string) error
	Serve(ctx context.Context) error
	Unmount(mountPoint string) error
}

// MountWithRetry establishes the mount, clearing a stale mountpoint left by a
// previous crash before each attempt.
func MountWithRetry(ctx context.Context, backend Backend, mountPoint string, retries int, baseDelay time.Duration, logger *log.Logger) error {
	if retries < 1 {
		retries = 1
	}
	if baseDelay <= 0 {
		baseDelay = 2 * time.Second
	}

	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		if err := cleanupStaleMountpoint(ctx, mountPoint, logger); err != nil {
			logger.Printf("Mount cleanup warning for %s: %v", mountPoint, err)
		}

		err := backend.Mount(ctx, mountPoint)
		if err == nil {
			return nil
		}
		lastErr = err
		logger.Printf("FUSE mount attempt %d/%d failed at %s: %v", attempt, retries, mountPoint, err)

		if attempt == retries {
			break
		}

		wait := time.Duration(attempt) * baseDelay
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("mount retries exhausted for %s: %w", mountPoint, lastErr)
}

func cleanupStaleMountpoint(ctx context.Context, mountPoint string, logger *log.Logger) error {
	commands := [][]string{
		{"fusermount3", "-u", "-z", mountPoint},
		{"fusermount", "-u", "-z", mountPoint},
		{"umount", "-l", mountPoint},
	}

	var lastErr error
	for _, cmdArgs := range commands {
		cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			lastErr = err
			output := strings.TrimSpace(string(out))
			if output != "" {
				logger.Printf("Mount cleanup command failed: %s (%v): %s", strings.Join(cmdArgs, " "), err, output)
			}
			continue
		}
		lastErr = nil
	}

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return err
	}
	return lastErr
}
