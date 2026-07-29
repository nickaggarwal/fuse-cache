//go:build !linux

package nodeinit

import "fmt"

// statfsBytes is Linux-only in production; non-Linux builds (macOS dev) rely
// on the test override of statfsFunc.
func statfsBytes(path string) (int64, int64, error) {
	return 0, 0, fmt.Errorf("statfs not supported on this platform (path %s)", path)
}
