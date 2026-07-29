//go:build linux

package nodeinit

import "golang.org/x/sys/unix"

// statfsBytes returns (total, free-for-unprivileged) bytes for path.
func statfsBytes(path string) (int64, int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bsize := int64(st.Bsize)
	return int64(st.Blocks) * bsize, int64(st.Bavail) * bsize, nil
}
