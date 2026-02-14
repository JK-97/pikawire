//go:build darwin

package replica

import "syscall"

// fsFreeBytes reports bytes available to unprivileged writers on dir's
// filesystem (ok=false on any statfs failure: the caller skips the check).
func fsFreeBytes(dir string) (int64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, false
	}
	return int64(st.Bavail) * int64(st.Bsize), true
}
