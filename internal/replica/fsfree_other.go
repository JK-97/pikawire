//go:build !linux && !darwin

package replica

// fsFreeBytes is unsupported on this platform; callers skip the check.
func fsFreeBytes(string) (int64, bool) { return 0, false }
