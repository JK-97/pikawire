package binlog

import "testing"

func TestMaybeTransformPksetexatRawRESP(t *testing.T) {
	argv := [][]byte{[]byte("pksetexat"), []byte("key"), []byte("1700000100"), []byte("v")}
	raw := []byte("*4\r\n$9\r\npksetexat\r\n$3\r\nkey\r\n$10\r\n1700000100\r\n$1\r\nv\r\n")
	out := string(MaybeTransformPksetexatRawRESP(argv, 1700000000, raw))
	want := "*4\r\n$5\r\nsetex\r\n$3\r\nkey\r\n$3\r\n100\r\n$1\r\nv\r\n"
	if out != want {
		t.Fatalf("got %q want %q", out, want)
	}

	if unchanged := MaybeTransformPksetexatRawRESP([][]byte{[]byte("set")}, 0, raw); string(unchanged) != string(raw) {
		t.Fatalf("unexpected transform for non-pksetexat")
	}
}
