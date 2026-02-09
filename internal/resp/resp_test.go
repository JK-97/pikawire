package resp

import (
	"bytes"
	"testing"
)

func TestParseArray_OK(t *testing.T) {
	content := []byte("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	argv, err := ParseArray(content)
	if err != nil {
		t.Fatalf("ParseArray: %v", err)
	}
	if len(argv) != 3 {
		t.Fatalf("argc=%d", len(argv))
	}
	if string(argv[0]) != "set" || string(argv[1]) != "key" || string(argv[2]) != "value" {
		t.Fatalf("argv=%q", argv)
	}
}

func TestParseArray_BinarySafe(t *testing.T) {
	arg := []byte{0x00, 'a', 0xff}
	content := SerializeArray([][]byte{[]byte("set"), []byte("k"), arg})
	argv, err := ParseArray(content)
	if err != nil {
		t.Fatalf("ParseArray: %v", err)
	}
	if !bytes.Equal(argv[2], arg) {
		t.Fatalf("arg mismatch: %x vs %x", argv[2], arg)
	}
}

func TestParseArray_Invalid(t *testing.T) {
	tests := [][]byte{
		[]byte(""),
		[]byte("+OK\r\n"),
		[]byte("*1\r\n$3\r\nset\r\ntrailing"),
		[]byte("*1\r\n$3\r\nse\r\n"),
		[]byte("*1\r\n$-1\r\n"),
	}
	for _, content := range tests {
		if _, err := ParseArray(content); err == nil {
			t.Fatalf("expected error for %q", content)
		}
	}
}
