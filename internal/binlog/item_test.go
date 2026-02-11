package binlog

import (
	"encoding/binary"
	"testing"
)

func TestDecodeTypeFirst_OK(t *testing.T) {
	content := []byte("*1\r\n$4\r\nPING\r\n")
	buf := make([]byte, 34+len(content))
	binary.LittleEndian.PutUint16(buf[0:2], TypeFirst)
	binary.LittleEndian.PutUint32(buf[2:6], 1700000000)
	binary.LittleEndian.PutUint32(buf[6:10], 7)
	binary.LittleEndian.PutUint64(buf[10:18], 8899)
	binary.LittleEndian.PutUint32(buf[18:22], 12)
	binary.LittleEndian.PutUint64(buf[22:30], 34567)
	binary.LittleEndian.PutUint32(buf[30:34], uint32(len(content)))
	copy(buf[34:], content)

	item, err := DecodeTypeFirst(buf)
	if err != nil {
		t.Fatalf("DecodeTypeFirst: %v", err)
	}
	if string(item.Content) != string(content) {
		t.Fatalf("content mismatch")
	}
	if item.Filenum != 12 || item.Offset != 34567 || item.TermID != 7 || item.LogicID != 8899 {
		t.Fatalf("unexpected item: %+v", item)
	}
}
