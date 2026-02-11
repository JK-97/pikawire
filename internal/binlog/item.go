package binlog

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	TypeFirst uint16 = 1
)

var ErrInvalidBinlog = errors.New("invalid binlog")

type Item struct {
	Type     uint16
	ExecTime uint32
	TermID   uint32
	LogicID  uint64
	Filenum  uint32
	Offset   uint64
	Content  []byte
}

func DecodeTypeFirst(binlog []byte) (Item, error) {
	const headerSize = 34
	if len(binlog) < headerSize {
		return Item{}, fmt.Errorf("%w: too short", ErrInvalidBinlog)
	}
	t := binary.LittleEndian.Uint16(binlog[0:2])
	if t != TypeFirst {
		return Item{}, fmt.Errorf("%w: unexpected type=%d", ErrInvalidBinlog, t)
	}
	execTime := binary.LittleEndian.Uint32(binlog[2:6])
	termID := binary.LittleEndian.Uint32(binlog[6:10])
	logicID := binary.LittleEndian.Uint64(binlog[10:18])
	filenum := binary.LittleEndian.Uint32(binlog[18:22])
	offset := binary.LittleEndian.Uint64(binlog[22:30])
	contentLen := binary.LittleEndian.Uint32(binlog[30:34])

	if len(binlog)-headerSize != int(contentLen) {
		return Item{}, fmt.Errorf("%w: content length mismatch want=%d got=%d",
			ErrInvalidBinlog, contentLen, len(binlog)-headerSize)
	}
	content := binlog[headerSize:]
	return Item{
		Type:     t,
		ExecTime: execTime,
		TermID:   termID,
		LogicID:  logicID,
		Filenum:  filenum,
		Offset:   offset,
		Content:  content,
	}, nil
}
