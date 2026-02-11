package info

import "testing"

func TestParseReplicationInfo_BinlogOffset(t *testing.T) {
	info := "# Replication\r\nrole:master\r\nbinlog_offset:7 12345\r\n"
	got, err := ParseReplicationInfo(info)
	if err != nil {
		t.Fatalf("ParseReplicationInfo: %v", err)
	}
	if !got.HasBinlogOffset || got.Filenum != 7 || got.Offset != 12345 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestParseReplicationInfo_BinlogOffsetEquals(t *testing.T) {
	info := "some=thing binlog_offset=12,34567 other\r\n"
	got, err := ParseReplicationInfo(info)
	if err != nil {
		t.Fatalf("ParseReplicationInfo: %v", err)
	}
	if !got.HasBinlogOffset || got.Filenum != 12 || got.Offset != 34567 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestParseReplicationInfo_BinlogFileNumFallback(t *testing.T) {
	info := "binlog_file_num: 9\r\nbinlog_offset: 999\r\n"
	got, err := ParseReplicationInfo(info)
	if err != nil {
		t.Fatalf("ParseReplicationInfo: %v", err)
	}
	if !got.HasBinlogOffset || got.Filenum != 9 || got.Offset != 999 {
		t.Fatalf("unexpected: %+v", got)
	}
}
