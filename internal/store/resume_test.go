package store

import "testing"

func TestResumeRoundTrip(t *testing.T) {
	path := t.TempDir() + "/ck.json"
	s, err := Open(path, "src")
	if err != nil {
		t.Fatal(err)
	}
	s.SetSnapshot(PhaseRunning)
	s.SetResume(&SnapshotResume{
		AnchorFilenum: 3, AnchorOffset: 77, TypeName: "hash", Key: "hsh:9",
		DumpDir: "/tmp/dump",
	})
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path, "src")
	if err != nil {
		t.Fatal(err)
	}
	r := s2.State().Resume
	if r == nil || r.TypeName != "hash" || r.Key != "hsh:9" || r.DumpDone {
		t.Fatalf("resume=%#v", r)
	}
	s2.ResetForRescan()
	if s2.State().Resume != nil || s2.State().Snapshot != PhaseRunning {
		t.Fatal("ResetForRescan must drop resume, keep running")
	}
}
