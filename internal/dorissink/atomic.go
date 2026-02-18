package dorissink

import "sync/atomic"

type atomicUint struct{ v atomic.Uint64 }

func (a *atomicUint) Add(n uint64) uint64 { return a.v.Add(n) }
