package spooler

import (
	"sync"
	"testing"

	"netmesh/internal/protocol"
)

func m(seq uint64) protocol.Metric { return protocol.Metric{Seq: seq} }

func TestFIFOOrder(t *testing.T) {
	s := New(8)
	for i := uint64(1); i <= 5; i++ {
		s.Push(m(i))
	}
	got := s.Drain()
	if len(got) != 5 {
		t.Fatalf("len = %d, want 5", len(got))
	}
	for i, x := range got {
		if x.Seq != uint64(i+1) {
			t.Errorf("got[%d].Seq = %d, want %d", i, x.Seq, i+1)
		}
	}
	if s.Len() != 0 {
		t.Errorf("Len after Drain = %d, want 0", s.Len())
	}
}

func TestOverflowDropsOldest(t *testing.T) {
	s := New(3)
	for i := uint64(1); i <= 6; i++ { // 1..6 into cap-3 ring
		s.Push(m(i))
	}
	if got := s.OverflowDropped(); got != 3 {
		t.Errorf("OverflowDropped = %d, want 3", got)
	}
	got := s.Drain()
	want := []uint64{4, 5, 6} // oldest three (1,2,3) discarded
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Seq != want[i] {
			t.Errorf("got[%d] = %d, want %d", i, got[i].Seq, want[i])
		}
	}
}

func TestDrainEmpty(t *testing.T) {
	s := New(4)
	if got := s.Drain(); got != nil {
		t.Errorf("Drain empty = %v, want nil", got)
	}
}

func TestConcurrentPush(t *testing.T) {
	s := New(DefaultCapacity)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				s.Push(m(uint64(i)))
			}
		}()
	}
	wg.Wait()
	// 8000 pushed into a 1000-cap ring => exactly 1000 retained, 7000 overflowed.
	if s.Len() != DefaultCapacity {
		t.Errorf("Len = %d, want %d", s.Len(), DefaultCapacity)
	}
	if got := s.OverflowDropped(); got != 7000 {
		t.Errorf("OverflowDropped = %d, want 7000", got)
	}
}
