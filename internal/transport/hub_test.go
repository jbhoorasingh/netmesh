package transport

import (
	"testing"

	"netmesh/internal/logging"
	"netmesh/internal/protocol"
)

// collectMissed drains the bus and returns total missed count across all
// PACKET_SEQUENCE_MISSED events seen so far.
func drainMissed(events <-chan logging.Event) (count int, totalMissed uint64) {
	for {
		select {
		case ev := <-events:
			if ev.Type == logging.PacketSequenceMissed {
				count++
				if v, ok := ev.Fields["missed"].(uint64); ok {
					totalMissed += v
				}
			}
		default:
			return
		}
	}
}

func batch(replay bool, seqs ...uint64) protocol.TelemetryBatch {
	b := protocol.TelemetryBatch{Replay: replay}
	for _, s := range seqs {
		b.Metrics = append(b.Metrics, protocol.Metric{Seq: s, AgentID: "a"})
	}
	return b
}

func TestSequenceGapDetection(t *testing.T) {
	log := logging.New("test")
	events, unsub := log.Bus().Subscribe(256)
	defer unsub()

	h := NewHub(log, "", nil, nil)

	// Contiguous: no gaps.
	h.handleTelemetry("a", batch(false, 1, 2, 3))
	if n, _ := drainMissed(events); n != 0 {
		t.Errorf("contiguous produced %d missed events, want 0", n)
	}

	// Jump 3 -> 6: two missed (4,5).
	h.handleTelemetry("a", batch(false, 6))
	n, missed := drainMissed(events)
	if n != 1 || missed != 2 {
		t.Errorf("gap: events=%d missed=%d, want 1 event / 2 missed", n, missed)
	}

	// Replay batch with a gap must NOT raise missed events.
	h.handleTelemetry("a", batch(true, 20))
	if n, _ := drainMissed(events); n != 0 {
		t.Errorf("replay produced %d missed events, want 0", n)
	}

	// After replay advanced watermark to 20, live 21 is contiguous.
	h.handleTelemetry("a", batch(false, 21))
	if n, _ := drainMissed(events); n != 0 {
		t.Errorf("post-replay contiguous produced %d missed events, want 0", n)
	}
}

func TestSequenceIndependentPerAgent(t *testing.T) {
	log := logging.New("test")
	events, unsub := log.Bus().Subscribe(256)
	defer unsub()
	h := NewHub(log, "", nil, nil)

	h.handleTelemetry("a", batch(false, 1))
	h.handleTelemetry("b", batch(false, 100))
	// Each agent's first observed seq sets its own baseline; no cross-talk.
	if n, _ := drainMissed(events); n != 0 {
		t.Errorf("independent agents produced %d missed events, want 0", n)
	}
}
