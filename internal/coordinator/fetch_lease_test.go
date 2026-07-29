package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestFetchLease_AcquireDenyRelease(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	// First requester wins.
	holder, granted, err := cs.AcquireFetchLease(ctx, "/hot.bin_chunk_0", "peer-a", time.Second)
	if err != nil || !granted || holder != "peer-a" {
		t.Fatalf("acquire = (%q, %v, %v), want (peer-a, true, nil)", holder, granted, err)
	}

	// Second requester is told who holds it.
	holder, granted, err = cs.AcquireFetchLease(ctx, "/hot.bin_chunk_0", "peer-b", time.Second)
	if err != nil || granted || holder != "peer-a" {
		t.Fatalf("contended acquire = (%q, %v, %v), want (peer-a, false, nil)", holder, granted, err)
	}

	// A different key is independent.
	_, granted, err = cs.AcquireFetchLease(ctx, "/other.bin_chunk_0", "peer-b", time.Second)
	if err != nil || !granted {
		t.Fatalf("independent key should be granted, got granted=%v err=%v", granted, err)
	}

	// Release by the holder frees the key for the next requester.
	if err := cs.ReleaseFetchLease(ctx, "/hot.bin_chunk_0", "peer-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	holder, granted, err = cs.AcquireFetchLease(ctx, "/hot.bin_chunk_0", "peer-b", time.Second)
	if err != nil || !granted || holder != "peer-b" {
		t.Fatalf("post-release acquire = (%q, %v, %v), want (peer-b, true, nil)", holder, granted, err)
	}
}

func TestFetchLease_ReleaseByNonHolderIsIgnored(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	if _, granted, _ := cs.AcquireFetchLease(ctx, "/k", "peer-a", time.Second); !granted {
		t.Fatal("initial acquire should be granted")
	}
	// peer-b releasing a lease it doesn't hold must not free peer-a's lease.
	if err := cs.ReleaseFetchLease(ctx, "/k", "peer-b"); err != nil {
		t.Fatalf("non-holder release: %v", err)
	}
	holder, granted, _ := cs.AcquireFetchLease(ctx, "/k", "peer-c", time.Second)
	if granted || holder != "peer-a" {
		t.Fatalf("lease should still be held by peer-a, got (%q, %v)", holder, granted)
	}
}

func TestFetchLease_ExpiresAfterTTL(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	if _, granted, _ := cs.AcquireFetchLease(ctx, "/exp", "peer-a", 20*time.Millisecond); !granted {
		t.Fatal("initial acquire should be granted")
	}
	// Held: contender denied.
	if _, granted, _ := cs.AcquireFetchLease(ctx, "/exp", "peer-b", time.Second); granted {
		t.Fatal("lease should still be held")
	}
	time.Sleep(30 * time.Millisecond)
	// Expired: contender wins without an explicit release (crashed holder).
	holder, granted, _ := cs.AcquireFetchLease(ctx, "/exp", "peer-b", time.Second)
	if !granted || holder != "peer-b" {
		t.Fatalf("post-expiry acquire = (%q, %v), want (peer-b, true)", holder, granted)
	}
}

func TestFetchLease_ReacquireByHolderRenews(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	if _, granted, _ := cs.AcquireFetchLease(ctx, "/renew", "peer-a", 30*time.Millisecond); !granted {
		t.Fatal("initial acquire should be granted")
	}
	time.Sleep(20 * time.Millisecond)
	// Holder re-acquires: TTL restarts.
	if _, granted, _ := cs.AcquireFetchLease(ctx, "/renew", "peer-a", 30*time.Millisecond); !granted {
		t.Fatal("holder re-acquire should be granted")
	}
	time.Sleep(20 * time.Millisecond) // 40ms after first acquire, 20ms after renewal
	if _, granted, _ := cs.AcquireFetchLease(ctx, "/renew", "peer-b", time.Second); granted {
		t.Fatal("renewed lease should still be held by peer-a")
	}
}

// TestFetchLease_SingleWinnerUnderContention: N goroutines race for one key;
// exactly one wins — the cross-node single-flight property.
func TestFetchLease_SingleWinnerUnderContention(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	const racers = 32
	var wg sync.WaitGroup
	granted := make([]bool, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, ok, err := cs.AcquireFetchLease(ctx, "/contended", peerName(idx), 5*time.Second)
			if err != nil {
				t.Errorf("racer %d: %v", idx, err)
				return
			}
			granted[idx] = ok
		}(i)
	}
	wg.Wait()

	winners := 0
	for _, ok := range granted {
		if ok {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
}

func peerName(i int) string {
	return "peer-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}

func TestClampFetchLeaseTTL(t *testing.T) {
	if got := ClampFetchLeaseTTL(0); got != DefaultFetchLeaseTTL {
		t.Fatalf("clamp(0) = %v, want default", got)
	}
	if got := ClampFetchLeaseTTL(5 * time.Minute); got != maxFetchLeaseTTL {
		t.Fatalf("clamp(5m) = %v, want max", got)
	}
	if got := ClampFetchLeaseTTL(3 * time.Second); got != 3*time.Second {
		t.Fatalf("clamp(3s) = %v, want 3s", got)
	}
}
