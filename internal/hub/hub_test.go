package hub

import (
	"errors"
	"testing"
	"time"
)

const recvTimeout = time.Second

func recv(t *testing.T, sub *Subscription) (Event, bool) {
	t.Helper()
	select {
	case ev, open := <-sub.Events():
		return ev, open
	case <-time.After(recvTimeout):
		t.Fatal("timed out waiting for event")
		return Event{}, false
	}
}

func TestPublishSubscribeDelivery(t *testing.T) {
	s := newStream("s", 0)

	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if gap {
		t.Fatal("unexpected gap on empty stream")
	}

	ev, err := s.Publish("hello")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ev.ID != 1 {
		t.Fatalf("first event id = %d, want 1", ev.ID)
	}

	got, open := recv(t, sub)
	if !open {
		t.Fatal("channel closed before delivering the event")
	}
	if got.ID != 1 || got.Data != "hello" {
		t.Fatalf("got %+v, want id 1 data hello", got)
	}

	ev2, err := s.Publish("world")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ev2.ID != 2 {
		t.Fatalf("second event id = %d, want 2", ev2.ID)
	}
}

func TestSubscribeReplaysBufferedEvents(t *testing.T) {
	s := newStream("s", 0)
	for _, data := range []string{"a", "b", "c"} {
		if _, err := s.Publish(data); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	sub, gap := s.Subscribe(1, 0)
	defer sub.Close()
	if gap {
		t.Fatal("unexpected gap: nothing was evicted from the buffer")
	}

	for _, want := range []string{"b", "c"} {
		ev, open := recv(t, sub)
		if !open {
			t.Fatal("channel closed before replay finished")
		}
		if ev.Data != want {
			t.Fatalf("replayed data = %q, want %q", ev.Data, want)
		}
	}
}

func TestSubscribeReportsGapWhenBufferEvicted(t *testing.T) {
	s := newStream("s", 2)
	for _, data := range []string{"a", "b", "c"} {
		if _, err := s.Publish(data); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	// Capacity 2 keeps only ids 2 and 3; id 1 ("a") was evicted.

	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if !gap {
		t.Fatal("expected a gap because id 1 was evicted before this subscribe")
	}

	for _, want := range []string{"b", "c"} {
		ev, open := recv(t, sub)
		if !open {
			t.Fatal("channel closed before replay finished")
		}
		if ev.Data != want {
			t.Fatalf("replayed data = %q, want %q", ev.Data, want)
		}
	}

	sub2, gap2 := s.Subscribe(2, 0)
	defer sub2.Close()
	if gap2 {
		t.Fatal("unexpected gap: caller already has everything up to the eviction boundary")
	}
}

func TestLaggingSubscriberIsDroppedNotBlocked(t *testing.T) {
	s := newStream("s", 0)
	sub, _ := s.Subscribe(0, 1)
	defer sub.Close()

	if _, err := s.Publish("first"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// The subscriber's buffer (size 1) now holds "first" and nothing has read
	// it yet, so this publish must find the channel full and drop the
	// subscriber rather than block the producer.
	if _, err := s.Publish("second"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ev, open := recv(t, sub)
	if !open {
		t.Fatal("expected the buffered event to still be delivered before closing")
	}
	if ev.Data != "first" {
		t.Fatalf("buffered event = %q, want %q", ev.Data, "first")
	}

	if _, open := recv(t, sub); open {
		t.Fatal("expected the channel to be closed after the drop")
	}
	if !errors.Is(sub.Err(), ErrLagged) {
		t.Fatalf("Err() = %v, want ErrLagged", sub.Err())
	}
}

func TestFinishClosesSubscribersCleanly(t *testing.T) {
	s := newStream("s", 0)
	sub, _ := s.Subscribe(0, 0)
	defer sub.Close()

	s.Finish()

	if _, open := recv(t, sub); open {
		t.Fatal("expected the channel to close once the stream finished")
	}
	if err := sub.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil after a normal finish", err)
	}
	if !s.Done() {
		t.Fatal("Done() = false after Finish")
	}

	if _, err := s.Publish("too late"); !errors.Is(err, ErrStreamDone) {
		t.Fatalf("Publish after Finish: %v, want ErrStreamDone", err)
	}
}

func TestSubscribeAfterFinishGetsHistoryThenCloses(t *testing.T) {
	s := newStream("s", 0)
	if _, err := s.Publish("a"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	s.Finish()

	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if gap {
		t.Fatal("unexpected gap")
	}

	ev, open := recv(t, sub)
	if !open || ev.Data != "a" {
		t.Fatalf("got %+v, open=%v, want the buffered event still delivered", ev, open)
	}
	if _, open := recv(t, sub); open {
		t.Fatal("expected the channel to be closed since the stream was already done")
	}
}

func TestCloseDetachesWithoutError(t *testing.T) {
	s := newStream("s", 0)
	sub, _ := s.Subscribe(0, 0)

	sub.Close()
	sub.Close() // must be safe to call twice

	if err := sub.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil after a local Close", err)
	}

	if _, err := s.Publish("x"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(s.subs) != 0 {
		t.Fatalf("closed subscription still attached: %d subs", len(s.subs))
	}
}

func TestHubGetOrCreateReturnsSameStream(t *testing.T) {
	h := New(0)

	a := h.GetOrCreate("x")
	b := h.GetOrCreate("x")
	if a != b {
		t.Fatal("GetOrCreate returned different streams for the same id")
	}

	if _, ok := h.Stream("missing"); ok {
		t.Fatal("Stream found an id that was never created")
	}
	if got, ok := h.Stream("x"); !ok || got != a {
		t.Fatal("Stream did not return the stream created by GetOrCreate")
	}
}

func TestHubIDsSorted(t *testing.T) {
	h := New(0)
	for _, id := range []string{"c", "a", "b"} {
		h.GetOrCreate(id)
	}

	got := h.IDs()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("IDs() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IDs() = %v, want %v", got, want)
		}
	}
}

func TestHubRemoveFinishesAndForgets(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("x")
	sub, _ := s.Subscribe(0, 0)
	defer sub.Close()

	if !h.Remove("x") {
		t.Fatal("Remove reported no such stream")
	}
	if h.Remove("x") {
		t.Fatal("Remove reported success on an id it already removed")
	}
	if _, ok := h.Stream("x"); ok {
		t.Fatal("removed stream is still reachable through Stream")
	}
	if _, open := recv(t, sub); open {
		t.Fatal("Remove did not finish the stream: subscriber channel still open")
	}
}

func TestHubCloseAllFinishesEveryStream(t *testing.T) {
	h := New(0)
	subs := make([]*Subscription, 0, 3)
	for _, id := range []string{"a", "b", "c"} {
		s := h.GetOrCreate(id)
		sub, _ := s.Subscribe(0, 0)
		subs = append(subs, sub)
	}

	h.CloseAll()

	for _, sub := range subs {
		if _, open := recv(t, sub); open {
			t.Fatal("CloseAll left a subscriber channel open")
		}
		sub.Close()
	}
}
