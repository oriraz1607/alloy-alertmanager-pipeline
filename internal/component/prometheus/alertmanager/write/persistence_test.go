package write

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

func persistentArguments(t *testing.T, url string) Arguments {
	args := testArguments(url)
	args.Queue.Persistent = true
	args.Queue.Directory = t.TempDir()
	args.Queue.DrainTimeout = 20 * time.Millisecond
	return args
}

func TestPersistentRestartBatch(t *testing.T) {
	available := atomic.NewBool(false)
	attempts := atomic.NewInt32(0)
	delivered := make(chan int, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Inc()
		if !available.Load() {
			w.WriteHeader(500)
			return
		}
		var body []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(400)
			return
		}
		delivered <- len(body)
	}))
	defer server.Close()
	args := persistentArguments(t, server.URL)
	args.Endpoint.BatchSize = 2
	first := startWriter(t, args)
	require.NoError(t, first.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("A", model.AlertFiring), testAlert("B", model.AlertResolved)}))
	require.Eventually(t, func() bool { return attempts.Load() > 5 }, time.Second, time.Millisecond)
	_, err := os.Stat(filepath.Join(args.Queue.Directory, "queue.snapshot"))
	require.NoError(t, err)
	first.cancel()
	require.NoError(t, <-first.done)
	first.done = nil
	available.Store(true)
	second := startWriter(t, args)
	select {
	case n := <-delivered:
		require.Equal(t, 2, n)
	case <-time.After(time.Second):
		t.Fatal("pending batch not restored")
	}
	require.Eventually(t, func() bool {
		second.component.queue.mut.Lock()
		defer second.component.queue.mut.Unlock()
		return len(second.component.queue.items)+len(second.component.queue.inflight) == 0
	}, time.Second, time.Millisecond)
}

func TestPersistentCrashBoundaryAndLimits(t *testing.T) {
	args := persistentArguments(t, "http://localhost:1")
	args.Queue.Capacity = 1
	args.Queue.BlockOnOverflow = false
	first := newWriterWithoutRun(t, args)
	alert := testAlert("A", model.AlertResolved)
	require.NoError(t, first.receiver.Send(t.Context(), []alertpipeline.Alert{alert}))
	batch := first.component.queue.dequeue(1)
	require.Len(t, batch, 1)
	// HTTP may have succeeded, but acknowledgement has not committed. Capacity
	// remains occupied and recovery must resend this batch (at least once).
	require.ErrorIs(t, first.receiver.Send(t.Context(), []alertpipeline.Alert{alert}), errQueueFull)
	first.component.queue.stop()
	require.NoError(t, first.component.queue.store.lock.Unlock())
	second := newWriterWithoutRun(t, args)
	require.Equal(t, []alertpipeline.Alert{alert}, second.component.queue.dequeue(1))
	require.NoError(t, second.component.queue.acknowledge())
	require.NoError(t, second.component.queue.store.lock.Unlock())
	third := newWriterWithoutRun(t, args)
	require.Zero(t, third.component.queue.len())
	require.NoError(t, third.component.queue.store.lock.Unlock())
}

func TestPersistentDiskLimitAndCorruption(t *testing.T) {
	args := persistentArguments(t, "http://localhost:1")
	args.Queue.MaxDiskBytes = 4096
	writer := newWriterWithoutRun(t, args)
	large := testAlert("large", model.AlertFiring)
	large.Annotations["summary"] = model.LabelValue(strings.Repeat("x", 4096))
	require.ErrorIs(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{large}), errQueueFull)
	require.Zero(t, writer.component.queue.len())
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("small", model.AlertFiring)}))
	require.NoError(t, writer.component.queue.store.lock.Unlock())
	path := filepath.Join(args.Queue.Directory, "queue.snapshot")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(args.Queue.Directory, "queue.tmp"), []byte("partial"), 0600))
	store, pending, err := openQueueStore(args.Queue.Directory, args, newAlertTracker())
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.NoError(t, store.lock.Unlock())
	require.NoError(t, os.WriteFile(path, data[:len(data)/2], 0600))
	_, _, err = openQueueStore(args.Queue.Directory, args, newAlertTracker())
	require.ErrorContains(t, err, "checksum")
}

func TestPersistentTrackerAndExclusiveLock(t *testing.T) {
	args := persistentArguments(t, "http://localhost:1")
	args.ResolvedRetention = time.Minute
	writer := newWriterWithoutRun(t, args)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("firing", model.AlertFiring), testAlert("resolved", model.AlertResolved)}))
	before := writer.component.tracker.persisted()
	_, _, err := openQueueStore(args.Queue.Directory, args, newAlertTracker())
	require.ErrorContains(t, err, "already in use")
	writer.component.queue.dequeue(2)
	require.NoError(t, writer.component.queue.acknowledge())
	require.NoError(t, writer.component.queue.store.lock.Unlock())
	tracker := newAlertTracker()
	store, pending, err := openQueueStore(args.Queue.Directory, args, tracker)
	require.NoError(t, err)
	defer store.lock.Unlock()
	require.Empty(t, pending)
	after := tracker.persisted()
	require.Len(t, after, len(before))
	for _, want := range before {
		found := false
		for _, got := range after {
			if got.Alert.Fingerprint() == want.Alert.Fingerprint() {
				require.Equal(t, want.Alert, got.Alert)
				require.True(t, want.LastSeen.Equal(got.LastSeen))
				require.True(t, want.Until.Equal(got.Until))
				found = true
			}
		}
		require.True(t, found)
	}
}

func TestHTTP500And429Retry(t *testing.T) {
	for _, status := range []int{500, 429, 400} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			attempts := atomic.NewInt32(0)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if attempts.Inc() <= 3 {
					w.WriteHeader(status)
				}
			}))
			defer server.Close()
			args := persistentArguments(t, server.URL)
			writer := startWriter(t, args)
			require.NoError(t, writer.receiver.Send(context.Background(), []alertpipeline.Alert{testAlert("test", model.AlertResolved)}))
			want := int32(4)
			if status == 400 {
				want = 1
			}
			require.Eventually(t, func() bool { return attempts.Load() >= want }, time.Second, time.Millisecond)
			require.Eventually(t, func() bool {
				writer.component.queue.mut.Lock()
				defer writer.component.queue.mut.Unlock()
				return len(writer.component.queue.items)+len(writer.component.queue.inflight) == 0
			}, time.Second, time.Millisecond)
			writer.cancel()
			require.NoError(t, <-writer.done)
			writer.done = nil
			require.Equal(t, want, attempts.Load())
		})
	}
}

func TestPersistentBackpressureReleasedOnAcknowledgement(t *testing.T) {
	args := persistentArguments(t, "http://localhost:1")
	args.Queue.Capacity = 1
	writer := newWriterWithoutRun(t, args)
	defer writer.component.queue.store.lock.Unlock()
	alert := []alertpipeline.Alert{testAlert("A", model.AlertResolved)}
	require.NoError(t, writer.receiver.Send(t.Context(), alert))
	writer.component.queue.dequeue(1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- writer.receiver.Send(ctx, alert) }()
	require.NoError(t, writer.component.queue.acknowledge())
	// The sender may consume work notifications independently of the producer.
	select {
	case <-writer.component.queue.changed:
	default:
	}
	require.NoError(t, <-done)
}

func TestPersistentReloadPreservesPending(t *testing.T) {
	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received <- struct{}{} }))
	defer server.Close()
	args := persistentArguments(t, "http://localhost:1")
	writer := newWriterWithoutRun(t, args)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("pending", model.AlertResolved)}))
	changed := args
	changed.Queue.Persistent = false
	changed.Queue.Directory = ""
	require.ErrorContains(t, writer.component.Update(changed), "restarting")
	changed = args
	changed.Queue.MaxDiskBytes *= 2
	require.ErrorContains(t, writer.component.Update(changed), "restarting")
	changed = args
	changed.Endpoint.URL = server.URL
	require.NoError(t, writer.component.Update(changed))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- writer.component.Run(ctx) }()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("pending alert not sent to updated endpoint")
	}
	cancel()
	require.NoError(t, <-done)
}

func TestPersistent429DisabledIsTerminal(t *testing.T) {
	attempts := atomic.NewInt32(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { attempts.Inc(); w.WriteHeader(429) }))
	defer server.Close()
	args := persistentArguments(t, server.URL)
	args.Endpoint.RetryOnHTTP429 = false
	writer := startWriter(t, args)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("limited", model.AlertResolved)}))
	require.Eventually(t, func() bool {
		writer.component.queue.mut.Lock()
		defer writer.component.queue.mut.Unlock()
		return attempts.Load() == 1 && len(writer.component.queue.items)+len(writer.component.queue.inflight) == 0
	}, time.Second, time.Millisecond)
}

func TestPersistentStorageFailureRejectsAcceptance(t *testing.T) {
	args := persistentArguments(t, "http://localhost:1")
	writer := newWriterWithoutRun(t, args)
	defer writer.component.queue.store.lock.Unlock()
	// A directory at the temporary-file path reliably fails writes even as root.
	require.NoError(t, os.Mkdir(filepath.Join(args.Queue.Directory, "queue.tmp"), 0700))
	require.Error(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("rejected", model.AlertFiring)}))
	require.Zero(t, writer.component.queue.len())
	firing, resolved := writer.component.tracker.counts()
	require.Zero(t, firing)
	require.Zero(t, resolved)
}
