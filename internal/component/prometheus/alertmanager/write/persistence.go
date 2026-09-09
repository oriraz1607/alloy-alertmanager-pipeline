package write

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	"github.com/prometheus/common/model"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

// A bounded snapshot is deliberately used instead of the metric/log WALs:
// those encode signal-specific records and their segment limits are not a hard
// disk bound. Two snapshot slots bound both normal and compaction disk usage.
// The checksum detects truncation/corruption; rename and fsync commit a snapshot.
type queueSnapshot struct {
	Version int
	Pending []alertpipeline.Alert
	Tracked []persistedAlert
}

type persistedAlert struct {
	Alert    alertpipeline.Alert
	LastSeen time.Time
	Until    time.Time
}

type queueStore struct {
	directory     string
	maxBytes      int64
	lock          *flock.Flock
	tracker       *alertTracker
	retention     time.Duration
	firingTimeout time.Duration
	failed        error
}

func (t *alertTracker) persisted() []persistedAlert {
	t.mut.Lock()
	defer t.mut.Unlock()
	result := make([]persistedAlert, 0, len(t.firing)+len(t.resolved))
	for _, a := range t.firing {
		result = append(result, persistedAlert{a.alert.Clone(), a.lastSeen, a.until})
	}
	for _, a := range t.resolved {
		result = append(result, persistedAlert{a.alert.Clone(), a.lastSeen, a.until})
	}
	return result
}

func (t *alertTracker) restore(records []persistedAlert) {
	t.mut.Lock()
	defer t.mut.Unlock()
	for _, a := range records {
		target := t.firing
		if a.Alert.State == model.AlertResolved {
			target = t.resolved
		}
		target[a.Alert.Fingerprint()] = trackedAlert{a.Alert.Clone(), a.LastSeen, a.Until}
	}
}

func openQueueStore(directory string, args Arguments, tracker *alertTracker) (*queueStore, []alertpipeline.Alert, error) {
	if directory == "" {
		return nil, nil, errors.New("persistent queue requires a component data path or queue_config directory")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, nil, err
	}
	// Commit newly created directory entries before accepting any alerts.
	for parent := filepath.Clean(directory); ; parent = filepath.Dir(parent) {
		dir, err := os.Open(parent)
		if err != nil {
			return nil, nil, err
		}
		err = dir.Sync()
		_ = dir.Close()
		if err != nil {
			return nil, nil, err
		}
		if filepath.Dir(parent) == parent {
			break
		}
	}
	s := &queueStore{directory: directory, maxBytes: args.Queue.MaxDiskBytes / 2, lock: flock.New(filepath.Join(directory, "queue.lock")), tracker: tracker, retention: args.ResolvedRetention, firingTimeout: args.FiringAlertTimeout}
	locked, err := s.lock.TryLock()
	if err != nil {
		return nil, nil, err
	}
	if !locked {
		return nil, nil, errors.New("persistent alert queue is already in use")
	}
	pending, err := s.load()
	if err != nil {
		_ = s.lock.Unlock()
		return nil, nil, err
	}
	return s, pending, nil
}

func (s *queueStore) load() ([]alertpipeline.Alert, error) {
	path := filepath.Join(s.directory, "queue.snapshot")
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		err = os.Remove(filepath.Join(s.directory, "queue.tmp"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, s.maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > s.maxBytes {
		return nil, errors.New("persisted queue exceeds max_disk_bytes; restore the previous limit")
	}
	if len(data) < sha256.Size {
		return nil, errors.New("truncated alert queue snapshot")
	}
	sum := sha256.Sum256(data[sha256.Size:])
	if !bytes.Equal(sum[:], data[:sha256.Size]) {
		return nil, errors.New("corrupt alert queue snapshot checksum")
	}
	var snapshot queueSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data[sha256.Size:])).Decode(&snapshot); err != nil {
		return nil, err
	}
	if snapshot.Version != 1 {
		return nil, fmt.Errorf("unsupported alert queue version %d", snapshot.Version)
	}
	for _, alert := range snapshot.Pending {
		if err := alert.Validate(); err != nil {
			return nil, err
		}
	}
	for _, record := range snapshot.Tracked {
		if err := record.Alert.Validate(); err != nil {
			return nil, err
		}
	}
	s.tracker.restore(snapshot.Tracked)
	// A temporary file was never acknowledged. The committed snapshot wins.
	if err := os.Remove(filepath.Join(s.directory, "queue.tmp")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return snapshot.Pending, nil
}

func (s *queueStore) save(pending, incoming []alertpipeline.Alert) error {
	if s.failed != nil {
		return s.failed
	}
	tracker := newAlertTracker()
	tracker.restore(s.tracker.persisted())
	tracker.snapshot(time.Now(), s.firingTimeout)
	tracker.observe(incoming, time.Now(), s.retention)
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(queueSnapshot{1, pending, tracker.persisted()}); err != nil {
		return err
	}
	if int64(body.Len()+sha256.Size) > s.maxBytes {
		return errQueueFull
	}
	sum := sha256.Sum256(body.Bytes())
	tmp := filepath.Join(s.directory, "queue.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	_, err = f.Write(append(sum[:], body.Bytes()...))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(s.directory, "queue.snapshot")); err != nil {
		return err
	}
	dir, err := os.Open(s.directory)
	if err == nil {
		err = dir.Sync()
		_ = dir.Close()
	}
	if err != nil {
		// The rename may have committed. Reject subsequent writes until restart;
		// recovery can replay this operation, but must never overwrite it.
		s.failed = fmt.Errorf("syncing alert queue directory: %w", err)
		return s.failed
	}
	return nil
}
