package write

import (
	"sync"
	"time"

	"github.com/prometheus/common/model"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

type trackedAlert struct {
	alert    alertpipeline.Alert
	lastSeen time.Time
	until    time.Time
}

type alertTracker struct {
	mut      sync.Mutex
	firing   map[model.Fingerprint]trackedAlert
	resolved map[model.Fingerprint]trackedAlert
}

func newAlertTracker() *alertTracker {
	return &alertTracker{
		firing:   make(map[model.Fingerprint]trackedAlert),
		resolved: make(map[model.Fingerprint]trackedAlert),
	}
}

func (t *alertTracker) observe(alerts []alertpipeline.Alert, now time.Time, resolvedRetention time.Duration) {
	t.mut.Lock()
	defer t.mut.Unlock()
	for _, alert := range alerts {
		fingerprint := alert.Fingerprint()
		switch alert.State {
		case model.AlertFiring:
			t.firing[fingerprint] = trackedAlert{alert: alert.Clone(), lastSeen: now}
			delete(t.resolved, fingerprint)
		case model.AlertResolved:
			delete(t.firing, fingerprint)
			if resolvedRetention > 0 {
				t.resolved[fingerprint] = trackedAlert{alert: alert.Clone(), lastSeen: now, until: now.Add(resolvedRetention)}
			} else {
				delete(t.resolved, fingerprint)
			}
		}
	}
}

func (t *alertTracker) snapshot(now time.Time, firingTimeout time.Duration) (alerts []alertpipeline.Alert, expired int) {
	t.mut.Lock()
	defer t.mut.Unlock()
	alerts = make([]alertpipeline.Alert, 0, len(t.firing)+len(t.resolved))
	for fingerprint, tracked := range t.firing {
		if now.Sub(tracked.lastSeen) >= firingTimeout {
			delete(t.firing, fingerprint)
			expired++
			continue
		}
		alerts = append(alerts, tracked.alert.Clone())
	}
	for fingerprint, tracked := range t.resolved {
		if !now.Before(tracked.until) {
			delete(t.resolved, fingerprint)
			continue
		}
		alerts = append(alerts, tracked.alert.Clone())
	}
	return alerts, expired
}

func (t *alertTracker) counts() (firing, resolved int) {
	t.mut.Lock()
	defer t.mut.Unlock()
	return len(t.firing), len(t.resolved)
}
