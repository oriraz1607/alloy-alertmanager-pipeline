package route

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

type recorder struct {
	calls [][]alertpipeline.Alert
}

func (r *recorder) Send(_ context.Context, alerts []alertpipeline.Alert) error {
	r.calls = append(r.calls, alerts)
	return nil
}

func newRouter(t *testing.T, args Arguments) (*Component, alertpipeline.Receiver) {
	t.Helper()
	var receiver alertpipeline.Receiver
	c, err := New(component.Options{
		ID:            "prometheus.alertmanager.route.test",
		OnStateChange: func(e component.Exports) { receiver = e.(Exports).Receiver },
	}, args)
	require.NoError(t, err)
	require.NotNil(t, receiver)
	return c, receiver
}

func testAlert(name, severity string) alertpipeline.Alert {
	return alertpipeline.Alert{
		Alert: model.Alert{
			Labels:       model.LabelSet{"alertname": model.LabelValue(name), "severity": model.LabelValue(severity)},
			Annotations:  model.LabelSet{"summary": model.LabelValue("summary for " + name)},
			StartsAt:     time.Date(2026, 5, 15, 12, 30, 0, 123456789, time.UTC),
			EndsAt:       time.Date(2026, 5, 15, 12, 45, 0, 987654321, time.UTC),
			GeneratorURL: "https://prom.example/graph",
		},
		State:             model.AlertFiring,
		SourceFingerprint: "source-" + name,
	}
}

func TestFirstMatchAndContinue(t *testing.T) {
	for _, tc := range []struct {
		name    string
		proceed bool
	}{
		{"first match", false},
		{"continue", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, second, third, fallback := &recorder{}, &recorder{}, &recorder{}, &recorder{}
			_, receiver := newRouter(t, Arguments{
				Routes: []Route{
					{Matchers: []string{`severity="critical"`}, ForwardTo: []alertpipeline.Receiver{first, second, first}, Continue: tc.proceed},
					{Matchers: []string{`alertname="overlap"`}, ForwardTo: []alertpipeline.Receiver{second, third}},
				},
				DefaultForwardTo: []alertpipeline.Receiver{fallback, fallback},
			})

			overlap := testAlert("overlap", "critical")
			onlyFirst := testAlert("other", "critical")
			onlySecond := testAlert("overlap", "warning")
			unmatched := testAlert("other", "warning")
			onlySecond.State = model.AlertResolved
			// The same incoming alert can occur twice. Deduplication applies to
			// destinations for each occurrence, not to the input collection.
			input := []alertpipeline.Alert{overlap, unmatched, onlySecond, onlyFirst, overlap}
			require.NoError(t, receiver.Send(t.Context(), input))
			require.Equal(t, [][]alertpipeline.Alert{{overlap, onlyFirst, overlap}}, first.calls)
			require.Equal(t, [][]alertpipeline.Alert{{overlap, onlySecond, onlyFirst, overlap}}, second.calls)
			if tc.proceed {
				require.Equal(t, [][]alertpipeline.Alert{{overlap, onlySecond, overlap}}, third.calls)
			} else {
				require.Equal(t, [][]alertpipeline.Alert{{onlySecond}}, third.calls)
			}
			require.Equal(t, [][]alertpipeline.Alert{{unmatched}}, fallback.calls)
		})
	}
}

func TestMatcherSemantics(t *testing.T) {
	missing := testAlert("missing", "critical")
	empty := testAlert("empty", "critical")
	empty.Labels["region"] = ""
	west := testAlert("west", "critical")
	west.Labels["region"] = "west"
	northwest := testAlert("northwest", "critical")
	northwest.Labels["region"] = "northwest"
	input := []alertpipeline.Alert{missing, empty, west, northwest}

	for _, tc := range []struct {
		expr string
		want []alertpipeline.Alert
	}{
		{`region=""`, []alertpipeline.Alert{missing, empty}},
		{`region!="west"`, []alertpipeline.Alert{missing, empty, northwest}},
		{`region=~"west|east"`, []alertpipeline.Alert{west}},
		{`region!~"west|east"`, []alertpipeline.Alert{missing, empty, northwest}},
		{`region=~".*"`, input},
		{`region!~".+"`, []alertpipeline.Alert{missing, empty}},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			selected := &recorder{}
			_, receiver := newRouter(t, Arguments{
				Routes:           []Route{{Matchers: []string{tc.expr}, ForwardTo: []alertpipeline.Receiver{selected}}},
				DefaultForwardTo: []alertpipeline.Receiver{&recorder{}},
			})
			require.NoError(t, receiver.Send(t.Context(), input))
			require.Equal(t, [][]alertpipeline.Alert{tc.want}, selected.calls)
		})
	}
}

func TestRouteRequiresAllMatchers(t *testing.T) {
	payments, all := &recorder{}, &recorder{}
	_, receiver := newRouter(t, Arguments{
		Routes: []Route{{
			Matchers:  []string{`namespace="payments"`, `environment="prod"`},
			ForwardTo: []alertpipeline.Receiver{payments, all},
		}},
		DefaultForwardTo: []alertpipeline.Receiver{all},
	})
	input := []alertpipeline.Alert{testAlert("staging", "warning"), testAlert("prod", "warning"), testAlert("infra", "critical")}
	input[0].Labels["namespace"], input[0].Labels["environment"] = "payments", "staging"
	input[1].Labels["namespace"], input[1].Labels["environment"] = "payments", "prod"
	input[2].Labels["namespace"], input[2].Labels["environment"] = "infra", "prod"
	require.NoError(t, receiver.Send(t.Context(), input))
	require.Equal(t, [][]alertpipeline.Alert{{input[1]}}, payments.calls)
	// Matched and default selections of the same receiver form a single ordered batch.
	require.Equal(t, [][]alertpipeline.Alert{input}, all.calls)
}

func TestFunctionReceiverIdentity(t *testing.T) {
	var calls []string
	sender := func(name string) alertpipeline.Receiver {
		return alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error {
			calls = append(calls, name)
			return nil
		})
	}
	var destinations []alertpipeline.Receiver
	for _, name := range []string{"first", "second"} {
		destinations = append(destinations, sender(name))
	}
	destinations = append(destinations, destinations[0])
	_, receiver := newRouter(t, Arguments{DefaultForwardTo: destinations})
	require.NoError(t, receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("one", "critical")}))
	require.Equal(t, []string{"first", "second"}, calls)
}

func TestAdmissionFailureAndRetry(t *testing.T) {
	first, last := &recorder{}, &recorder{}
	full := errors.New("queue full")
	refusal := full
	middle := alertpipeline.ReceiverFunc(func(_ context.Context, alerts []alertpipeline.Alert) error {
		// A downstream receiver may mutate its own batch.
		alerts[0].Labels["severity"] = "mutated"
		alerts[0].Annotations["summary"] = "mutated"
		return refusal
	})
	_, receiver := newRouter(t, Arguments{DefaultForwardTo: []alertpipeline.Receiver{first, middle, last}})
	input := []alertpipeline.Alert{testAlert("overlap", "critical")}
	expected := alertpipeline.CloneAlerts(input)

	require.ErrorIs(t, receiver.Send(t.Context(), input), full)
	require.Equal(t, [][]alertpipeline.Alert{expected}, first.calls)
	require.Empty(t, last.calls)
	require.Equal(t, expected, input)

	refusal = nil
	require.NoError(t, receiver.Send(t.Context(), input))
	require.Equal(t, [][]alertpipeline.Alert{expected, expected}, first.calls)
	require.Equal(t, [][]alertpipeline.Alert{expected}, last.calls)
	require.Equal(t, expected, input)
}

func TestCancellationDuringAdmission(t *testing.T) {
	entered := make(chan struct{})
	blocking := alertpipeline.ReceiverFunc(func(ctx context.Context, _ []alertpipeline.Alert) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	_, receiver := newRouter(t, Arguments{DefaultForwardTo: []alertpipeline.Receiver{blocking}})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- receiver.Send(ctx, []alertpipeline.Alert{testAlert("one", "critical")}) }()
	<-entered
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestInvalidConfigurationAndReload(t *testing.T) {
	old, replacement := &recorder{}, &recorder{}
	original := Arguments{DefaultForwardTo: []alertpipeline.Receiver{old}}
	c, receiver := newRouter(t, original)
	good := Route{Matchers: []string{`severity="critical"`}, ForwardTo: []alertpipeline.Receiver{replacement}}
	withSecond := func(second Route) Arguments {
		return Arguments{Routes: []Route{good, second}, DefaultForwardTo: original.DefaultForwardTo}
	}
	for _, tc := range []struct {
		name     string
		bad      Arguments
		location string
	}{
		{"empty default", Arguments{Routes: []Route{good}}, "default_forward_to"},
		{"no route destinations", withSecond(Route{Matchers: good.Matchers}), `route[^0-9]*2`},
		{"bad matcher", withSecond(Route{Matchers: []string{"severity"}, ForwardTo: good.ForwardTo}), `route[^0-9]*2`},
		{"bad regex", withSecond(Route{Matchers: []string{`severity=~"["`}, ForwardTo: good.ForwardTo}), `route[^0-9]*2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(component.Options{}, tc.bad)
			require.Error(t, err)
			require.Regexp(t, tc.location, err.Error())

			err = c.Update(tc.bad)
			require.Error(t, err)
			require.Regexp(t, tc.location, err.Error())
			input := []alertpipeline.Alert{testAlert(tc.name, "critical")}
			require.NoError(t, receiver.Send(t.Context(), input))
			require.Equal(t, [][]alertpipeline.Alert{input}, old.calls)
			require.Empty(t, replacement.calls)
			old.calls = nil
		})
	}
	input := []alertpipeline.Alert{testAlert("replacement", "critical")}
	require.NoError(t, c.Update(Arguments{Routes: []Route{good}, DefaultForwardTo: original.DefaultForwardTo}))
	require.NoError(t, receiver.Send(t.Context(), input))
	require.Equal(t, [][]alertpipeline.Alert{input}, replacement.calls)
	require.Empty(t, old.calls)
}

func TestCollectionSeesOneConfiguration(t *testing.T) {
	first, second := &recorder{}, &recorder{}
	configs := []Arguments{
		{DefaultForwardTo: []alertpipeline.Receiver{first}},
		{DefaultForwardTo: []alertpipeline.Receiver{second}},
	}
	c, receiver := newRouter(t, configs[0])
	start := make(chan struct{})
	done := make(chan error, 1)
	const collections = 100
	go func() {
		<-start
		for i := range collections {
			if err := c.Update(configs[i%2]); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	input := []alertpipeline.Alert{testAlert("a", "critical"), testAlert("b", "warning"), testAlert("c", "critical")}
	close(start)
	for range collections {
		require.NoError(t, receiver.Send(t.Context(), input))
	}
	require.NoError(t, <-done)
	require.Equal(t, collections, len(first.calls)+len(second.calls))
	for _, sink := range []*recorder{first, second} {
		for _, batch := range sink.calls {
			require.Equal(t, input, batch)
		}
	}
}

func TestReloadDrainsOldAdmissionWhileNewSendCanTimeOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release, finishOld := context.WithCancel(t.Context())
		defer finishOld()
		oldFirst := alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error {
			<-release.Done()
			return nil
		})
		oldLast := &recorder{}
		c, receiver := newRouter(t, Arguments{DefaultForwardTo: []alertpipeline.Receiver{oldFirst, oldLast}})
		oldBatch := []alertpipeline.Alert{testAlert("old", "critical")}
		oldDone := make(chan error, 1)
		go func() { oldDone <- receiver.Send(t.Context(), oldBatch) }()
		synctest.Wait() // The old collection is blocked at its first destination.

		var newBatch []alertpipeline.Alert
		replacement := alertpipeline.ReceiverFunc(func(ctx context.Context, alerts []alertpipeline.Alert) error {
			newBatch = alerts
			<-ctx.Done()
			return ctx.Err()
		})
		updated := make(chan error, 1)
		go func() { updated <- c.Update(Arguments{DefaultForwardTo: []alertpipeline.Receiver{replacement}}) }()
		synctest.Wait() // The update has either returned or is waiting for the old send.
		select {
		case err := <-updated:
			t.Fatalf("Update returned before old admission finished: %v", err)
		default:
		}

		input := []alertpipeline.Alert{testAlert("new", "warning")}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.ErrorIs(t, receiver.Send(ctx, input), context.DeadlineExceeded)
		require.Equal(t, input, newBatch)
		require.Empty(t, oldLast.calls)

		finishOld()
		require.NoError(t, <-oldDone)
		require.NoError(t, <-updated)
		require.Equal(t, [][]alertpipeline.Alert{oldBatch}, oldLast.calls)
	})
}
