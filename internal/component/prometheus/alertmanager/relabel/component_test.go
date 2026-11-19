package relabel

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
	alloyrelabel "github.com/grafana/alloy/internal/component/common/relabel"
	"github.com/grafana/alloy/syntax"
)

func TestOrderedRulesFilterAlertsAndIsolateFanout(t *testing.T) {
	reg, ok := component.Get("prometheus.alertmanager.relabel")
	require.True(t, ok)
	require.True(t, reg.Community)

	args := parseRules(t, `
rule {
	source_labels = ["cluster", "clustername"]
	separator = ";"
	regex = ";(.+)"
	target_label = "cluster"
	replacement = "$1"
	action = "replace"
}
rule {
	regex = "clustername"
	action = "labeldrop"
}
rule {
	target_label = "source_network"
	replacement = "network-a"
}
rule {
	source_labels = ["severity"]
	regex = "critical"
	action = "keep"
}
rule {
	source_labels = ["cluster"]
	regex = "ignored"
	action = "drop"
}
rule {
	regex = "legacy_(.+)"
	replacement = "$1"
	action = "labelmap"
}
rule {
	regex = "alertname|cluster|source_network|team"
	action = "labelkeep"
}
`)
	var batches [][]alertpipeline.Alert
	args.ForwardTo = []alertpipeline.Receiver{alertpipeline.ReceiverFunc(func(_ context.Context, batch []alertpipeline.Alert) error {
		batches = append(batches, batch)
		return nil
	})}
	c, receiver := newComponent(t, args)
	require.Same(t, c, receiver)
	var sibling []alertpipeline.Alert
	upstream := alertpipeline.NewFanout([]alertpipeline.Receiver{
		receiver,
		alertpipeline.ReceiverFunc(func(_ context.Context, batch []alertpipeline.Alert) error {
			sibling = batch
			return nil
		}),
	})
	firing := sampleAlert()
	resolved := firing.Clone()
	resolved.State = model.AlertResolved
	resolved.EndsAt = firing.StartsAt.Add(time.Hour)
	existing := firing.Clone()
	existing.Labels["cluster"] = "preferred"
	empty := firing.Clone()
	empty.Labels["cluster"] = ""
	filtered := firing.Clone()
	filtered.Labels["clustername"] = "ignored"
	info := firing.Clone()
	info.Labels["severity"] = "info"
	input := []alertpipeline.Alert{firing, resolved, filtered, existing, info, empty}
	original := alertpipeline.CloneAlerts(input)
	require.NoError(t, upstream.Send(t.Context(), input))
	require.Equal(t, original, input)
	require.Equal(t, original, sibling)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 4)
	for i, index := range []int{0, 1, 3, 5} {
		want := original[index].Clone()
		cluster := model.LabelValue("fallback")
		if index == 3 {
			cluster = "preferred"
		}
		want.Labels = model.LabelSet{
			"alertname": "HighCPU", "cluster": cluster, "source_network": "network-a", "team": "noc",
		}
		require.Equal(t, want, batches[0][i])
	}
	require.Equal(t, batches[0][0].Fingerprint(), batches[0][1].Fingerprint())
	require.NotEqual(t, firing.Fingerprint(), batches[0][0].Fingerprint())

	require.NoError(t, receiver.Send(t.Context(), []alertpipeline.Alert{info, filtered}))
	require.NoError(t, receiver.Send(t.Context(), nil))
	require.Len(t, batches, 1)
}

func TestPrometheusActionsAndReceiverOwnership(t *testing.T) {
	args := parseRules(t, `
rule {
	source_labels = ["legacy_team"]
	target_label = "upper"
	action = "uppercase"
}
rule {
	source_labels = ["upper"]
	target_label = "lower"
	action = "lowercase"
}
rule {
	source_labels = ["lower"]
	target_label = "legacy_team"
	action = "keepequal"
}
rule {
	source_labels = ["upper"]
	target_label = "legacy_team"
	action = "dropequal"
}
rule {
	source_labels = ["upper"]
	target_label = "shard"
	modulus = 1
	action = "hashmod"
}
`)
	args.ForwardTo = []alertpipeline.Receiver{alertpipeline.ReceiverFunc(func(_ context.Context, batch []alertpipeline.Alert) error {
		require.Len(t, batch, 1)
		require.Equal(t, model.LabelValue("NOC"), batch[0].Labels["upper"])
		require.Equal(t, model.LabelValue("noc"), batch[0].Labels["lower"])
		require.Equal(t, model.LabelValue("0"), batch[0].Labels["shard"])
		batch[0].Labels["alertname"] = "mutated"
		batch[0].Annotations["summary"] = "mutated"
		return nil
	})}
	_, receiver := newComponent(t, args)
	input := []alertpipeline.Alert{sampleAlert()}
	original := alertpipeline.CloneAlerts(input)
	require.NoError(t, receiver.Send(t.Context(), input))
	require.Equal(t, original, input)
}

func TestValidationAndReload(t *testing.T) {
	for _, rule := range []string{
		"action = \"unknown\"",
		"action = \"keep\"\nsource_labels = [\"\"]",
		"action = \"replace\"",
		"action = \"drop\"\nregex = \"[\"",
		"action = \"replace\"\ntarget_label = \"!invalid\"",
		"action = \"labeldrop\"\ntarget_label = \"x\"",
	} {
		var args Arguments
		require.Error(t, syntax.Unmarshal([]byte("forward_to = []\nrule {\n"+rule+"\n}\n"), &args))
	}

	initial := parseRules(t, "rule {\ntarget_label = \"environment\"\nreplacement = \"one\"\n}")
	var environments []model.LabelValue
	initial.ForwardTo = []alertpipeline.Receiver{alertpipeline.ReceiverFunc(func(_ context.Context, batch []alertpipeline.Alert) error {
		environments = append(environments, batch[0].Labels["environment"])
		return nil
	})}
	c, receiver := newComponent(t, initial)
	// The rejected configuration would remove the old destination as well.
	invalid := alloyrelabel.DefaultRelabelConfig
	require.Error(t, c.Update(Arguments{Rules: []*alloyrelabel.Config{&invalid}}))
	require.NoError(t, receiver.Send(t.Context(), []alertpipeline.Alert{sampleAlert()}))

	next := parseRules(t, "rule {\ntarget_label = \"environment\"\nreplacement = \"two\"\n}")
	next.ForwardTo = initial.ForwardTo
	require.NoError(t, c.Update(next))
	require.NoError(t, receiver.Send(t.Context(), []alertpipeline.Alert{sampleAlert()}))
	require.Equal(t, []model.LabelValue{"one", "two"}, environments)
}

func TestForwardErrorsAndCancellation(t *testing.T) {
	rejected := errors.New("queue full")
	var calls int
	args := Arguments{ForwardTo: []alertpipeline.Receiver{alertpipeline.ReceiverFunc(func(ctx context.Context, _ []alertpipeline.Alert) error {
		require.Same(t, t.Context(), ctx)
		calls++
		return rejected
	})}}
	c, receiver := newComponent(t, args)
	input := []alertpipeline.Alert{sampleAlert()}
	require.ErrorIs(t, receiver.Send(t.Context(), input), rejected)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, receiver.Send(ctx, input), context.Canceled)
	require.Equal(t, 1, calls)

	// Removing every label leaves an invalid typed alert. Only an explicit
	// filtering action may silently discard an otherwise matching alert.
	dropLabels := parseRules(t, "rule {\naction = \"labeldrop\"\nregex = \".*\"\n}")
	dropLabels.ForwardTo = args.ForwardTo
	require.NoError(t, c.Update(dropLabels))
	require.ErrorContains(t, receiver.Send(t.Context(), input), "at least one label pair required")
	require.Equal(t, 1, calls)
}

func TestReloadDrainsInFlightCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blocked := alertpipeline.ReceiverFunc(func(ctx context.Context, _ []alertpipeline.Alert) error {
			<-ctx.Done()
			return ctx.Err()
		})
		c, receiver := newComponent(t, Arguments{ForwardTo: []alertpipeline.Receiver{blocked}})
		input := []alertpipeline.Alert{sampleAlert()}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		admission := make(chan error, 1)
		go func() { admission <- receiver.Send(ctx, input) }()
		synctest.Wait()

		var got []alertpipeline.Alert
		next := parseRules(t, "rule {\ntarget_label = \"generation\"\nreplacement = \"new\"\n}")
		next.ForwardTo = []alertpipeline.Receiver{alertpipeline.ReceiverFunc(func(_ context.Context, alerts []alertpipeline.Alert) error {
			got = alerts
			return nil
		})}
		reloaded := make(chan error, 1)
		go func() { reloaded <- c.Update(next) }()
		synctest.Wait()
		require.Empty(t, reloaded) // Old receiver is still in use.
		require.NoError(t, receiver.Send(t.Context(), input))
		require.Len(t, got, 1)
		require.Equal(t, model.LabelValue("new"), got[0].Labels["generation"])
		cancel()
		require.ErrorIs(t, <-admission, context.Canceled)
		require.NoError(t, <-reloaded)
	})
}

func parseRules(t *testing.T, body string) Arguments {
	t.Helper()
	var args Arguments
	require.NoError(t, syntax.Unmarshal([]byte("forward_to = []\n"+body+"\n"), &args))
	return args
}

func newComponent(t *testing.T, args Arguments) (*Component, alertpipeline.Receiver) {
	t.Helper()
	var receiver alertpipeline.Receiver
	c, err := New(component.Options{OnStateChange: func(e component.Exports) {
		receiver = e.(Exports).Receiver
	}}, args)
	require.NoError(t, err)
	require.NotNil(t, receiver)
	return c, receiver
}

func sampleAlert() alertpipeline.Alert {
	return alertpipeline.Alert{
		Alert: model.Alert{
			Labels: model.LabelSet{
				"alertname": "HighCPU", "clustername": "fallback", "severity": "critical", "legacy_team": "noc",
			},
			Annotations:  model.LabelSet{"summary": "CPU is high"},
			StartsAt:     time.Unix(12345, 0).UTC(),
			GeneratorURL: "https://prometheus.example/graph",
		},
		State:             model.AlertFiring,
		SourceFingerprint: "original-source-id",
	}
}
