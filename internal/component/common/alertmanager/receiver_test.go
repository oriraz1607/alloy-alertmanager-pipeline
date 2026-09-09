package alertmanager

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestFanout(t *testing.T) {
	var first, second []Alert
	fanout := NewFanout([]Receiver{
		ReceiverFunc(func(_ context.Context, alerts []Alert) error {
			first = alerts
			alerts[0].Labels["mutated"] = "true"
			return nil
		}),
		ReceiverFunc(func(_ context.Context, alerts []Alert) error {
			second = alerts
			return nil
		}),
	})

	input := []Alert{{
		Alert: model.Alert{Labels: model.LabelSet{"alertname": "Example"}},
		State: model.AlertFiring,
	}}
	require.NoError(t, fanout.Send(t.Context(), input))
	require.Len(t, first, 1)
	require.Len(t, second, 1)
	require.NotContains(t, second[0].Labels, model.LabelName("mutated"))
	require.NotContains(t, input[0].Labels, model.LabelName("mutated"))
}

func TestFanoutReturnsReceiverError(t *testing.T) {
	want := errors.New("queue full")
	fanout := NewFanout([]Receiver{ReceiverFunc(func(context.Context, []Alert) error { return want })})
	require.ErrorIs(t, fanout.Send(t.Context(), []Alert{{}}), want)
}

func TestFanoutUpdatesFunctionReceivers(t *testing.T) {
	called := false
	fanout := NewFanout(nil)
	require.NotPanics(t, func() {
		fanout.UpdateChildren([]Receiver{ReceiverFunc(func(context.Context, []Alert) error {
			called = true
			return nil
		})})
	})
	require.NoError(t, fanout.Send(t.Context(), []Alert{{}}))
	require.True(t, called)
}
