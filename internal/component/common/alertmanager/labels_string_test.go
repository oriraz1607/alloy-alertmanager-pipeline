package alertmanager

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLabelsString(t *testing.T) {
	for _, labels := range []map[string]string{
		{}, {"a": ""}, {"severity": "critical"},
		{"severity": "critical", "alertname": "test", "instance": "server01"},
		{"custom_label": " spaces שלום 世界 : , { } \" \\ % \n\t\x00", "another": ""},
		{"arbitrary,:{}\"\\% name世界": "value"},
	} {
		encoded, err := LabelsToString(labels)
		require.NoError(t, err)
		for i := 0; i < 20; i++ {
			again, err := LabelsToString(labels)
			require.NoError(t, err)
			require.Equal(t, encoded, again)
		}
		require.True(t, json.Valid([]byte(`{"labels":"`+encoded+`"}`)))
		decoded, err := LabelsFromString(encoded)
		require.NoError(t, err)
		require.Equal(t, labels, decoded)
	}
	encoded, err := LabelsToString(map[string]string{"severity": "critical", "alertname": "test"})
	require.NoError(t, err)
	require.Equal(t, "{alertname:test,severity:critical}", encoded)
	encoded, err = LabelsToString(map[string]string{"message": `disk: almost, full`})
	require.NoError(t, err)
	require.Equal(t, "{message:disk%3A almost%2C full}", encoded)
	_, err = LabelsToString(map[string]string{"a": string([]byte{255})})
	require.Error(t, err)
}

func TestMalformedLabelsString(t *testing.T) {
	for _, input := range []string{"", "{severity}", "{severity:critical", "severity:critical}", "{a:%}", "{a:%2}", "{a:%GG}", "{a:%FF}", "{a:b,}", "{a:b,a:c}", "{a:b,%61:c}", "{a:b:c}", "{a:{b}}", "{a:\"}", "{a:\\}", "{a:\n}"} {
		got, err := LabelsFromString(input)
		require.Error(t, err, input)
		require.Nil(t, got, input)
	}
}

func FuzzLabelsString(f *testing.F) {
	f.Add("custom", "disk: almost, full")
	f.Fuzz(func(t *testing.T, key, value string) {
		labels := map[string]string{key: value}
		encoded, err := LabelsToString(labels)
		if err != nil {
			return
		}
		decoded, err := LabelsFromString(encoded)
		require.NoError(t, err)
		require.Equal(t, labels, decoded)
	})
}
