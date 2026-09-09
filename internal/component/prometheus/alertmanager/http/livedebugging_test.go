package http

import (
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	"github.com/grafana/alloy/internal/component/common/config"
	"github.com/grafana/alloy/internal/service/livedebugging"
	"github.com/grafana/alloy/internal/util/testlivedebugging"
	"github.com/grafana/alloy/syntax/alloytypes"
	"github.com/stretchr/testify/require"
)

func TestLiveDebuggingAuthenticatedRetries(t *testing.T) {
	requests := 0
	var requestsMut sync.Mutex
	payload := []byte("{ \"alertname\": \"TestAlert\" }\n")
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		requestsMut.Lock()
		defer requestsMut.Unlock()
		requests++
		user, password, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "test-user", user)
		require.Equal(t, "test-password", password)
		require.Equal(t, "custom-secret", r.Header.Get("X-Custom-Credential"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, payload, body)
		if requests == 1 {
			w.WriteHeader(500)
		}
		_, _ = io.WriteString(w, "test-password custom-secret")
	}))
	defer server.Close()
	args := testArguments(server.URL+"/alerts", jsonTransformer("test"))
	args.Endpoint.HTTPClientConfig.BasicAuth = &config.BasicAuth{Username: "test-user", Password: "test-password"}
	args.Endpoint.HTTPClientConfig.HTTPHeaders = &config.Headers{Headers: map[string][]alloytypes.Secret{"X-Custom-Credential": {"custom-secret"}}}
	sender := newSenderWithoutRun(t, args)
	svc := livedebugging.NewLiveDebugging()
	svc.SetEnabled(true)
	id := component.ParseID(sender.component.opts.ID)
	host := &testlivedebugging.FakeServiceHost{ComponentsInfo: map[component.ID]testlivedebugging.FakeInfo{id: {ComponentName: "prometheus.alertmanager.http", Component: sender.component}}}
	log := testlivedebugging.NewLog()
	require.NoError(t, svc.AddCallback(host, "test", livedebugging.ComponentID(id.String()), func(d livedebugging.Data) { log.Append(d.DataFunc()) }))
	sender.component.debugDataPublisher = alertpipeline.NewDebugPublisher(component.Options{ID: id.String(), GetServiceData: func(string) (any, error) { return svc, nil }})
	require.NoError(t, sender.component.sendWithRetry(t.Context(), payload))
	requestsMut.Lock()
	require.Equal(t, 2, requests)
	requestsMut.Unlock()
	text := strings.Join(log.Get(), "\n")
	for _, value := range []string{"test-user", "test-password", "custom-secret"} {
		require.NotContains(t, text, value)
	}
	for _, value := range []string{`"Authorization":["***"]`, `"X-Custom-Credential":["***"]`, "Retry attempt: 0", "Retry attempt: 1", "Status: 500 Internal Server Error", "Status: 200 OK", string(payload), "body omitted"} {
		require.Contains(t, text, value)
	}
}
