package alertmanager

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/service/livedebugging"
	"github.com/grafana/alloy/internal/util/testlivedebugging"
	"github.com/stretchr/testify/require"
)

func debugTestPublisher(t *testing.T) (DebugPublisher, *testlivedebugging.Log) {
	t.Helper()
	svc := livedebugging.NewLiveDebugging()
	svc.SetEnabled(true)
	host := &testlivedebugging.FakeServiceHost{ComponentsInfo: map[component.ID]testlivedebugging.FakeInfo{
		component.ParseID("prometheus.alertmanager.http.test"): {ComponentName: "prometheus.alertmanager.http", Component: &testlivedebugging.FakeComponentLiveDebugging{}},
	}}
	log := testlivedebugging.NewLog()
	require.NoError(t, svc.AddCallback(host, "test", "prometheus.alertmanager.http.test", func(d livedebugging.Data) { log.Append(d.DataFunc()) }))
	return NewDebugPublisher(component.Options{ID: "prometheus.alertmanager.http.test", GetServiceData: func(string) (any, error) { return svc, nil }}), log
}

func TestDebugRequestRedactionAndFidelity(t *testing.T) {
	d, log := debugTestPublisher(t)
	req := httptest.NewRequest(http.MethodPost, "https://username:password@example.test/a%2Fb?token=query-secret", nil)
	for _, name := range []string{"aUtHoRiZaTiOn", "Proxy-Authorization", "Cookie", "Set-Cookie", "X-API-Key", "Custom-Credential"} {
		req.Header[name] = []string{"header-secret"}
	}
	req.Header.Set("Content-Type", "application/json")
	originalHeaders := req.Header.Clone()
	body := []byte("{ \"alertname\": \"TestAlert\" }\n")
	d.Request("HTTP REQUEST", req, body, 2)
	text := strings.Join(log.Get(), "\n")
	for _, secret := range []string{"username", "password", "query-secret", "header-secret"} {
		require.NotContains(t, text, secret)
	}
	require.Contains(t, text, "https://example.test/a%2Fb?token=***")
	require.Contains(t, text, "application/json")
	require.Contains(t, text, "Retry attempt: 2")
	require.True(t, strings.HasSuffix(text, string(body)))
	require.Equal(t, originalHeaders, req.Header, "debugging must not mutate the request")
}

func TestPostJSONLiveDebugging(t *testing.T) {
	for _, active := range []bool{false, true} {
		for _, status := range []int{200, 500} {
			t.Run(fmt.Sprintf("active=%t/status=%d", active, status), func(t *testing.T) {
				d, log := debugTestPublisher(t)
				if !active {
					d = DebugPublisher{}
				}
				body := []byte("{\n  \"alertname\": \"TestAlert\"\n}\n")
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					got, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					require.Equal(t, body, got)
					w.Header().Set("Set-Cookie", "session-secret")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, "downstream receiver rejected alert")
				}))
				defer server.Close()
				_, err := PostJSON(t.Context(), server.Client(), server.URL+"/alerts", body, nil, time.Second, HTTPDebug{Publisher: d, Retry: 3})
				if status == 200 {
					require.NoError(t, err)
				} else {
					var failure *HTTPFailure
					require.ErrorAs(t, err, &failure)
					require.Equal(t, status, failure.StatusCode)
				}
				text := strings.Join(log.Get(), "\n")
				if !active {
					require.Empty(t, text)
					return
				}
				require.Contains(t, text, string(body))
				require.Contains(t, text, "HTTP RESPONSE")
				require.Contains(t, text, "downstream receiver rejected alert")
				require.Contains(t, text, "Retry attempt: 3")
				require.NotContains(t, text, "session-secret")
			})
		}
	}
}

type debugRoundTripper func(*http.Request) (*http.Response, error)

func (f debugRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDebugTransportFailureDoesNotDumpErrorCredentials(t *testing.T) {
	for _, err := range []error{x509.UnknownAuthorityError{}, errors.New("Bearer transport-secret")} {
		d, log := debugTestPublisher(t)
		client := &http.Client{Transport: debugRoundTripper(func(*http.Request) (*http.Response, error) { return nil, err })}
		_, got := PostJSON(context.Background(), client, "https://user:password@example.test/alerts?key=query-secret", []byte(`{}`), nil, time.Second, HTTPDebug{Publisher: d})
		require.Error(t, got)
		text := strings.Join(log.Get(), "\n")
		require.Contains(t, text, "HTTP TRANSPORT ERROR")
		for _, secret := range []string{"password", "query-secret", "transport-secret"} {
			require.NotContains(t, text, secret)
		}
		if ClassifyRequestError(err) == HTTPFailureTLS {
			require.Contains(t, text, "x509: certificate signed by unknown authority")
		}
	}
}

func TestDebugResponseBoundAndCredentialReflection(t *testing.T) {
	for _, omit := range []bool{false, true} {
		d, log := debugTestPublisher(t)
		reader := &debugCountingReader{Reader: strings.NewReader(strings.Repeat("x", 8192))}
		client := &http.Client{Transport: debugRoundTripper(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: reader, Request: r}, nil
		})}
		_, err := PostJSON(t.Context(), client, "http://example.test/alerts", nil, nil, time.Second, HTTPDebug{Publisher: d, OmitResponseBody: omit})
		require.NoError(t, err)
		require.Equal(t, 4096, reader.read)
		require.True(t, reader.closed)
		text := strings.Join(log.Get(), "\n")
		require.Contains(t, text, "may be truncated")
		if omit {
			require.Contains(t, text, "body omitted")
			require.NotContains(t, text, strings.Repeat("x", 10))
		} else {
			require.Contains(t, text, strings.Repeat("x", 4096))
		}
	}
}

type debugCountingReader struct {
	io.Reader
	read   int
	closed bool
}

func (r *debugCountingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.read += n
	return n, err
}
func (r *debugCountingReader) Close() error { r.closed = true; return nil }

func TestDebugFormattingIsLazy(t *testing.T) {
	for _, get := range []func(string) (any, error){nil, func(string) (any, error) { return nil, errors.New("unavailable") }, func(string) (any, error) { return struct{}{}, nil }, func(string) (any, error) { return livedebugging.NewLiveDebugging(), nil }} {
		d := NewDebugPublisher(component.Options{GetServiceData: get})
		d.Publish(1, func() string { t.Fatal("formatted without subscribers"); return "" })
		require.False(t, d.Active())
	}
}
