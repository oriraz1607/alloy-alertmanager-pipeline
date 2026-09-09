package alertmanager

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"
)

// HTTPFailureReason identifies a transport or response failure class.
type HTTPFailureReason string

const (
	HTTPFailureConnection  HTTPFailureReason = "connection"
	HTTPFailureTLS         HTTPFailureReason = "tls"
	HTTPFailureTimeout     HTTPFailureReason = "timeout"
	HTTPFailureStatus4xx   HTTPFailureReason = "status_4xx"
	HTTPFailureStatus5xx   HTTPFailureReason = "status_5xx"
	HTTPFailureStatusOther HTTPFailureReason = "status_other"
)

// HTTPFailure describes a failed outbound JSON request.
type HTTPFailure struct {
	Reason     HTTPFailureReason
	StatusCode int
	Err        error
}

func (e *HTTPFailure) Error() string { return e.Err.Error() }
func (e *HTTPFailure) Unwrap() error { return e.Err }

// HTTPDebug carries the native publisher and the current zero-based retry.
// OmitResponseBody prevents credential reflection by authenticated endpoints.
type HTTPDebug struct {
	Publisher         DebugPublisher
	Retry             int
	OmitResponseBody  bool
	ConfiguredHeaders []string
}

// PostJSON sends one JSON document and drains a bounded response body.
func PostJSON(ctx context.Context, client *http.Client, endpointURL string, body []byte, headers map[string]string, timeout time.Duration, debug ...HTTPDebug) (time.Duration, error) {
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpointURL, bytes.NewReader(body))
	if err != nil {
		return 0, &HTTPFailure{Reason: HTTPFailureStatusOther, Err: fmt.Errorf("creating request: %w", err)}
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	req.Header.Set("Content-Type", "application/json")

	var inspection HTTPDebug
	if len(debug) > 0 {
		inspection = debug[0]
	}
	inspection.Publisher.Request("[OUT] HTTP REQUEST", req, body, inspection.Retry, inspection.ConfiguredHeaders...)
	started := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(started)
	if err != nil {
		inspection.Publisher.Publish(0, func() string {
			// net/url errors contain the original URL, and arbitrary transport errors
			// may include credentials. Report a safe diagnosis instead of dumping them.
			diagnosis := string(ClassifyRequestError(err)) + " failure"
			var (
				unknownAuthority x509.UnknownAuthorityError
				hostnameError    x509.HostnameError
				certificateError x509.CertificateInvalidError
				dnsError         *net.DNSError
			)
			switch {
			case errors.As(err, &unknownAuthority):
				diagnosis = "x509: certificate signed by unknown authority"
			case errors.As(err, &hostnameError):
				diagnosis = "x509: certificate is not valid for the destination hostname"
			case errors.As(err, &certificateError):
				diagnosis = "x509: certificate verification failed"
			case errors.Is(err, context.Canceled):
				diagnosis = "request canceled"
			case errors.Is(err, syscall.ECONNREFUSED):
				diagnosis = "connection refused"
			case errors.As(err, &dnsError):
				diagnosis = "DNS lookup failed"
			}
			return fmt.Sprintf("HTTP TRANSPORT ERROR\n%s %s\nRetry attempt: %d\nError: %s", req.Method, safeDebugURL(endpointURL), inspection.Retry, diagnosis)
		})
		return duration, &HTTPFailure{Reason: ClassifyRequestError(err), Err: err}
	}
	defer resp.Body.Close()
	if inspection.Publisher.Active() {
		// Keep exactly the existing bounded drain, including its treatment of read
		// errors. Debugging must not change response or retry semantics.
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		inspection.Publisher.Publish(0, func() string {
			shown := string(responseBody)
			if inspection.OmitResponseBody || req.URL.User != nil || req.URL.RawQuery != "" || len(headers) > 0 {
				shown = "[body omitted: endpoint may reflect credentials]"
			}
			suffix := ""
			if len(responseBody) == 4096 {
				suffix = "\n[body limited to 4096 bytes; may be truncated]"
			}
			if readErr != nil {
				suffix += "\n[response body read incomplete]"
			}
			return fmt.Sprintf("HTTP RESPONSE\n%s %s\nRetry attempt: %d\nStatus: %d %s\nHeaders: %s\nDuration: %s\nBody:\n%s%s", req.Method, safeDebugURL(endpointURL), inspection.Retry, resp.StatusCode, http.StatusText(resp.StatusCode), debugHeaders(resp.Header), duration, shown, suffix)
		})
	} else {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return duration, &HTTPFailure{
			Reason:     ClassifyStatusCode(resp.StatusCode),
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("destination returned HTTP status %d", resp.StatusCode),
		}
	}
	return duration, nil
}

// IsRetryableHTTP reports whether a request failure is transient.
func IsRetryableHTTP(failure *HTTPFailure, retryOnHTTP429 bool) bool {
	switch failure.Reason {
	case HTTPFailureConnection, HTTPFailureTimeout, HTTPFailureStatus5xx:
		return true
	case HTTPFailureStatus4xx:
		return retryOnHTTP429 && failure.StatusCode == http.StatusTooManyRequests
	default:
		return false
	}
}

// HTTPRetryDelay returns capped exponential backoff for a zero-based retry.
func HTTPRetryDelay(minBackoff, maxBackoff time.Duration, retry int) time.Duration {
	delay := minBackoff
	for range retry {
		if delay >= maxBackoff/2 {
			return maxBackoff
		}
		delay *= 2
	}
	return min(delay, maxBackoff)
}

// ClassifyRequestError identifies network, TLS, and timeout failures.
func ClassifyRequestError(err error) HTTPFailureReason {
	if errors.Is(err, context.DeadlineExceeded) {
		return HTTPFailureTimeout
	}
	var timeoutError interface{ Timeout() bool }
	if errors.As(err, &timeoutError) && timeoutError.Timeout() {
		return HTTPFailureTimeout
	}

	var (
		unknownAuthority x509.UnknownAuthorityError
		hostnameError    x509.HostnameError
		certificateError x509.CertificateInvalidError
		verificationErr  *tls.CertificateVerificationError
		recordHeaderErr  tls.RecordHeaderError
	)
	if errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostnameError) ||
		errors.As(err, &certificateError) ||
		errors.As(err, &verificationErr) ||
		errors.As(err, &recordHeaderErr) {

		return HTTPFailureTLS
	}
	return HTTPFailureConnection
}

// ClassifyStatusCode identifies the response status family.
func ClassifyStatusCode(statusCode int) HTTPFailureReason {
	switch {
	case statusCode >= 400 && statusCode < 500:
		return HTTPFailureStatus4xx
	case statusCode >= 500 && statusCode < 600:
		return HTTPFailureStatus5xx
	default:
		return HTTPFailureStatusOther
	}
}
