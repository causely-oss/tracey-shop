package checkout

import (
	"testing"

	"google.golang.org/grpc/codes"
)

// otelgrpcErrorCodes is the exact set otelgrpc's serverStatus maps to an Error
// span status, per the OTel gRPC semantic conventions. Every other code yields
// Unset.
//
// Causely decides whether a server span counts against a service's error rate
// from the span status alone — `isError := span.StatusCode() == ERROR` in
// pkg/spananalyzer/span_analyzer.go — so a gRPC code outside this set produces
// no error-rate signal at all, no matter how many requests fail.
var otelgrpcErrorCodes = map[codes.Code]bool{
	codes.Unknown:          true,
	codes.DeadlineExceeded: true,
	codes.Unimplemented:    true,
	codes.Internal:         true,
	codes.Unavailable:      true,
	codes.DataLoss:         true,
}

// TestBackpressureRejectCodeIsVisibleToCausely guards a bug that already
// happened once.
//
// The scenario shed ~21% of orders with ResourceExhausted, storefront returned
// 5xx, the load generator counted the failures — and checkout-api's error rate
// in Causely stayed flat, because ResourceExhausted maps to an Unset span
// status. Its success SLO never went at risk, so the Slow Consumer root cause
// remained Non-Urgent, which is the whole thing the backpressure exists to fix.
func TestBackpressureRejectCodeIsVisibleToCausely(t *testing.T) {
	if !otelgrpcErrorCodes[backpressureRejectCode] {
		t.Errorf("backpressureRejectCode is %v, which otelgrpc maps to an Unset span status; "+
			"Causely would not count shed orders as errors and the root cause would stay Non-Urgent. "+
			"Use one of: Unknown, DeadlineExceeded, Unimplemented, Internal, Unavailable, DataLoss",
			backpressureRejectCode)
	}
}

// TestBackpressureRejectCodeIsRetryable is a softer check on intent: shedding is
// a transient, retryable condition, so the code should say so to callers.
func TestBackpressureRejectCodeIsRetryable(t *testing.T) {
	switch backpressureRejectCode {
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
		// Conventionally retryable.
	default:
		t.Errorf("backpressureRejectCode is %v; load shedding should use a retryable code "+
			"so callers back off rather than treating the order as permanently failed",
			backpressureRejectCode)
	}
}
