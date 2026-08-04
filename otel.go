package nxsugar

import (
	"context"
	"time"

	"github.com/jaracil/ei"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/nayarsystems/nxgo/nxotel"
)

var rpcOtel = nxotel.New("github.com/nayarsystems/nxsugar-go", nxotel.Server, serverErrorType)

// extractTraceparent reads params["@metadata"]["traceparent"] and returns a
// context that carries the remote parent span, enabling end-to-end trace
// correlation from the task pusher to this worker.
//
// If no traceparent is present the returned context is context.Background().
func extractTraceparent(params interface{}) context.Context {
	tp := ei.N(params).M("@metadata").M("traceparent").StringZ()
	if tp == "" {
		return context.Background()
	}
	carrier := propagation.MapCarrier{"traceparent": tp}
	return otel.GetTextMapPropagator().Extract(context.Background(), carrier)
}

// startServerSpan starts an OTel server span for an inbound Nexus task.
// ctx should carry the remote parent (obtained from extractTraceparent).
func startServerSpan(ctx context.Context, method, path string) (context.Context, trace.Span) {
	return rpcOtel.StartSpan(ctx, method, path)
}

// endServerSpan sets the span status based on err and ends it.
func endServerSpan(span trace.Span, err error) {
	rpcOtel.EndSpan(span, err)
}

// recordServerRPCCall records duration and request count for one task execution.
func recordServerRPCCall(ctx context.Context, start time.Time, method, path string, err error) {
	rpcOtel.RecordCall(ctx, start, method, path, err)
}

// serverErrorType returns a short string describing the error for use as the
// error.type metric attribute.
func serverErrorType(err error) string {
	return nxotel.ErrorTypeFromCode(err, ErrStr, "unknown")
}
