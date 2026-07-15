package nxsugar

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jaracil/ei"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	rpcSystem  = "nexus"
	instrScope = "github.com/nayarsystems/nxsugar-go"
)

// serverInstruments holds the OTel metric instruments for inbound Nexus tasks.
// Initialised lazily on first use so otel.SetMeterProvider has already been
// called by the application before the instruments are registered.
type serverInstruments struct {
	duration metric.Float64Histogram // rpc.server.request.duration (ms)
	requests metric.Int64Counter     // rpc.server.requests
}

var (
	srvInstruments     *serverInstruments
	srvInstrumentsOnce sync.Once
)

// getInstruments returns the singleton serverInstruments, initialising them
// against the current global MeterProvider on the first call.
func getInstruments() *serverInstruments {
	srvInstrumentsOnce.Do(func() {
		meter := otel.GetMeterProvider().Meter(instrScope)

		dur, err := meter.Float64Histogram(
			"rpc.server.request.duration",
			metric.WithDescription("Duration of inbound Nexus task executions"),
			metric.WithUnit("ms"),
		)
		if err != nil {
			dur, _ = noop.NewMeterProvider().Meter("").Float64Histogram("")
		}

		req, err := meter.Int64Counter(
			"rpc.server.requests",
			metric.WithDescription("Total inbound Nexus task executions"),
		)
		if err != nil {
			req, _ = noop.NewMeterProvider().Meter("").Int64Counter("")
		}

		srvInstruments = &serverInstruments{duration: dur, requests: req}
	})
	return srvInstruments
}

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
	return otel.Tracer(instrScope).Start(ctx, method,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("rpc.system", rpcSystem),
			attribute.String("rpc.method", method),
			attribute.String("rpc.service", path),
		),
	)
}

// endServerSpan sets the span status based on err and ends it.
func endServerSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

// recordServerRPCCall records duration and request count for one task execution.
func recordServerRPCCall(ctx context.Context, start time.Time, method, path string, err error) {
	instr := getInstruments()
	durMs := float64(time.Since(start).Microseconds()) / 1000.0

	attrs := []attribute.KeyValue{
		attribute.String("rpc.system", rpcSystem),
		attribute.String("rpc.method", method),
		attribute.String("rpc.service", path),
	}
	if err != nil {
		attrs = append(attrs, attribute.String("error.type", serverErrorType(err)))
	}

	opt := metric.WithAttributes(attrs...)
	instr.duration.Record(ctx, durMs, opt)
	instr.requests.Add(ctx, 1, opt)
}

// serverErrorType returns a short string describing the error for use as the
// error.type metric attribute.
func serverErrorType(err error) string {
	if rpcErr, ok := err.(*JsonRpcErr); ok {
		if s, ok := ErrStr[rpcErr.Cod]; ok {
			return s
		}
		return fmt.Sprintf("rpc_error_%d", rpcErr.Cod)
	}
	return "unknown"
}
