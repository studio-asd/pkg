package instrumentation

import (
	"context"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
)

const (
	// List of instrumentation using text map carrier. The text map carrier instrumentation is different
	// from http.Header or gRPC metadata because the text map carrier can carry the instrumentation using
	// map[string]string attributes.
	//
	// While http.Header is also a map[string]string, but we don't want to make things complicated by combining
	// two different things into one.
	requestIDInst         = "inst-request-id"
	apiNameInst           = "inst-bff-api-name"
	apiOwnerInst          = "inst-bff-api-owner"
	forwardedForInst      = "inst-forwarded-for"
	debugIDInst           = "inst-debug-id"
	preferredLanguageInst = "inst-preferred-language"
)

const (
	httpRequestPatternName = "http.request.pattern"
	httpRequestMethodName  = "http.request.method"
	requestIDName          = "request.id"
	apiNameName            = "api.name"
	apiOwnerName           = "api.owner"
	debugIDName            = "debug.id"
	preferredLanguageName  = "language.preferred"
)

// baggageKey is the struct key that is used to store Baggage information inside context.Context.
var baggageKey struct{}

// Baggage is the instrumentation bagge that will be included in context.Context to ensure
// all informations are propagated.
//
// Please NOTE that not all headers/metadata are included in the instrumentation, because we
// only ensure we have something that need to be propageted consistently across multiple services.
type Baggage struct {
	HTTPMethod         string
	HTTPRequestPattern string
	RequestID          string // RequestID is the unique id for every request.
	APIName            string // APIname is the BFF api name for the request.
	APIOwner           string // APIOwner is the BFF api owner for the request.
	DebugID            string // DebugID is a special id to identify a debug request.
	PreferredLanguage  string // PreferredLanguage is a parameter for the langauge usage.
	// valid is a flag to check whether the baggage is a valid baggage that created from the
	// http header, gRPC metadata or something else that supported in the instrumentation package.
	valid bool
	// empty defines whether the baggage is empty or not, so we don't have to compare the struct.
	empty bool
}

// ToSlogAttributes returns the slog attributes from the baggage.
func (b Baggage) ToSlogAttributes() []slog.Attr {
	// If the bagge is not valid, we will return an empty slice so the caller can re-use
	// the slice if needed.
	if !b.valid {
		return []slog.Attr{}
	}

	var attrs []slog.Attr
	if b.HTTPRequestPattern != "" {
		attrs = append(attrs, slog.String(httpRequestPatternName, b.HTTPRequestPattern))
	}
	if b.HTTPMethod != "" {
		attrs = append(attrs, slog.String(httpRequestMethodName, b.HTTPMethod))
	}
	if b.RequestID != "" {
		attrs = append(attrs, slog.String(requestIDName, b.RequestID))
	}
	if b.APIName != "" {
		attrs = append(attrs, slog.String(apiNameName, b.APIName))
	}
	if b.APIOwner != "" {
		attrs = append(attrs, slog.String(apiOwnerName, b.APIOwner))
	}
	if b.DebugID != "" {
		attrs = append(attrs, slog.String(debugIDName, b.DebugID))
	}
	return attrs
}

// ToTextMapCarrier transform the baggage into map[string]string as the carrier.
//
// This function can be used if we were about to propagate informations to a message-bus
// platform because these platforms usually supports attributes in the form of map[string]string.
func (b Baggage) ToTextMapCarrier() map[string]string {
	// Please adjust the length of the text map carrier based on the baggage KV.
	carrier := make(map[string]string, 6)
	carrier[requestIDInst] = b.RequestID
	carrier[apiNameInst] = b.APIName
	carrier[apiOwnerInst] = b.APIOwner
	carrier[debugIDInst] = b.DebugID
	carrier[preferredLanguageInst] = b.PreferredLanguage
	return carrier
}

// ToHTTPHeader transform the baggage into map[string]string as the carrier.
func (b Baggage) ToHTTPHeader() http.Header {
	// Please adjust the length of the text map carrier based on the baggage KV.
	carrier := make(map[string][]string, 6)
	carrier[requestIDInst] = []string{b.RequestID}
	carrier[apiNameInst] = []string{b.APIName}
	carrier[apiOwnerInst] = []string{b.APIOwner}
	carrier[debugIDInst] = []string{b.DebugID}
	carrier[preferredLanguageInst] = []string{b.PreferredLanguage}
	return carrier
}

// ToOpenTelemetryAttributesMetrics returns open telemetry attributes based on the baggage specifically for metrics.
// We have a special function for metrics because we don't want to blown up the metrics caridnality because we propagated
// the request_id and something else that has high cardinality numbers.
func (b Baggage) ToOpenTelemetryAttributesForMetrics() []attribute.KeyValue {
	if !b.valid || b.Empty() {
		return []attribute.KeyValue{}
	}

	var attrs []attribute.KeyValue
	if b.HTTPRequestPattern != "" {
		attrs = append(attrs, attribute.String(httpRequestPatternName, b.HTTPRequestPattern))
	}
	if b.HTTPMethod != "" {
		attrs = append(attrs, attribute.String(httpRequestMethodName, b.HTTPMethod))
	}
	if b.APIName != "" {
		attrs = append(attrs, attribute.String(apiNameName, b.APIName))
	}
	if b.APIOwner != "" {
		attrs = append(attrs, attribute.String(apiOwnerName, b.APIOwner))
	}
	return attrs
}

// ToOpenTelemetryAttributes returns open telemetry attributes based on the baggage.
func (b Baggage) ToOpenTelemetryAttributes() []attribute.KeyValue {
	if !b.valid || b.Empty() {
		return []attribute.KeyValue{}
	}

	var attrs []attribute.KeyValue
	if b.HTTPRequestPattern != "" {
		attrs = append(attrs, attribute.String(httpRequestPatternName, b.HTTPRequestPattern))
	}
	if b.HTTPMethod != "" {
		attrs = append(attrs, attribute.String(httpRequestMethodName, b.HTTPMethod))
	}
	if b.APIName != "" {
		attrs = append(attrs, attribute.String(apiNameName, b.APIName))
	}
	if b.APIOwner != "" {
		attrs = append(attrs, attribute.String(apiOwnerName, b.APIOwner))
	}
	if b.DebugID != "" {
		attrs = append(attrs, attribute.String("api.debug_id", b.DebugID))
	}
	return attrs
}

func (b Baggage) Empty() bool {
	return b.empty
}

// BaggageFromTextMapCarrier creates a new baggage from text map carrier. This kind of format is used to easily inject them to
// another protocol like HTTP(header) and gRPC(metadata). This is why several observability provider also use this kind of format.
func BaggageFromTextMapCarrier(carrier map[string]string) Baggage {
	baggage := Baggage{
		RequestID:         carrier[requestIDInst],
		APIName:           carrier[apiNameInst],
		APIOwner:          carrier[apiOwnerInst],
		DebugID:           carrier[debugIDInst],
		PreferredLanguage: carrier[preferredLanguageInst],
		valid:             true,
	}
	return baggage
}

// BaggageFromContext returns the insturmented bagage from a context.Context.
func BaggageFromContext(ctx context.Context) Baggage {
	bg := baggage.FromContext(ctx)
	if bg.Len() == 0 {
		return Baggage{
			empty: true,
		}
	}

	return Baggage{
		RequestID:         bg.Member(requestIDName).Value(),
		APIName:           bg.Member(apiNameName).Value(),
		APIOwner:          bg.Member(apiOwnerName).Value(),
		DebugID:           bg.Member(debugIDName).Value(),
		PreferredLanguage: bg.Member(preferredLanguageName).Value(),
		valid:             true,
		empty:             false,
	}
}

// ContextWithBaggage set the context key using the passed baggage value.
func ContextWithBaggage(ctx context.Context, bg Baggage) context.Context {
	var members []baggage.Member
	if bg.RequestID != "" {
		ridMem, _ := baggage.NewMember(requestIDName, bg.RequestID)
		members = append(members, ridMem)
	}
	if bg.APIName != "" {
		apiNameMem, _ := baggage.NewMember(apiNameName, bg.APIName)
		members = append(members, apiNameMem)
	}
	if bg.APIOwner != "" {
		apiOwnerMem, _ := baggage.NewMember(apiOwnerName, bg.APIOwner)
		members = append(members, apiOwnerMem)
	}
	if bg.DebugID != "" {
		debugIDMem, _ := baggage.NewMember(debugIDName, bg.DebugID)
		members = append(members, debugIDMem)
	}
	if bg.PreferredLanguage != "" {
		preferredLangMem, _ := baggage.NewMember(preferredLanguageName, bg.PreferredLanguage)
		members = append(members, preferredLangMem)
	}
	otelBg, _ := baggage.New(members...)
	newCtx := baggage.ContextWithBaggage(ctx, otelBg)
	return newCtx
}
