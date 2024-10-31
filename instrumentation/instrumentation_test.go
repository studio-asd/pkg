package instrumentation

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestContextWithBaggage(t *testing.T) {
	t.Parallel()

	bg := Baggage{
		RequestID:         "req_id",
		APIName:           "api_name",
		APIOwner:          "test",
		DebugID:           "debug_id",
		PreferredLanguage: "id",
		valid:             true,
	}

	ctx := context.Background()
	ctx = ContextWithBaggage(ctx, bg)
	got := BaggageFromContext(ctx)

	if diff := cmp.Diff(bg, got, cmpopts.EquateComparable(Baggage{})); diff != "" {
		t.Fatalf("(-wan/+got)\n%s", diff)
	}
}

func TestBaggageFromTextMapCarrier(t *testing.T) {
	t.Parallel()

	textMap := map[string]string{
		requestIDInst:         "req_id",
		apiNameInst:           "api_name",
		apiOwnerInst:          "test",
		debugIDInst:           "debug_id",
		preferredLanguageInst: "id",
	}
	expect := Baggage{
		RequestID:         "req_id",
		APIName:           "api_name",
		APIOwner:          "test",
		DebugID:           "debug_id",
		PreferredLanguage: "id",
		valid:             true,
	}

	got := BaggageFromTextMapCarrier(textMap)
	if diff := cmp.Diff(expect, got, cmpopts.EquateComparable(Baggage{})); diff != "" {
		t.Fatalf("(-want/+got)\n%s", diff)
	}
}

func TestToSlogAttr(t *testing.T) {
	bg := Baggage{
		RequestID:         "req_id",
		APIName:           "api_name",
		APIOwner:          "test",
		DebugID:           "debug_id",
		PreferredLanguage: "id",
		valid:             true,
	}

	expect := []slog.Attr{
		{
			Key:   "request.id",
			Value: slog.StringValue("req_id"),
		},
		{
			Key:   "api.name",
			Value: slog.StringValue("api_name"),
		},
		{
			Key:   "api.owner",
			Value: slog.StringValue("test"),
		},
		{
			Key:   "debug.id",
			Value: slog.StringValue("debug_id"),
		},
	}

	if diff := cmp.Diff(expect, bg.ToSlogAttributes()); diff != "" {
		t.Fatalf("(-want/+got)\n%s", diff)
	}
}
