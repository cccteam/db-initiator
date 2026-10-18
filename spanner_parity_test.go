package dbinitiator

import (
	"context"
	"strings"
	"testing"
)

// TestSpannerParity proves the parity check over the emulator: a sequence and its fold
// tidied by hand (an index created and dropped in the history is gone) come out identical,
// data included, with the commit-timestamp column and the version tables left out of the
// comparison; a fold that lost an index is reported by the statement; a seed that differs is
// reported by table, row and column.
func TestSpannerParity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("NewSpannerContainer() error = %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	const base = "file://testdata/spanner/parity/"
	tests := []struct {
		name          string
		a, b          ParitySource
		wantIdentical bool
		wantContains  []string
	}{
		{
			name:          "the sequence and its tidied fold are identical, data included",
			a:             ParitySource{Schema: base + "sequence", Data: base + "data"},
			b:             ParitySource{Schema: base + "folded", Data: base + "data"},
			wantIdentical: true,
		},
		{
			name:         "a fold that lost an index is reported by the statement",
			a:            ParitySource{Schema: base + "sequence"},
			b:            ParitySource{Schema: base + "folded_missing_index"},
			wantContains: []string{"schema: only the first side has: ", "CustomersByNameAndId"},
		},
		{
			name:         "a seed that differs is reported by table, row and column",
			a:            ParitySource{Schema: base + "sequence", Data: base + "data"},
			b:            ParitySource{Schema: base + "folded", Data: base + "data_other"},
			wantContains: []string{`table Customers, row ("c2"), column Name: "Grace" and "Grace Hopper"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			report, err := container.Parity(ctx, tt.a, tt.b)
			if err != nil {
				t.Fatalf("SpannerContainer.Parity() error = %v", err)
			}
			if report.Identical() != tt.wantIdentical {
				t.Fatalf("Identical() = %v, want %v; report:\n%s", report.Identical(), tt.wantIdentical, report)
			}
			if tt.wantIdentical && report.String() != "identical" {
				t.Errorf("String() = %q, want identical", report.String())
			}
			for _, want := range tt.wantContains {
				if !strings.Contains(report.String(), want) {
					t.Errorf("report lacks %q:\n%s", want, report)
				}
			}
		})
	}
}

// TestSpannerParity_Refusals proves a source that names no schema migrations is refused
// before any database is made.
func TestSpannerParity_Refusals(t *testing.T) {
	t.Parallel()

	if _, err := SpannerParity(t.Context(), "p", "i", ParitySource{}, ParitySource{Schema: "file://x"}); err == nil || !strings.Contains(err.Error(), "names no schema migrations") {
		t.Fatalf("SpannerParity() with an empty first source error = %v, want the refusal", err)
	}
}
