package ledger

import "testing"

// TestSmallSettlementIsASingleLine covers the common case: a consumer order is
// far below the downstream system's line cap, so it must not be split at all.
func TestSmallSettlementIsASingleLine(t *testing.T) {
	for _, cents := range []int64{1, 1130, 99_616, maxPostingCents} {
		lines := splitPostings(cents)
		if len(lines) != 1 {
			t.Errorf("splitPostings(%d) produced %d lines, want 1", cents, len(lines))
			continue
		}
		if lines[0] != cents {
			t.Errorf("splitPostings(%d) posted %d, want %d", cents, lines[0], cents)
		}
	}
}

// TestNoPostingExceedsTheLineCap is the invariant the downstream accounting
// system enforces: it rejects the whole journal if any single line is over the
// cap, so this must hold for every settlement size.
func TestNoPostingExceedsTheLineCap(t *testing.T) {
	for _, cents := range []int64{1, maxPostingCents, maxPostingCents + 1, 540_160, 13_946_240} {
		for i, line := range splitPostings(cents) {
			if line > maxPostingCents {
				t.Errorf("splitPostings(%d) line %d is %d, over the %d cap",
					cents, i, line, maxPostingCents)
			}
			if line <= 0 {
				t.Errorf("splitPostings(%d) line %d is %d, which is not a postable amount",
					cents, i, line)
			}
		}
	}
}

// TestPostingsSumToSettlement is the invariant RecordTransaction checks
// before opening a transaction: the split lines must sum to exactly the
// authorized amount, or the journal is refused as unbalanced. This guards
// against regressions like the one where the final (partial) line was
// rounded up to maxPostingCents instead of carrying the remainder.
func TestPostingsSumToSettlement(t *testing.T) {
	for _, cents := range []int64{1, maxPostingCents, maxPostingCents + 1, 540_160, 13_946_240, 3 * maxPostingCents} {
		var sum int64
		for _, line := range splitPostings(cents) {
			sum += line
		}
		if sum != cents {
			t.Errorf("splitPostings(%d) lines sum to %d, want %d", cents, sum, cents)
		}
	}
}

// TestLineCapIsAboveConsumerOrderRange pins the cap against the catalogue.
//
// A consumer cart holds at most two lines of two units, and the catalogue tops
// out at 24,904 cents, so the largest consumer settlement is 4 x 24,904 plus
// 8.25% tax plus shipping = 110,225 cents. If the seed prices in
// internal/store ever grow past that, ordinary checkouts would start splitting
// into multiple journal lines, which is not what the cap is for.
func TestLineCapIsAboveConsumerOrderRange(t *testing.T) {
	const largestConsumerSettlementCents = 110_225

	if maxPostingCents <= largestConsumerSettlementCents {
		t.Fatalf("maxPostingCents is %d, at or below the largest consumer settlement (%d); "+
			"every checkout would be split", maxPostingCents, largestConsumerSettlementCents)
	}
}
