package loadgen

import (
	"math/rand"
	"testing"
)

// TestWholesaleProductIDsAreDistinct guards the one thing that would quietly
// change what a wholesale order exercises.
//
// cart-service merges repeated lines for the same product, so a duplicate would
// collapse two case quantities into a single line that can exceed the stock on
// hand. The order would then fail the stock check rather than completing, and
// the traffic would no longer be testing what it looks like it is testing.
func TestWholesaleProductIDsAreDistinct(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	for i := 0; i < 2000; i++ {
		ids := distinctProductIDs(rng, wholesaleLines)
		if len(ids) != wholesaleLines {
			t.Fatalf("got %d product ids, want %d", len(ids), wholesaleLines)
		}
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				t.Fatalf("iteration %d: product %s appears twice in %v", i, id, ids)
			}
			seen[id] = true
		}
	}
}

// TestWholesaleQuantitiesAreCaseSized keeps the order size in the range the
// wholesale channel is for. A quantity range that drifted down toward consumer
// sizes would make this traffic indistinguishable from an ordinary checkout.
func TestWholesaleQuantitiesAreCaseSized(t *testing.T) {
	if wholesaleMinQty < 100 {
		t.Errorf("wholesaleMinQty is %d; a wholesale line is a case quantity, not a handful",
			wholesaleMinQty)
	}
	if wholesaleMaxQty < wholesaleMinQty {
		t.Errorf("wholesaleMaxQty (%d) is below wholesaleMinQty (%d)",
			wholesaleMaxQty, wholesaleMinQty)
	}
	// Stock is restocked to 1000 and topped up whenever it falls below the low
	// water mark, so a line must stay well clear of that mark to pass the
	// stock check. See internal/store.StartRestocker.
	if wholesaleMaxQty > 200 {
		t.Errorf("wholesaleMaxQty is %d, which can exceed available stock and fail "+
			"the stock check before the order is priced", wholesaleMaxQty)
	}
}
