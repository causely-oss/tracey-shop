package domain

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
)

// USD builds a dollar-denominated Money from cents.
func USD(cents int64) Money { return Money{Cents: cents, Currency: "USD"} }

// Proto converts to the wire type.
func (m Money) Proto() *shopv1.Money {
	currency := m.Currency
	if currency == "" {
		currency = "USD"
	}
	return &shopv1.Money{Cents: m.Cents, Currency: currency}
}

// MoneyFromProto converts from the wire type, tolerating nil.
func MoneyFromProto(m *shopv1.Money) Money {
	if m == nil {
		return USD(0)
	}
	currency := m.GetCurrency()
	if currency == "" {
		currency = "USD"
	}
	return Money{Cents: m.GetCents(), Currency: currency}
}

// ProductFromProto converts a catalogue entry from the wire type.
func ProductFromProto(p *shopv1.Product) Product {
	if p == nil {
		return Product{}
	}
	return Product{
		ID:        p.GetId(),
		SKU:       p.GetSku(),
		Name:      p.GetName(),
		Category:  p.GetCategory(),
		Price:     MoneyFromProto(p.GetPrice()),
		Available: p.GetAvailable(),
	}
}

// ProductsFromProto converts a slice of catalogue entries.
func ProductsFromProto(in []*shopv1.Product) []Product {
	out := make([]Product, 0, len(in))
	for _, p := range in {
		out = append(out, ProductFromProto(p))
	}
	return out
}

// LineItemsToProto converts cart items to the wire type.
func LineItemsToProto(items []CartItem) []*shopv1.LineItem {
	out := make([]*shopv1.LineItem, 0, len(items))
	for _, it := range items {
		out = append(out, &shopv1.LineItem{ProductId: it.ProductID, Quantity: it.Quantity})
	}
	return out
}

// LineItemsFromProto converts wire line items to cart items.
func LineItemsFromProto(items []*shopv1.LineItem) []CartItem {
	out := make([]CartItem, 0, len(items))
	for _, it := range items {
		out = append(out, CartItem{ProductID: it.GetProductId(), Quantity: it.GetQuantity()})
	}
	return out
}

// NewID returns a short random identifier with the given prefix.
func NewID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail in practice; fall back to a fixed marker
		// rather than panicking inside a request path.
		return prefix + "-0000000000000000"
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b[:]))
}
