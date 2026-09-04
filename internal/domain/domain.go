// Package domain holds the types exchanged over the demo's HTTP hops.
// gRPC hops use the generated protobuf types in gen/shop/v1 instead.
package domain

import "time"

// Money mirrors shopv1.Money for JSON payloads.
type Money struct {
	Cents    int64  `json:"cents"`
	Currency string `json:"currency"`
}

// Product is the storefront's view of a catalogue item.
type Product struct {
	ID        string `json:"id"`
	SKU       string `json:"sku"`
	Name      string `json:"name"`
	Category  string `json:"category"`
	Price     Money  `json:"price"`
	Available int32  `json:"available"`
}

// CartItem is one line in a shopping cart.
type CartItem struct {
	ProductID string `json:"productId"`
	Quantity  int32  `json:"quantity"`
}

// Cart is the cart-service representation, cached in Valkey.
type Cart struct {
	ID        string     `json:"id"`
	Items     []CartItem `json:"items"`
	UpdatedAt time.Time  `json:"updatedAt"`
}

// AddToCartRequest is the cart-service mutation payload.
type AddToCartRequest struct {
	ProductID string `json:"productId"`
	Quantity  int32  `json:"quantity"`
}

// Address is a shipping destination.
type Address struct {
	Street     string `json:"street"`
	City       string `json:"city"`
	Region     string `json:"region"`
	PostalCode string `json:"postalCode"`
	Country    string `json:"country"`
}

// ShippingQuoteRequest asks shipping-quote for a carrier rate.
type ShippingQuoteRequest struct {
	OrderID   string   `json:"orderId"`
	Address   Address  `json:"address"`
	ItemCount int32    `json:"itemCount"`
	WeightG   int32    `json:"weightG"`
	Options   []string `json:"options,omitempty"`
}

// ShippingQuoteResponse is a carrier rate plus a tracking identifier.
type ShippingQuoteResponse struct {
	Carrier      string `json:"carrier"`
	Service      string `json:"service"`
	Cost         Money  `json:"cost"`
	EstimatedETA string `json:"estimatedEta"`
	TrackingID   string `json:"trackingId"`
}

// CheckoutRequest is the storefront's checkout payload.
type CheckoutRequest struct {
	CartID       string  `json:"cartId"`
	CustomerID   string  `json:"customerId"`
	CustomerTier string  `json:"customerTier"`
	Email        string  `json:"email"`
	Address      Address `json:"address"`
	CardLastFour string  `json:"cardLastFour"`
	CardBrand    string  `json:"cardBrand"`
}

// CheckoutResponse is the storefront's checkout result.
type CheckoutResponse struct {
	OrderID       string `json:"orderId"`
	Total         Money  `json:"total"`
	ShippingCost  Money  `json:"shippingCost"`
	TransactionID string `json:"transactionId"`
	TrackingID    string `json:"trackingId"`
}

// OrderEvent is published to the "orders" Kafka topic by checkout-api and
// consumed by fraud-detector.
type OrderEvent struct {
	OrderID       string     `json:"orderId"`
	CustomerID    string     `json:"customerId"`
	CustomerTier  string     `json:"customerTier"`
	Email         string     `json:"email"`
	Country       string     `json:"country"`
	Total         Money      `json:"total"`
	Items         []CartItem `json:"items"`
	TransactionID string     `json:"transactionId"`
	PlacedAt      time.Time  `json:"placedAt"`
}

// LedgerEvent is published to "ledger.events" by ledger-svc.
type LedgerEvent struct {
	JournalID     string    `json:"journalId"`
	TransactionID string    `json:"transactionId"`
	OrderID       string    `json:"orderId"`
	Amount        Money     `json:"amount"`
	RecordedAt    time.Time `json:"recordedAt"`
}

// NotificationEvent is published to "notifications" by fraud-detector and
// consumed by notification-worker.
type NotificationEvent struct {
	OrderID   string    `json:"orderId"`
	Email     string    `json:"email"`
	Template  string    `json:"template"`
	Decision  string    `json:"decision"`
	RiskScore float64   `json:"riskScore"`
	Total     Money     `json:"total"`
	CreatedAt time.Time `json:"createdAt"`
}

// PartnerRequest is the generic payload accepted by partner-sim, standing in
// for a third-party payment processor, carrier or email provider.
type PartnerRequest struct {
	Reference string            `json:"reference"`
	AmountC   int64             `json:"amountCents,omitempty"`
	Currency  string            `json:"currency,omitempty"`
	To        string            `json:"to,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// PartnerResponse is partner-sim's acknowledgement.
type PartnerResponse struct {
	Provider  string `json:"provider"`
	Reference string `json:"reference"`
	Status    string `json:"status"`
	Code      string `json:"code"`
}

// AssistRequest is a shopper's question for ai-assistant.
type AssistRequest struct {
	Question  string `json:"question"`
	ProductID string `json:"productId,omitempty"`
	CartID    string `json:"cartId,omitempty"`
}

// AssistResponse is ai-assistant's answer, with the model attribution a
// genAI-aware UI would show. The token counts are the same values that go onto
// the span as gen_ai.usage.*, which makes a browser click enough to sanity-check
// what Causely will receive.
type AssistResponse struct {
	Answer       string `json:"answer"`
	Model        string `json:"model"`
	Provider     string `json:"provider"`
	InputTokens  int64  `json:"inputTokens"`
	OutputTokens int64  `json:"outputTokens"`
}
