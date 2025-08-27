package benchmarks

import (
	"testing"
	"time"
)

// OrderData represents order data for benchmarking
type OrderData struct {
	ID        int64
	UserID    int64
	Amount    float64
	Status    string
	CreatedAt time.Time
}

// generateOrdersData generates test order data
func generateOrdersData(count int) []OrderData {
	data := make([]OrderData, count)
	for i := 0; i < count; i++ {
		data[i] = OrderData{
			ID:        int64(i + 1),
			UserID:    int64((i % 100) + 1),
			Amount:    float64((i % 1000) + 1),
			Status:    []string{"active", "completed", "cancelled"}[i%3],
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Hour),
		}
	}
	return data
}

// BenchmarkQueryPerformance benchmarks basic query performance
func BenchmarkQueryPerformance(b *testing.B) {
	// Generate test data
	testData := generateOrdersData(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate basic query operations
		totalAmount := 0.0
		activeOrders := 0

		for _, order := range testData {
			totalAmount += order.Amount
			if order.Status == "active" {
				activeOrders++
			}
		}

		// Use the results to prevent optimization
		_ = totalAmount
		_ = activeOrders
	}
}

// BenchmarkConcurrentQueries benchmarks concurrent query performance
func BenchmarkConcurrentQueries(b *testing.B) {
	// Generate test data
	testData := generateOrdersData(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate concurrent processing
		results := make(chan float64, 4)

		// Process data in parallel
		for j := 0; j < 4; j++ {
			go func(workerID int) {
				total := 0.0
				start := workerID * (len(testData) / 4)
				end := (workerID + 1) * (len(testData) / 4)
				if workerID == 3 {
					end = len(testData)
				}

				for k := start; k < end; k++ {
					total += testData[k].Amount
				}
				results <- total
			}(j)
		}

		// Collect results
		total := 0.0
		for j := 0; j < 4; j++ {
			total += <-results
		}

		// Use the result to prevent optimization
		_ = total
	}
}

// BenchmarkDataScanning benchmarks data scanning performance
func BenchmarkDataScanning(b *testing.B) {
	// Generate test data
	testData := generateOrdersData(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate data scanning operations
		totalAmount := 0.0
		activeOrders := 0
		highValueOrders := 0

		for _, order := range testData {
			totalAmount += order.Amount
			if order.Status == "active" {
				activeOrders++
			}
			if order.Amount > 1000.0 {
				highValueOrders++
			}
		}

		// Use the results to prevent optimization
		_ = totalAmount
		_ = activeOrders
		_ = highValueOrders
	}
}