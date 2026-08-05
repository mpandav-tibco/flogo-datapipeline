// Command fixture is a tiny local HTTP API used only to exercise the REST
// source example end-to-end without depending on a real external API. It
// serves a page-paginated JSON list of fake orders at GET /orders.
//
//	go run . [-addr :8089]
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"
)

type order struct {
	ID       int     `json:"id"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
	Status   string  `json:"status"`
}

func allOrders() []order {
	customers := []string{"Acme Corp", "Globex", "Initech", "Umbrella", "Soylent"}
	statuses := []string{"open", "shipped", "closed"}
	out := make([]order, 25)
	for i := range out {
		out[i] = order{
			ID:       i + 1,
			Customer: customers[i%len(customers)],
			Amount:   float64(100+i*7) + 0.5,
			Status:   statuses[i%len(statuses)],
		}
	}
	return out
}

func main() {
	addr := flag.String("addr", ":8089", "listen address")
	flag.Parse()

	orders := allOrders()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 1 {
			page = 1
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 10
		}
		start := (page - 1) * limit
		if start >= len(orders) {
			_ = json.NewEncoder(w).Encode([]order{})
			return
		}
		end := start + limit
		if end > len(orders) {
			end = len(orders)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(orders[start:end])
	})

	log.Printf("fixture API serving %d orders on %s (GET /orders?page=N&limit=M)", len(orders), *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
