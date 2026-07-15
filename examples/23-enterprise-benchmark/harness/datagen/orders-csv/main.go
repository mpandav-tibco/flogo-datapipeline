// Command orders-csv generates the deterministic e-commerce-orders benchmark
// dataset as CSV. The generator is seeded (42) so the dataset is reproducible
// byte-for-byte. Deliberately messy: ~25% CANCELLED, ~2.7% zero-price, ~1.9%
// malformed emails — so the validate/filter/dq transforms do real work.
//
// Usage: orders-csv <rows> <output.csv>
package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var (
	regions    = []string{"NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"}
	products   = []string{"Widget", "Gadget", "Gizmo", "Sprocket", "Cog", "Lever", "Piston", "Valve"}
	categories = []string{"Hardware", "Electronics", "Tools", "Industrial", "Consumer"}
	statuses   = []string{"PENDING", "SHIPPED", "DELIVERED"}
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: orders-csv <rows> <output.csv>")
		os.Exit(1)
	}
	rows, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid row count %q: %v\n", os.Args[1], err)
		os.Exit(1)
	}
	f, err := os.Create(os.Args[2])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	w.WriteString("order_id,customer_id,region,product,category,quantity,unit_price,status,order_date,email\n")
	r := rand.New(rand.NewSource(42))
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 1; i <= rows; i++ {
		region := regions[r.Intn(len(regions))]
		product := products[r.Intn(len(products))]
		category := categories[r.Intn(len(categories))]
		qty := r.Intn(1000) + 1

		price := float64(r.Intn(9999999))/100.0 + 0.01
		if r.Float64() < 0.027 { // ~2.7% zero-price (messy)
			price = 0.0
		}

		status := statuses[r.Intn(len(statuses))]
		if r.Float64() < 0.25 { // ~25% CANCELLED
			status = "CANCELLED"
		}

		date := base.AddDate(0, 0, r.Intn(365)).Format("2006-01-02")

		email := "cust" + strconv.Itoa(i) + "@example.com"
		if r.Float64() < 0.019 { // ~1.9% malformed email
			email = "bad-email-" + strconv.Itoa(i)
		}

		w.WriteString("ORD-")
		w.WriteString(strconv.Itoa(i))
		w.WriteString(",CUST-")
		w.WriteString(strconv.Itoa(i))
		w.WriteByte(',')
		w.WriteString(region)
		w.WriteByte(',')
		w.WriteString(product)
		w.WriteByte(',')
		w.WriteString(category)
		w.WriteByte(',')
		w.WriteString(strconv.Itoa(qty))
		w.WriteByte(',')
		w.WriteString(strconv.FormatFloat(price, 'f', 2, 64))
		w.WriteByte(',')
		w.WriteString(status)
		w.WriteByte(',')
		w.WriteString(date)
		w.WriteByte(',')
		w.WriteString(email)
		w.WriteByte('\n')
	}
	fmt.Printf("wrote %d rows to %s\n", rows, os.Args[2])
}
