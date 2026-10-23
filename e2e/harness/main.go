// Command harness provides the data generators and mock services used by the
// runtime end-to-end scenarios in e2e/ (see e2e/README.md). It is test
// tooling, not part of the engine.
//
//	harness gen     -rows N -out FILE [-format csv|tsv|jsonl] [-customers K]
//	harness produce -broker HOST:PORT -topic T -n N [-bad M] [-start S]
//	harness mock    -addr :18080 [-latency 5ms] [-missing-every 10] [-throttle-every 7]
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: harness gen|produce|mock ...")
	}
	switch os.Args[1] {
	case "gen":
		gen(os.Args[2:])
	case "produce":
		produce(os.Args[2:])
	case "mock":
		mock(os.Args[2:])
	default:
		log.Fatalf("unknown subcommand %q", os.Args[1])
	}
}

var regions = []string{"EU", "US", "APAC"}
var statuses = []string{"OPEN", "PAID", "VOID"}

// gen writes deterministic order rows. Row i: id=i, customer_id=i%customers,
// amount=i%1000, status/region cycling, plus a free-text note.
func gen(args []string) {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	rows := fs.Int("rows", 1000, "rows")
	out := fs.String("out", "", "output file")
	format := fs.String("format", "csv", "csv|tsv|jsonl")
	customers := fs.Int("customers", 500, "distinct customer ids")
	start := fs.Int("start", 0, "first id")
	_ = fs.Parse(args)
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()
	sep := ","
	if *format == "tsv" {
		sep = "\t"
	}
	if *format != "jsonl" {
		fmt.Fprintln(w, strings.Join([]string{"id", "customer_id", "amount", "status", "region", "note"}, sep))
	}
	for k := 0; k < *rows; k++ {
		i := *start + k
		cust, amt, st, rg := i%*customers, i%1000, statuses[i%3], regions[i%3]
		note := fmt.Sprintf("order %d for customer %d", i, cust)
		if *format == "jsonl" {
			b, _ := json.Marshal(map[string]interface{}{"id": i, "customer_id": cust, "amount": amt, "status": st, "region": rg, "note": note})
			w.Write(b)
			w.WriteByte('\n')
			continue
		}
		fmt.Fprintf(w, "%d%s%d%s%d%s%s%s%s%s%s\n", i, sep, cust, sep, amt, sep, st, sep, rg, sep, note)
	}
}

// produce sends n JSON sensor messages (ids start..start+n-1) and, when b  ad>0,
// interleaves bad-1 undecodable messages evenly plus one as the final message.
func produce(args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	broker := fs.String("broker", "localhost:9092", "broker")
	topic := fs.String("topic", "", "topic")
	n := fs.Int("n", 1000, "valid messages")
	bad := fs.Int("bad", 0, "undecodable messages")
	start := fs.Int("start", 0, "first id")
	_ = fs.Parse(args)
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Flush.Messages = 500
	p, err := sarama.NewAsyncProducer([]string{*broker}, cfg)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	var errs int64
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range p.Successes() {
		}
	}()
	go func() {
		defer wg.Done()
		for e := range p.Errors() {
			atomic.AddInt64(&errs, 1)
			log.Printf("produce error: %v", e)
		}
	}()
	every := 0
	if *bad > 1 {
		every = *n / (*bad - 1)
	}
	sent, badSent := 0, 0
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < *n; i++ {
		id := *start + i
		v, _ := json.Marshal(map[string]interface{}{
			"id": id, "sensor_id": fmt.Sprintf("s-%d", id%50),
			"temperature": float64(id%120) - 10, "ts": base.Add(time.Duration(id) * time.Second).Format(time.RFC3339),
		})
		p.Input() <- &sarama.ProducerMessage{Topic: *topic, Key: sarama.StringEncoder(strconv.Itoa(id)), Value: sarama.ByteEncoder(v)}
		sent++
		if every > 0 && i%every == every-1 && badSent < *bad-1 {
			p.Input() <- &sarama.ProducerMessage{Topic: *topic, Value: sarama.StringEncoder("{not json")}
			badSent++
		}
	}
	if *bad > 0 {
		p.Input() <- &sarama.ProducerMessage{Topic: *topic, Value: sarama.StringEncoder("trailing-garbage")}
		badSent++
	}
	p.AsyncClose()
	wg.Wait()
	fmt.Printf("produced valid=%d bad=%d errors=%d\n", sent, badSent, errs)
}

// mock serves:
//
//	GET  /customers/{id}  enrichment: 404 for ids divisible by missing-every, else {name,tier}
//	POST /ingest          REST sink target: 429 + Retry-After on every throttle-every'th call
//	GET  /stats           JSON hit counters
//	POST /reset           zero the counters
func mock(args []string) {
	fs := flag.NewFlagSet("mock", flag.ExitOnError)
	addr := fs.String("addr", ":18080", "listen address")
	latency := fs.Duration("latency", 5*time.Millisecond, "per-request latency")
	missingEvery := fs.Int("missing-every", 10, "ids divisible by this are 404")
	throttleEvery := fs.Int64("throttle-every", 7, "every Nth ingest call returns 429")
	_ = fs.Parse(args)

	var mu sync.Mutex
	perKey := map[string]int{}
	ingested := map[string]int{}
	var lookups, lookups404, ingests, ingest429, ingestRows int64

	http.HandleFunc("/customers/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(*latency)
		id := strings.TrimPrefix(r.URL.Path, "/customers/")
		atomic.AddInt64(&lookups, 1)
		mu.Lock()
		perKey[id]++
		mu.Unlock()
		n, err := strconv.Atoi(id)
		if err != nil || n%*missingEvery == 0 {
			atomic.AddInt64(&lookups404, 1)
			http.NotFound(w, r)
			return
		}
		tier := []string{"gold", "silver", "bronze"}[n%3]
		json.NewEncoder(w).Encode(map[string]interface{}{"name": fmt.Sprintf("Customer %d", n), "tier": tier})
	})
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		c := atomic.AddInt64(&ingests, 1)
		if *throttleEvery > 0 && c%*throttleEvery == 0 {
			atomic.AddInt64(&ingest429, 1)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		var raw json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var rows []map[string]interface{}
		if len(raw) > 0 && raw[0] == '[' {
			_ = json.Unmarshal(raw, &rows)
		} else {
			var one map[string]interface{}
			if json.Unmarshal(raw, &one) == nil {
				rows = append(rows, one)
			}
		}
		mu.Lock()
		for _, row := range rows {
			ingested[fmt.Sprint(row["id"])]++
		}
		mu.Unlock()
		atomic.AddInt64(&ingestRows, int64(len(rows)))
		w.WriteHeader(http.StatusCreated)
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		maxPerKey, dupIngest := 0, 0
		for _, v := range perKey {
			if v > maxPerKey {
				maxPerKey = v
			}
		}
		for _, v := range ingested {
			if v > 1 {
				dupIngest++
			}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"lookups": atomic.LoadInt64(&lookups), "lookups404": atomic.LoadInt64(&lookups404),
			"distinctKeys": len(perKey), "maxCallsPerKey": maxPerKey,
			"ingestCalls": atomic.LoadInt64(&ingests), "ingest429": atomic.LoadInt64(&ingest429),
			"ingestRows": atomic.LoadInt64(&ingestRows), "distinctIngested": len(ingested), "duplicateIngested": dupIngest,
		})
	})
	http.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		perKey, ingested = map[string]int{}, map[string]int{}
		mu.Unlock()
		for _, p := range []*int64{&lookups, &lookups404, &ingests, &ingest429, &ingestRows} {
			atomic.StoreInt64(p, 0)
		}
	})
	log.Printf("mock listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
