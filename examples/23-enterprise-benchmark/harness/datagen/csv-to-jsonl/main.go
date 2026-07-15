// Command csv-to-jsonl converts the orders CSV to newline-delimited JSON
// (one JSON object per line, keyed by the CSV header). Values stay as strings —
// the jsonlines source coerces them per the pipeline column types, exactly as
// the CSV source does.
//
// Usage: csv-to-jsonl <input.csv> <output.jsonl>
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: csv-to-jsonl <input.csv> <output.jsonl>")
		os.Exit(1)
	}
	fi, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	fo, err := os.Create(os.Args[2])
	if err != nil {
		panic(err)
	}
	defer fo.Close()

	sc := bufio.NewScanner(fi)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	w := bufio.NewWriterSize(fo, 1<<20)
	defer w.Flush()

	var header []string
	n := 0
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if header == nil {
			header = fields
			continue
		}
		w.WriteByte('{')
		for i, h := range header {
			if i >= len(fields) {
				break
			}
			if i > 0 {
				w.WriteByte(',')
			}
			w.WriteByte('"')
			w.WriteString(h)
			w.WriteString("\":\"")
			w.WriteString(jsonEscape(fields[i]))
			w.WriteByte('"')
		}
		w.WriteString("}\n")
		n++
	}
	if err := sc.Err(); err != nil {
		panic(err)
	}
	fmt.Printf("converted %d rows: %s -> %s\n", n, os.Args[1], os.Args[2])
}

// jsonEscape escapes the few characters that can appear in these fields.
func jsonEscape(s string) string {
	if !strings.ContainsAny(s, "\"\\") {
		return s
	}
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "\"", "\\\"")
}
