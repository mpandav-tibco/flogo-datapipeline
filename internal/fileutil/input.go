package fileutil

import (
	"fmt"
	"io"
	"os"
)

// OpenInput returns the byte stream a file-format source should parse: the
// io.Reader injected under cfg["reader"] when present (used by the object-store
// source to stream an object without a temp file), otherwise the file at
// cfg["path"]. The returned ReadCloser closes the injected reader when it is an
// io.Closer. kind prefixes error messages (e.g. "csv source").
func OpenInput(cfg map[string]interface{}, kind string) (io.ReadCloser, error) {
	if r, ok := cfg["reader"].(io.Reader); ok && r != nil {
		if rc, ok := r.(io.ReadCloser); ok {
			return rc, nil
		}
		return io.NopCloser(r), nil
	}
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("%s: 'path' is required", kind)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%s: open %q: %w", kind, path, err)
	}
	return f, nil
}
