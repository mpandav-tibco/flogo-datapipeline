package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/fileutil"
	"github.com/mpandav-tibco/flogo-datapipeline/metrics"
	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
)

// Durable spool for the rolling object-store sink (option "spoolDir").
//
// Without a spool, a checkpoint flush uploads the open objects, so the
// object store's availability gates the whole pipeline: while it is down,
// checkpoints fail and the source stops advancing. For a node-local log
// collector that means reading stops during an outage, and logs that kubelet
// rotates away or deletes meanwhile (removed pods) are lost.
//
// With a spool, a checkpoint flush instead SEALS the open objects in spoolDir
// (fsynced data file + atomically written metadata) and succeeds: the rows are
// durable on the node and the source position may advance. A background
// uploader — one per spool directory, shared by every sink using it and
// stopped when the last of them closes (reference-counted) — ships
// sealed objects (data, then manifest) with retries and deletes them once
// uploaded. Objects left over by a previous process (crash, restart) are
// uploaded on startup; unsealed leftovers belonged to un-checkpointed rows,
// which the source replays, and are discarded.
//
// spoolMaxBytes bounds the spool: when sealed objects waiting for upload
// exceed it, Flush fails (the checkpoint does not advance) until the
// uploader catches up — back-pressure instead of filling the disk.

const (
	dataSuffix = ".data"
	metaSuffix = ".meta"
	partSuffix = ".part"
)

// spoolMeta describes a sealed object.
type spoolMeta struct {
	Key      string    `json:"key"`
	Records  int64     `json:"records"`
	Bytes    int64     `json:"bytes"`
	OpenedAt time.Time `json:"openedAt"`
	ClosedAt time.Time `json:"closedAt"`
}

// spoolUploader ships the sealed objects of one spool directory.
type spoolUploader struct {
	dir      string
	provider string
	cfg      map[string]interface{}
	maxBytes int64

	mu       sync.Mutex
	cond     *sync.Cond
	pending  int64 // bytes of sealed objects not yet uploaded
	objects  int
	inflight map[string]bool
	lastErr  error
	failures int64
	uploaded int64
	recorder metrics.Recorder
	wake     chan struct{}

	refs   int // sinks using this uploader (guarded by spoolsMu)
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var (
	spoolsMu sync.Mutex
	spools   = map[string]*spoolUploader{}
)

// spoolFor acquires the uploader for dir, starting it (and recovering
// leftovers) on first use. Every successful call must be paired with release.
func spoolFor(dir, provider string, cfg map[string]interface{}, maxBytes int64) (*spoolUploader, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	spoolsMu.Lock()
	defer spoolsMu.Unlock()
	if u, ok := spools[abs]; ok {
		u.refs++
		return u, nil
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("spool dir: %w", err)
	}
	u := &spoolUploader{dir: abs, provider: provider, cfg: cfg, maxBytes: maxBytes,
		inflight: map[string]bool{}, wake: make(chan struct{}, 1), recorder: metrics.NoopRecorder{}}
	u.cond = sync.NewCond(&u.mu)
	if err := u.recover(); err != nil {
		return nil, err
	}
	u.ctx, u.cancel = context.WithCancel(context.Background())
	u.refs = 1
	spools[abs] = u
	for i := 0; i < spoolUploadWorkers; i++ {
		u.wg.Add(1)
		go u.run()
	}
	return u, nil
}

// acquire adds a user of an already started uploader.
func (u *spoolUploader) acquire() *spoolUploader {
	spoolsMu.Lock()
	u.refs++
	spoolsMu.Unlock()
	return u
}

// release drops a user; the last one stops the workers and waits for them, so
// a later spoolFor for the same directory starts from a consistent state
// (sealed objects not yet uploaded stay on disk and are recovered then).
func (u *spoolUploader) release() {
	spoolsMu.Lock()
	defer spoolsMu.Unlock()
	u.refs--
	if u.refs > 0 {
		return
	}
	if spools[u.dir] == u {
		delete(spools, u.dir)
	}
	u.cancel()
	u.wg.Wait()
}

const spoolUploadWorkers = 4

// recover discards unsealed leftovers and accounts the sealed ones.
func (u *spoolUploader) recover() error {
	entries, err := os.ReadDir(u.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, partSuffix):
			// Rows of an object that was never sealed were not checkpointed:
			// the source replays them.
			_ = os.Remove(filepath.Join(u.dir, name))
		case strings.HasSuffix(name, metaSuffix):
			if fi, err := os.Stat(filepath.Join(u.dir, strings.TrimSuffix(name, metaSuffix)+dataSuffix)); err == nil {
				u.pending += fi.Size()
				u.objects++
			} else {
				_ = os.Remove(filepath.Join(u.dir, name)) // metadata without data
			}
		}
	}
	if u.objects > 0 {
		objLog.Infof("spool %s: %d sealed objects (%d bytes) from a previous run will be uploaded", u.dir, u.objects, u.pending)
	}
	return nil
}

// newPartFile creates the file an open object is written to.
func (u *spoolUploader) newPartFile() (string, error) {
	f, err := os.CreateTemp(u.dir, "obj-*"+partSuffix)
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

// seal makes a completed object durable in the spool and queues it.
func (u *spoolUploader) seal(part string, meta spoolMeta) error {
	f, err := os.OpenFile(part, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	fi, err := f.Stat()
	f.Close()
	if err != nil {
		return err
	}
	base := strings.TrimSuffix(part, partSuffix)
	if err := os.Rename(part, base+dataSuffix); err != nil {
		return err
	}
	b, _ := json.Marshal(meta)
	// The metadata, written atomically and durably (temp + fsync + rename +
	// dir fsync), is what marks the object as sealed.
	if err := fileutil.WriteFileAtomic(base+metaSuffix, b, 0o644); err != nil {
		return err
	}
	u.mu.Lock()
	u.pending += fi.Size()
	u.objects++
	u.mu.Unlock()
	u.report()
	select {
	case u.wake <- struct{}{}:
	default:
	}
	return nil
}

// full reports whether sealed objects awaiting upload exceed the cap.
func (u *spoolUploader) full() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.maxBytes > 0 && u.pending >= u.maxBytes {
		return fmt.Errorf("spool %s full: %d bytes in %d objects await upload (spoolMaxBytes %d); last upload error: %v",
			u.dir, u.pending, u.objects, u.maxBytes, u.lastErr)
	}
	return nil
}

// waitEmpty waits until every sealed object is uploaded or the timeout
// passes (best effort on shutdown: whatever remains is uploaded by the next
// process).
func (u *spoolUploader) waitEmpty(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		u.mu.Lock()
		empty := u.objects == 0
		u.mu.Unlock()
		if empty {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func (u *spoolUploader) setRecorder(r metrics.Recorder) {
	if r == nil {
		return
	}
	u.mu.Lock()
	u.recorder = r
	u.mu.Unlock()
	u.report()
}

func (u *spoolUploader) report() {
	u.mu.Lock()
	r, pending, objects, failures, uploaded := u.recorder, u.pending, u.objects, u.failures, u.uploaded
	u.mu.Unlock()
	labels := map[string]string{"spool": u.dir}
	r.Gauge("flogo_objstore_spool_bytes", "Bytes of sealed objects in the spool awaiting upload.", labels, float64(pending))
	r.Gauge("flogo_objstore_spool_objects", "Sealed objects in the spool awaiting upload.", labels, float64(objects))
	r.Gauge("flogo_objstore_spool_upload_failures", "Cumulative failed upload attempts from the spool.", labels, float64(failures))
	r.Gauge("flogo_objstore_spool_uploaded_objects", "Cumulative objects uploaded from the spool.", labels, float64(uploaded))
}

// next claims the oldest sealed object not being uploaded.
func (u *spoolUploader) next() (string, bool) {
	entries, err := os.ReadDir(u.dir)
	if err != nil {
		return "", false
	}
	var metas []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), metaSuffix) && !strings.HasPrefix(e.Name(), ".") {
			metas = append(metas, e.Name())
		}
	}
	sort.Strings(metas)
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, m := range metas {
		base := filepath.Join(u.dir, strings.TrimSuffix(m, metaSuffix))
		if !u.inflight[base] {
			u.inflight[base] = true
			return base, true
		}
	}
	return "", false
}

func (u *spoolUploader) run() {
	defer u.wg.Done()
	var store objstore.Store
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	backoff := time.Second
	sleep := func(d time.Duration) bool {
		select {
		case <-u.ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}
	for u.ctx.Err() == nil {
		base, ok := u.next()
		if !ok {
			select {
			case <-u.ctx.Done():
			case <-u.wake:
			case <-time.After(2 * time.Second):
			}
			continue
		}
		if store == nil {
			s, err := objstore.New(u.provider, u.cfg)
			if err != nil {
				u.failed(base, fmt.Errorf("open store: %w", err))
				if !sleep(backoff) {
					return
				}
				backoff = minDur(backoff*2, 30*time.Second)
				continue
			}
			store = s
		}
		size, err := u.upload(store, base)
		if errors.Is(err, fs.ErrNotExist) {
			// Claimed from a stale directory listing after another worker had
			// already uploaded and removed it: nothing to do. (A sealed object
			// always has its data before its metadata, so metadata without data
			// is an orphan and is dropped rather than retried forever.)
			_ = os.Remove(base + metaSuffix)
			u.mu.Lock()
			delete(u.inflight, base)
			u.mu.Unlock()
			continue
		}
		if err != nil {
			if u.ctx.Err() != nil { // stopping: the object stays sealed on disk
				u.mu.Lock()
				delete(u.inflight, base)
				u.mu.Unlock()
				return
			}
			u.failed(base, err)
			if !sleep(backoff) {
				return
			}
			backoff = minDur(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		_ = os.Remove(base + metaSuffix)
		_ = os.Remove(base + dataSuffix)
		u.mu.Lock()
		delete(u.inflight, base)
		u.pending -= size
		u.objects--
		u.uploaded++
		u.lastErr = nil
		u.mu.Unlock()
		u.report()
	}
}

func (u *spoolUploader) failed(base string, err error) {
	u.mu.Lock()
	delete(u.inflight, base)
	u.lastErr = err
	u.failures++
	n := u.failures
	u.mu.Unlock()
	if n == 1 || n%10 == 0 {
		objLog.Warnf("spool %s: upload failed (attempt %d; objects stay spooled and are retried): %v", u.dir, n, err)
	}
	u.report()
}

// upload sends one sealed object and its manifest sidecar; both PUTs are
// idempotent (same keys), so a retry after a partial failure is safe.
func (u *spoolUploader) upload(store objstore.Store, base string) (int64, error) {
	mb, err := os.ReadFile(base + metaSuffix)
	if err != nil {
		return 0, err
	}
	var meta spoolMeta
	if err := json.Unmarshal(mb, &meta); err != nil {
		return 0, fmt.Errorf("spool metadata %s: %w", base, err)
	}
	f, err := os.Open(base + dataSuffix)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pw, err := store.PutWriter(u.ctx, meta.Key)
	if err != nil {
		return 0, fmt.Errorf("put %q: %w", meta.Key, err)
	}
	if _, err := io.Copy(pw, f); err != nil {
		_ = pw.Close()
		return 0, fmt.Errorf("upload %q: %w", meta.Key, err)
	}
	if err := pw.Close(); err != nil {
		return 0, fmt.Errorf("commit %q: %w", meta.Key, err)
	}
	if meta.Records > 0 {
		if err := putManifest(u.ctx, store, meta.Key, meta.Records, meta.Bytes, meta.OpenedAt, meta.ClosedAt); err != nil {
			return 0, err
		}
	}
	return fi.Size(), nil
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
