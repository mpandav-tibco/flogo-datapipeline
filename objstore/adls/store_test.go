package adls

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
)

// Well-known Azurite development account credentials.
const (
	azuriteAccount = "devstoreaccount1"
	azuriteKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	adlsContainer  = "dp-test-container"
)

// liveADLSStore builds an adlsStore against the Azurite emulator in
// DP_TEST_AZURITE_URL, skipping when unset. It creates the container first (the
// store itself does not create containers).
func liveADLSStore(t *testing.T) objstore.Store {
	t.Helper()
	serviceURL := os.Getenv("DP_TEST_AZURITE_URL")
	if serviceURL == "" {
		t.Skip("set DP_TEST_AZURITE_URL (e.g. http://127.0.0.1:10000/devstoreaccount1) to run the live ADLS store test")
	}
	cred, err := azblob.NewSharedKeyCredential(azuriteAccount, azuriteKey)
	if err != nil {
		t.Fatalf("shared-key credential: %v", err)
	}
	raw, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		t.Fatalf("raw azblob client: %v", err)
	}
	if _, err := raw.CreateContainer(context.Background(), adlsContainer, nil); err != nil {
		if !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
			t.Fatalf("create container: %v", err)
		}
	}

	st, err := newADLSStore(map[string]interface{}{
		"container":   adlsContainer,
		"accountName": azuriteAccount,
		"accountKey":  azuriteKey,
		"serviceURL":  serviceURL,
	})
	if err != nil {
		t.Fatalf("newADLSStore: %v", err)
	}
	return st
}

func containsKey(objs []objstore.Object, key string) bool {
	for _, o := range objs {
		if o.Key == key {
			return true
		}
	}
	return false
}

func TestADLSStore_RoundTrip(t *testing.T) {
	st := liveADLSStore(t)
	defer st.Close()
	ctx := context.Background()
	prefix := fmt.Sprintf("dptest/unit/%d/", time.Now().UnixNano())
	key := prefix + "obj.txt"
	payload := []byte("hello world")

	w, err := st.PutWriter(ctx, key)
	if err != nil {
		t.Fatalf("PutWriter: %v", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("commit upload: %v", err)
	}
	t.Cleanup(func() { _ = st.Delete(ctx, key) })

	rc, err := st.GetReader(ctx, key)
	if err != nil {
		t.Fatalf("GetReader: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("GetReader = %q, want %q", got, payload)
	}

	objs, err := st.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List prefix: %v", err)
	}
	if !containsKey(objs, key) {
		t.Errorf("List(%q) missing %q", prefix, key)
	}

	if rr, ok := st.(objstore.RangeReader); ok {
		r2, err := rr.GetRange(ctx, key, 6, 5) // "world"
		if err != nil {
			t.Fatalf("GetRange: %v", err)
		}
		part, _ := io.ReadAll(r2)
		_ = r2.Close()
		if string(part) != "world" {
			t.Errorf("GetRange = %q, want world", part)
		}
	}

	if err := st.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Delete(ctx, key); err != nil {
		t.Errorf("Delete of missing blob should be a no-op, got %v", err)
	}
}

func TestADLSStore_GetReaderMissing(t *testing.T) {
	st := liveADLSStore(t)
	defer st.Close()
	if _, err := st.GetReader(context.Background(), "dptest/definitely/missing.txt"); err == nil {
		t.Error("GetReader of a missing blob expected an error")
	}
}

// TestNewADLSStoreErrors covers the offline construction error paths.
func TestNewADLSStoreErrors(t *testing.T) {
	if _, err := newADLSStore(map[string]interface{}{}); err == nil {
		t.Error("missing container expected an error")
	}
	if _, err := newADLSStore(map[string]interface{}{"container": "c"}); err == nil {
		t.Error("missing accountName expected an error")
	}
	if _, err := newADLSStore(map[string]interface{}{"container": "c", "accountName": "a"}); err == nil {
		t.Error("no credentials expected an error")
	}
	if _, err := newADLSStore(map[string]interface{}{"container": "c", "accountName": "a", "sasTokenEnv": "NO_SUCH_SAS_ENV_VAR"}); err == nil {
		t.Error("empty sasTokenEnv-referenced credential expected an error")
	}
}
