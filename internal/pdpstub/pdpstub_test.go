package pdpstub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/o3co/protobuf.interceptors"
	"github.com/o3co/protobuf.interceptors/endpoint"
)

func post(t *testing.T, srv *httptest.Server, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(srv.URL+"/verify", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /verify: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestVerifyEchoesAnAllowEnvelope(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()

	resp := post(t, srv, `{"resource":"dids","action":"read","context":{"x":"y"}}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := map[string]any{
		"resource": "dids",
		"action":   "read",
		"decision": "allow",
		"reason":   map[string]any{"groups": []any{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %#v, want %#v", got, want)
	}
}

// The real client is the contract: interceptors v0.6.0 takes only a whole
// decision envelope as an allow.
func TestInterceptorsAcceptTheEnvelope(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()

	ep, err := endpoint.NewO3coEndpoint(srv.URL)
	if err != nil {
		t.Fatalf("NewO3coEndpoint: %v", err)
	}
	ctx := interceptors.WithBearerToken(context.Background(), "stub-token")
	if err := ep.Verify(ctx, "dids", "read"); err != nil {
		t.Fatalf("Verify = %v, want an allow", err)
	}
}

func TestVerifyRefusesAMalformedBody(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()

	for _, body := range []string{`not json`, `{"resource":1,"action":"read"}`, `{"resource":"dids"}`} {
		if resp := post(t, srv, body); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, resp.StatusCode)
		}
	}
}

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}
