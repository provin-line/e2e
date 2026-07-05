// Package harness runs real provin binaries and infrastructure for e2e
// scenarios: building cmd/standalone from repos/oss, running node processes
// with generated config, an embedded real NATS server, and an allow-all PDP.
//
// The node is strictly black-box: the harness touches it only through config
// files, environment, TCP ports, and stdout.
package harness

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// BuildStandalone compiles cmd/standalone from the cloned oss repo once per
// test binary and returns the executable path.
func BuildStandalone(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		root := repoRoot(t)
		out := filepath.Join(root, ".tmp", "standalone")
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			buildErr = err
			return
		}
		cmd := exec.Command("go", "build", "-o", out, "./cmd/standalone")
		cmd.Dir = filepath.Join(root, "repos", "oss")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if b, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build cmd/standalone: %v\n%s", err, b)
			return
		}
		builtPath = out
	})
	if buildErr != nil {
		t.Fatalf("BuildStandalone: %v", buildErr)
	}
	return builtPath
}

var (
	buildOnce sync.Once
	builtPath string
	buildErr  error
)

// repoRoot walks up from the CWD to the directory containing go.mod (the e2e
// repo root), so tests work regardless of which package directory runs them.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root (go.mod) not found above CWD")
		}
		dir = parent
	}
}

// Node is one running standalone process.
type Node struct {
	Name    string
	Dir     string // working directory (config/ + data/ live here)
	BaseURL string // control-plane base URL (http://127.0.0.1:<port>)

	cmd    *exec.Cmd
	stdout *LogBuffer
	stderr *LogBuffer
}

// StartNode writes cfg to <dir>/config/application.conf and starts the
// standalone binary with dir as its working directory. It waits for /healthz.
func StartNode(t *testing.T, name, bin, dir, listenAddr, cfg string) *Node {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "config"), 0o755); err != nil {
		t.Fatalf("node %s: mkdir config: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config", "application.conf"), []byte(cfg), 0o644); err != nil {
		t.Fatalf("node %s: write config: %v", name, err)
	}

	n := &Node{
		Name:    name,
		Dir:     dir,
		BaseURL: "http://127.0.0.1" + listenAddr,
		stdout:  &LogBuffer{},
		stderr:  &LogBuffer{},
	}
	n.cmd = exec.Command(bin)
	n.cmd.Dir = dir
	n.cmd.Stdout = n.stdout
	n.cmd.Stderr = n.stderr
	if err := n.cmd.Start(); err != nil {
		t.Fatalf("node %s: start: %v", name, err)
	}
	t.Cleanup(func() { n.Stop(t) })

	waitHealthy(t, name, n.BaseURL+"/healthz", n)
	return n
}

// Stop terminates the node process (idempotent).
func (n *Node) Stop(t *testing.T) {
	t.Helper()
	if n.cmd.Process == nil {
		return
	}
	_ = n.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _, _ = n.cmd.Process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = n.cmd.Process.Kill()
		<-done
	}
}

// Stdout returns everything the node has written to stdout so far.
func (n *Node) Stdout() string { return n.stdout.String() }

// Stderr returns everything the node has written to stderr so far.
func (n *Node) Stderr() string { return n.stderr.String() }

// SinkLines returns stdout lines that look like NDJSON sink records.
func (n *Node) SinkLines() []string {
	var out []string
	sc := bufio.NewScanner(strings.NewReader(n.Stdout()))
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "{") {
			out = append(out, line)
		}
	}
	return out
}

// waitHealthy polls url until 200 or a 30s deadline; on failure it dumps the
// node's stderr so a boot error is visible in the test log.
func waitHealthy(t *testing.T, name, url string, n *Node) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("node %s: never became healthy at %s\n--- stderr ---\n%s\n--- stdout ---\n%s",
		name, url, n.Stderr(), n.Stdout())
}

// LogBuffer is a concurrency-safe accumulating writer.
type LogBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *LogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// WaitFor polls fn until it returns true or the deadline passes.
func WaitFor(t *testing.T, what string, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (%s)", what, timeout)
}

// StartPDPStub serves an allow-all o3co policy-verifier on addr for the
// lifetime of the test (the process-runtime twin of cmd/pdpstub).
func StartPDPStub(t *testing.T, addr string) (baseURL string) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /verify", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	waitURL := "http://127.0.0.1" + addr + "/healthz"
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(waitURL); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return "http://127.0.0.1" + addr
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("pdp stub never became healthy on %s", addr)
	return ""
}
