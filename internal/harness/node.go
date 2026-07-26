// Package harness runs real provin binaries and infrastructure for e2e
// scenarios: building cmd/network + cmd/pipeline (the separated topology;
// see separated.go) from repos/oss, running node processes with generated
// config, an embedded real NATS server, and an allow-all PDP.
//
// The node is strictly black-box: the harness touches it only through config
// files, environment, TCP ports, and stdout.
package harness

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// BuildBinaries compiles cmd/network (control plane) and cmd/pipeline (data
// plane) from the cloned oss repo once per test binary and returns both
// executable paths — the separated topology's twin of BuildStandalone, same
// once-per-package sync.Once and output-keying discipline (the output paths
// are keyed by the test binary's name so parallel scenario packages cannot
// clobber each other's in-progress `go build`).
func BuildBinaries(t *testing.T) (networkBin, pipelineBin string) {
	t.Helper()
	buildBinariesOnce.Do(func() {
		root := repoRoot(t)
		ossDir := filepath.Join(root, "repos", "oss")
		suffix := filepath.Base(os.Args[0])
		netOut := filepath.Join(root, ".tmp", "network-"+suffix)
		pipeOut := filepath.Join(root, ".tmp", "pipeline-"+suffix)
		if err := os.MkdirAll(filepath.Dir(netOut), 0o755); err != nil {
			buildBinariesErr = err
			return
		}
		build := func(out, pkg string) error {
			cmd := exec.Command("go", "build", "-o", out, pkg)
			cmd.Dir = ossDir
			cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
			if b, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("go build %s: %v\n%s", pkg, err, b)
			}
			return nil
		}
		if err := build(netOut, "./cmd/network"); err != nil {
			buildBinariesErr = err
			return
		}
		if err := build(pipeOut, "./cmd/pipeline"); err != nil {
			buildBinariesErr = err
			return
		}
		networkBinPath, pipelineBinPath = netOut, pipeOut
	})
	if buildBinariesErr != nil {
		t.Fatalf("BuildBinaries: %v", buildBinariesErr)
	}
	return networkBinPath, pipelineBinPath
}

var (
	buildBinariesOnce sync.Once
	networkBinPath    string
	pipelineBinPath   string
	buildBinariesErr  error
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
	done   chan struct{} // closed when cmd.Wait returns (output flushed)
}

// FreePort reserves an ephemeral TCP port on loopback and returns it as a
// ":<port>" listen address. Scenario packages run in parallel; fixed ports
// collide across test binaries.
func FreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().(*net.TCPAddr)
	_ = l.Close()
	return fmt.Sprintf(":%d", addr.Port)
}

// runNodeProcess execs bin with dir as its working directory and extraEnv
// appended to the environment, wiring stdout/stderr into a Node and
// registering Stop as t.Cleanup. It writes no config and waits for no
// readiness signal — callers do both. This lower-level helper exists for
// StartSeparatedNode, whose two processes take config through different
// mechanisms (application.conf vs CONFIG_FILE) and become ready on different
// criteria (each process's OWN /readyz, not /healthz — see separated.go).
func runNodeProcess(t *testing.T, name, bin, dir, baseURL string, extraEnv []string) *Node {
	t.Helper()
	n := &Node{
		Name:    name,
		Dir:     dir,
		BaseURL: baseURL,
		stdout:  &LogBuffer{},
		stderr:  &LogBuffer{},
	}
	n.cmd = exec.Command(bin)
	n.cmd.Dir = dir
	if len(extraEnv) > 0 {
		n.cmd.Env = append(os.Environ(), extraEnv...)
	}
	n.cmd.Stdout = n.stdout
	n.cmd.Stderr = n.stderr
	if err := n.cmd.Start(); err != nil {
		t.Fatalf("node %s: start: %v", name, err)
	}
	n.done = make(chan struct{})
	go func() { _ = n.cmd.Wait(); close(n.done) }() // cmd.Wait flushes the stdout/stderr copiers
	t.Cleanup(func() { n.Stop(t) })
	return n
}

// Stop terminates the node process (idempotent).
func (n *Node) Stop(t *testing.T) {
	t.Helper()
	if n.cmd.Process == nil {
		return
	}
	select {
	case <-n.done: // already exited
		return
	default:
	}
	_ = n.cmd.Process.Signal(os.Interrupt)
	select {
	case <-n.done:
	case <-time.After(10 * time.Second):
		_ = n.cmd.Process.Kill()
		<-n.done
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

// waitHealthy polls url until 200 or a 30s deadline, failing fast if the node
// process exits first; on failure it dumps the node's stderr so a boot error
// is visible in the test log.
func waitHealthy(t *testing.T, name, url string, n *Node) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	// 90s, not the 30s this used to allow. Boot readiness was the tightest
	// budget in the harness — half of what the compose runtime's own health
	// waits get — and it is the one that breaks first on a loaded or slow host:
	// an otherwise-passing scenario failed here at 60s wall-clock while a large
	// git clone ran alongside it, which is a mild version of what a two-core CI
	// runner looks like all the time.
	//
	// Extending it is close to free. A healthy boot returns as soon as /readyz
	// answers 200, so nothing slows down; and a node that DIES during boot is
	// still reported immediately by the n.done branch below rather than after
	// the deadline. The only case this lengthens is "alive but not ready yet" —
	// exactly the case that deserves more patience.
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-n.done:
			t.Fatalf("node %s: exited during boot\n--- stderr ---\n%s\n--- stdout ---\n%s",
				name, n.Stderr(), n.Stdout())
		default:
		}
		resp, err := client.Get(url)
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
	serveErr := &LogBuffer{}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			_, _ = serveErr.Write([]byte(err.Error()))
		}
	}()
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
	t.Fatalf("pdp stub never became healthy on %s (serve error: %s)", addr, serveErr.String())
	return ""
}

// BuildProvinCLI compiles cmd/provin (the operator / relying-party CLI) from
// the cloned oss repo once per test binary and returns the executable path —
// the same clobber-avoidance keying as BuildStandalone. Scenarios exercising
// product commands (bundle export/verify) run the REAL binary a relying
// party would run, not a library shortcut.
func BuildProvinCLI(t *testing.T) string {
	t.Helper()
	cliOnce.Do(func() {
		root := repoRoot(t)
		out := filepath.Join(root, ".tmp", "provin-"+filepath.Base(os.Args[0]))
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			cliErr = err
			return
		}
		cmd := exec.Command("go", "build", "-o", out, "./cmd/provin")
		cmd.Dir = filepath.Join(root, "repos", "oss")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if b, err := cmd.CombinedOutput(); err != nil {
			cliErr = fmt.Errorf("go build cmd/provin: %v\n%s", err, b)
			return
		}
		cliPath = out
	})
	if cliErr != nil {
		t.Fatalf("BuildProvinCLI: %v", cliErr)
	}
	return cliPath
}

var (
	cliOnce sync.Once
	cliPath string
	cliErr  error
)
