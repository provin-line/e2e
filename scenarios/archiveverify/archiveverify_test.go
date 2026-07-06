// Scenario archiveverify: the auditor verifies AFTER the infrastructure died.
//
// Story: during live operation, a relying party archives what a later audit
// needs — the credential bytes (content-addressed, proof embedded) and the
// issuers' DID documents (the verification keys, served on the public /did/
// route). Then the node is gone: decommissioned, the vendor folded, the
// evidence subpoenaed years later. The auditor re-verifies the ENTIRE chain
// from the archived bytes alone — no registry, no broker, no provin service.
//
// This pins provin's strongest survivability property: verification does not
// depend on any live infrastructure. Credentials carry their proofs
// (EdDSA-JCS-2022) and chain links (previousCredential content addresses)
// inside the signed body, so bytes + public keys are sufficient forever.
// (The broker stays up — it is irrelevant: the verifier's only outward
// dependency is the resolver, and the offline resolver is a map. The node,
// which serves every surface a verifier COULD reach, is stopped and asserted
// dead.)
//
// It also documents the flip side (finding): the archive format used here —
// "credential JSON + the /did/ document JSON per issuer" — is INVENTED BY
// THIS TEST. The product defines no snapshot/export convention, so every
// relying party must improvise one; until a convention exists, the
// survivability property is real but unclaimable in practice.
//
// Runtimes: process (default) and compose (E2E_RUNTIME=compose).
package archiveverify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/provin-line/oss/crypto/ed25519"
	"github.com/provin-line/oss/did"
	vcpb "github.com/provin-line/oss/gen/go/dplaax/vc/v1"
	"github.com/provin-line/oss/gen/go/dplaax/vc/v1/vcpbconnect"
	natstransport "github.com/provin-line/oss/pipeline/transport/nats"
	"github.com/provin-line/oss/vc"

	"github.com/provin-line/e2e/internal/harness"
)

const (
	registryID = "poc.dplaax.dev"
	ownerDID   = "did:dplaax:poc.dplaax.dev:org:acme"

	srcPipelineDID   = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:src"
	srcProcessDID    = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:src:process:s1"
	relayPipelineDID = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:relay"
	relayProcessDID  = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:relay:process:r1"

	ingressSubject = "ingest.src"
	rawJSON        = `{"reading":42}`
)

func loopsBlock(selfBase string) string {
	return fmt.Sprintf(`
      src {
        role            = "source"
        ingress-subject = %q
        output-subject  = %q
        issuer {
          did                 = %q
          key-id              = "signing"
          verification-method = %q
        }
        pipeline-id          = "src"
        process-id           = "s1"
        transformation-claim = "convert"
      }
      relay {
        role            = "chained"
        ingress-subject = %q
        chained {
          output-subject = %q
          issuer {
            did                 = %q
            key-id              = "signing"
            verification-method = %q
          }
          pipeline-id           = "relay"
          process-id            = "r1"
          transformation-claim  = "convert"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
        }
      }
      archive {
        role            = "sink"
        ingress-subject = %q
        sink {
          kind                  = "observation-only"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
        }
      }`,
		ingressSubject, srcPipelineDID, srcProcessDID, srcProcessDID+"#signing",
		srcPipelineDID, relayPipelineDID, relayProcessDID, relayProcessDID+"#signing", selfBase,
		relayPipelineDID, selfBase)
}

// archive is what the relying party keeps: credential bytes by content
// address, issuer DID documents by DID. THIS FORMAT IS AD HOC — see the
// package comment; the product defines no snapshot convention.
type archive struct {
	credentials map[string][]byte
	didDocs     map[string][]byte
}

// Resolve implements the product's resolver contract over archived documents
// only — the offline auditor's resolver. It can, by construction, reach no
// network.
func (a *archive) Resolve(_ context.Context, didStr string) (*did.DIDDocument, error) {
	raw, ok := a.didDocs[didStr]
	if !ok {
		return nil, fmt.Errorf("archive: no DID document archived for %s", didStr)
	}
	var doc did.DIDDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("archive: parse archived document for %s: %w", didStr, err)
	}
	return &doc, nil
}

// didDocURL converts a dplaax DID into its public W3C resolution route on the
// issuing node: /did/<path segments>/did.json.
func didDocURL(t *testing.T, nodeBase, didStr string) string {
	t.Helper()
	rest, ok := strings.CutPrefix(didStr, "did:dplaax:"+registryID+":")
	if !ok {
		t.Fatalf("DID %q is not under registry %s", didStr, registryID)
	}
	return nodeBase + "/did/" + strings.ReplaceAll(rest, ":", "/") + "/did.json"
}

func TestArchiveVerify_ChainOutlivesInfrastructure(t *testing.T) {
	e := harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:         "acme",
		RegistryID:      registryID,
		NodeDID:         ownerDID,
		Loops:           loopsBlock,
		Tunables:        harness.FastTunables,
		IngressSubjects: []string{ingressSubject, srcPipelineDID, relayPipelineDID},
	})
	ctx := context.Background()

	owner := harness.NewOwner(t, ownerDID)
	harness.Bootstrap(t, e.NodeBase, owner,
		[]string{srcPipelineDID, relayPipelineDID},
		[]string{srcProcessDID, relayProcessDID},
	)

	// --- Live phase: run the story and take the archive. ---
	conn, err := natstransport.Connect(natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Publisher(ingressSubject).Publish([]byte(rawJSON)); err != nil {
		t.Fatalf("publish input: %v", err)
	}

	type sinkRecord struct {
		Credential string `json:"credential"`
		Confidence string `json:"confidence"`
	}
	var head string
	harness.WaitFor(t, "sink record", 60*time.Second, func() bool {
		for _, line := range e.SinkLines() {
			var rec sinkRecord
			if json.Unmarshal([]byte(line), &rec) == nil && rec.Credential != "" {
				head = rec.Credential
				return true
			}
		}
		return false
	})

	arch := &archive{credentials: map[string][]byte{}, didDocs: map[string][]byte{}}

	// Walk the chain over the wire while it is still alive: head (relay), then
	// its predecessor (src FirstDrop), archiving raw bytes by content address.
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.NodeBase)
	fetch := func(hash string) *vc.PipelinePassCredential {
		t.Helper()
		res, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: hash})))
		if err != nil {
			t.Fatalf("ResolveVC(%s): %v", hash, err)
		}
		raw := res.Msg.GetCredential()
		arch.credentials[hash] = raw
		var cred vc.PipelinePassCredential
		if err := json.Unmarshal(raw, &cred); err != nil {
			t.Fatalf("unmarshal credential %s: %v", hash, err)
		}
		return &cred
	}
	headCred := fetch(head)
	if headCred.PreviousCredential() == "" {
		t.Fatal("head credential has no previousCredential — expected the relay hop")
	}
	fetch(headCred.PreviousCredential())

	// Archive the DID documents from the PUBLIC resolution route — the part
	// of the archive anyone can take without credentials. The signing keys
	// alone are NOT enough: signer-authenticity walks the controller chain
	// (process → pipeline → owner) to a self-controlled terminal owner, so a
	// usable audit archive must snapshot the WHOLE authority chain. (Learned
	// empirically: archiving only the process docs leaves that axis
	// indeterminate — prime content for the missing snapshot convention.)
	for _, d := range []string{srcProcessDID, relayProcessDID, srcPipelineDID, relayPipelineDID, ownerDID} {
		resp, err := http.Get(didDocURL(t, e.NodeBase, d))
		if err != nil {
			t.Fatalf("GET did doc for %s: %v", d, err)
		}
		raw, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("read did doc for %s: status %d, err %v", d, resp.StatusCode, readErr)
		}
		arch.didDocs[d] = raw
	}

	// --- The infrastructure dies. ---
	e.StopNode()
	if _, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: head}))); err == nil {
		t.Fatal("node still serving after StopNode — the offline claim below would be hollow")
	}

	// --- Offline phase: the auditor has only the archive. ---
	verifier := vc.NewVerifier(arch, ed25519.Verifier{})
	verifyArchived := func(hash, wantIssuer string) *vc.PipelinePassCredential {
		t.Helper()
		var cred vc.PipelinePassCredential
		if err := json.Unmarshal(arch.credentials[hash], &cred); err != nil {
			t.Fatalf("archived credential %s: %v", hash, err)
		}
		if got, err := cred.Hash(); err != nil || got != hash {
			t.Fatalf("archived credential content address = %q (err %v), want %q", got, err, hash)
		}
		if cred.Issuer() != wantIssuer {
			t.Fatalf("archived credential issuer = %q, want %q", cred.Issuer(), wantIssuer)
		}
		vres, err := verifier.Verify(context.Background(), &cred)
		if err != nil || vres.Overall != vc.ConfidenceVerified {
			t.Fatalf("offline verify %s: overall=%v err=%v", hash, vres, err)
		}
		return &cred
	}

	offlineHead := verifyArchived(head, relayProcessDID)
	pred := verifyArchived(offlineHead.PreviousCredential(), srcProcessDID)

	// The chain link is inside the signed bytes: the head's previousCredential
	// IS the predecessor's content address, and the origin is a FirstDrop.
	if pc := pred.PreviousCredential(); pc != "" {
		t.Errorf("origin credential has previousCredential %q, want a FirstDrop", pc)
	}

	// The product's own CHAIN verdict, offline: VerifyChain layers the
	// cross-credential structure checks per-credential Verify cannot see —
	// the data-flow invariant (outputHash[n] == inputHash[n+1]),
	// previousCredential linkage, the no-predecessor-at-origin rule,
	// proof.created monotonicity. With the node down this is the only chain
	// verdict obtainable at all (the async audit runner died with the node).
	cres, err := verifier.VerifyChain(context.Background(), []*vc.PipelinePassCredential{pred, offlineHead})
	if err != nil || cres.Overall != vc.ConfidenceVerified {
		t.Fatalf("offline VerifyChain: overall=%v err=%v", cres, err)
	}
}
