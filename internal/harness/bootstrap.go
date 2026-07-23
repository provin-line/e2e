package harness

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	"github.com/provin-line/oss/crypto"
	"github.com/provin-line/oss/crypto/ed25519"
	"github.com/provin-line/oss/delegation"
	"github.com/provin-line/oss/did"
	didpb "github.com/provin-line/oss/gen/go/dplaax/did/v1"
	"github.com/provin-line/oss/gen/go/dplaax/did/v1/didpbconnect"
	"github.com/provin-line/oss/keystore"
	"github.com/provin-line/oss/keystore/filestore"
	"github.com/provin-line/oss/vc"
)

// BearerToken is the L1 token scenarios present. Its value is arbitrary: the
// node forwards it to the scenario's allow-all PDP, which permits everything.
const BearerToken = "e2e-dummy"

// Bearer stamps the e2e bearer token on a Connect request.
func Bearer[T any](req *connect.Request[T]) *connect.Request[T] {
	req.Header().Set("Authorization", "Bearer "+BearerToken)
	return req
}

// Owner is a client-side pipeline owner: the only identity whose private key
// lives outside the registry (KMS model).
type Owner struct {
	DID    string
	Signer crypto.Signer
	Pub    []byte
}

// NewOwner generates the owner keypair into a throwaway client-side keystore.
func NewOwner(t *testing.T, ownerDID string) *Owner {
	t.Helper()
	kp, err := (ed25519.Generator{}).Generate()
	if err != nil {
		t.Fatalf("owner keygen: %v", err)
	}
	ks := filestore.New(t.TempDir())
	if err := ks.SaveKeyPair(ownerDID, map[keystore.KeyID]*crypto.KeyPair{keystore.KeyIDSigning: kp}); err != nil {
		t.Fatalf("owner key save: %v", err)
	}
	// ks (filestore.Store) implements crypto.Signer directly (the KMS-shaped
	// Sign(did, keyID, data) seam) — the P0-3 keystore/crypto break removed the
	// ed25519.NewSigner wrapper this used to need.
	return &Owner{DID: ownerDID, Signer: ks, Pub: kp.PublicKey}
}

// signedOwnerDoc builds the owner's self-signed DID document registration
// body. Multikey, not JWK: the W3C eddsa-jcs-2022 suite the registry's
// verifyDocProof requires (exact dispatch — signer.suite.exact-dispatch)
// pairs a Multikey-encoded verification method with an @context-bearing
// proof; a JWK-encoded key matches no contract under that dispatch and is
// rejected fail-closed (Fork-W's proof/key-shape migration — the legacy
// JWK-era shape this used to build can still be READ by current oss, but a
// document freshly signed in that shape no longer verifies). Mirrors oss's
// own cmd/provin/internal/commands/owner.go selfSignedOwnerDoc exactly.
func (o *Owner) signedOwnerDoc(t *testing.T) []byte {
	t.Helper()
	vm, err := did.NewMultikeyVerificationMethod(o.DID+"#signing", o.DID, o.Pub)
	if err != nil {
		t.Fatalf("encode owner signing key: %v", err)
	}
	base := did.New(did.DocumentFields{
		// Load-bearing: the proof mirrors this onto itself (vc-di-eddsa
		// §3.3.1 step 2), and the suite classifier requires that mirror.
		Context: did.IssuedDocumentContexts(),
		ID:      o.DID, Controller: o.DID,
		VerificationMethod: []did.VerificationMethod{vm},
		AssertionMethod:    []string{o.DID + "#signing"},
	})
	body := base.Body()
	proof, err := vc.CreateProof(o.Signer, o.DID, string(keystore.KeyIDSigning), o.DID+"#signing", body, vc.CryptosuiteEdDSAJCS2022)
	if err != nil {
		t.Fatalf("owner doc proof: %v", err)
	}
	pb, err := json.Marshal(proof)
	if err != nil {
		t.Fatalf("marshal proof: %v", err)
	}
	var pm map[string]any
	if err := json.Unmarshal(pb, &pm); err != nil {
		t.Fatalf("unmarshal proof: %v", err)
	}
	body["proof"] = pm
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal owner doc: %v", err)
	}
	return raw
}

// Bootstrap registers the owner and issues every pipeline and process DID a
// scenario's loops sign as, all over the node's ConnectRPC surface. The
// registry generates and holds the pipeline/process signing keys (KMS model);
// the client never sees a private key besides the owner's.
func Bootstrap(t *testing.T, nodeBaseURL string, owner *Owner, pipelineDIDs []string, processDIDs []string) {
	t.Helper()
	ctx := context.Background()
	client := didpbconnect.NewDIDServiceClient(http.DefaultClient, nodeBaseURL)

	if _, err := client.RegisterOwner(ctx, Bearer(connect.NewRequest(&didpb.RegisterOwnerRequest{
		DidDocument: owner.signedOwnerDoc(t),
	}))); err != nil {
		t.Fatalf("RegisterOwner(%s): %v (code %v)", owner.DID, err, connect.CodeOf(err))
	}

	for _, p := range pipelineDIDs {
		dlg, err := delegation.Build(owner.Signer, owner.DID, delegation.DelegationSubject{ID: p, DelegatedBy: owner.DID})
		if err != nil {
			t.Fatalf("delegation.Build(%s): %v", p, err)
		}
		dlgBytes, err := json.Marshal(dlg)
		if err != nil {
			t.Fatalf("marshal delegation: %v", err)
		}
		if _, err := client.IssuePipeline(ctx, Bearer(connect.NewRequest(&didpb.IssuePipelineRequest{
			TargetDid: p, Delegation: dlgBytes,
		}))); err != nil {
			t.Fatalf("IssuePipeline(%s): %v (code %v)", p, err, connect.CodeOf(err))
		}
	}
	for _, p := range processDIDs {
		dlg, err := delegation.Build(owner.Signer, owner.DID, delegation.DelegationSubject{ID: p, DelegatedBy: owner.DID})
		if err != nil {
			t.Fatalf("delegation.Build(%s): %v", p, err)
		}
		dlgBytes, err := json.Marshal(dlg)
		if err != nil {
			t.Fatalf("marshal delegation: %v", err)
		}
		if _, err := client.IssueProcess(ctx, Bearer(connect.NewRequest(&didpb.IssueProcessRequest{
			TargetDid: p, Delegation: dlgBytes,
		}))); err != nil {
			t.Fatalf("IssueProcess(%s): %v (code %v)", p, err, connect.CodeOf(err))
		}
	}
}
