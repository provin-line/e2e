package harness

import (
	"context"
	"encoding/base64"
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
	return &Owner{DID: ownerDID, Signer: ed25519.NewSigner(ks), Pub: kp.PublicKey}
}

// signedOwnerDoc builds the owner's self-signed DID document registration body.
func (o *Owner) signedOwnerDoc(t *testing.T) []byte {
	t.Helper()
	base := did.New(did.DocumentFields{
		ID: o.DID, Controller: o.DID,
		VerificationMethod: []did.VerificationMethod{{
			ID: o.DID + "#signing", Type: "JsonWebKey2020", Controller: o.DID,
			PublicKeyJWK: map[string]any{
				"kty": "OKP", "crv": "Ed25519",
				"x": base64.RawURLEncoding.EncodeToString(o.Pub),
			},
		}},
		AssertionMethod: []string{o.DID + "#signing"},
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
