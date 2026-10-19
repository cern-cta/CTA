!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Admin Frontend Internals {#admin-api-internals}

Command dispatch and authentication for administrative clients. Operator settings belong in [Admin Frontend Configuration](../../../ops/deploy-and-configure/configuration/admin-frontend.md).

## Admin request dispatch

Both kinds of admin commands are received by the `cta-admin-api` module, which authenticates the request
(see [Authentication dependencies](#authentication-dependencies)) and then serves it according to how its result goes back to `cta-admin`:

- **Commands answered with a single response** (`add`, `ch`, `rm`, `up`, `down`, ...) are received by `CtaRpcSyncImpl`.
  The `process` method of `SyncCmd` (`commands/sync/`) looks up the handler of the `<cmd, subcmd>` pair in the `SyncCmdRegistry`
  and runs it. The handlers are grouped by domain in `commands/sync/handlers/`, and receive a `SyncCmdContext` with the catalogue,
  scheduler and configuration values they need.
- **Commands streaming their result** (`ls`, `showqueues`) are received by `CtaRpcStreamImpl`. The `StreamFactory`
  (`commands/stream/`) builds the `CtaAdminResponseStream` of the `<cmd, subcmd>` pair, which the
  `CtaAdminServerWriteReactor` then writes to the client item by item. The commands listing a plain list of items use a
  `VectorResponseStream` with a converter per item type (`commands/stream/converters/`), the others have a stream
  class of their own (`commands/stream/tapes/` and `commands/stream/scheduling/`).

```mermaid
flowchart LR
  A[cta-admin] -->|rpc CtaRpc.Admin| B[CtaRpcSyncImpl]
  A -->|rpc CtaRpcStream.GenericAdminStream| C[CtaRpcStreamImpl]
  B --> D[SyncCmd::process]
  D --> E["handlers (add, ch, rm, ...)"]
  C --> G["createStream()"]
  G --> H[StreamFactory]
  H --> I[CtaAdminServerWriteReactor]
```

<!-- void RequestMessage::process(const cta::xrd::Request& request, cta::xrd::Response& response, XrdSsiStream*& stream) -->

<!-- ```mermaid
classDiagram
class FrontendService {
  getters/setters for the members
  m_log
  m_catalogue
  m_scheddbInit
  m_scheddb
  m_scheduler
  m_acceptRepackRequests
  ....
  m_namespaceMap
}

class GrpcClient {
  std::unique_ptr<eos::rpc::Eos::Stub> stub_
  std::string m_token
  uint64_t m_tag
}
``` -->


## Authentication dependencies


JWT authentication uses the `jwt-cpp` header-only library for JWT decoding, validation, and signature verification.
The only supported algorithm is RS256 (RSA with SHA-256). The library is added as a git submodule at `common/jwt-cpp`.

For fetching JWKS from remote endpoints the CURL library is used.

TLS transport uses the gRPC TLS stack.



## Kerberos negotiation


Each Kerberos-authenticated `cta-admin` command consists of two remote procedure calls:
First, the client calls `Negotiate`, which is a bidirectional streaming rpc. The client and the server perform the kerberos token exchange.
When the server successfully authenticates the client, it generates and stores a 256-bit nonce to be used for this session, then the nonce is sent back to the client.

1. Obtain Kerberos Ticket: The client must have a valid Kerberos ticket-granting ticket (TGT), typically obtained via kinit. The ticket must be valid for the configured Kerberos realm.
2. Initiate Negotiation: The client calls the dedicated `Negotiate` bidirectional streaming RPC. This is a separate service from the main CTA admin API, specifically designed for Kerberos authentication. The client also learns the Service Principal Name to use for the GSSAPI calls when beginning the Negotiate rpc.
3. GSSAPI Token Exchange: The client generates a GSSAPI/SPNEGO token using `gss_init_sec_context()` and sends it in the challenge field of the negotiation request. The server validates this
token using `gss_accept_sec_context()` against its service keytab.
4. (Multi-Round) Negotiation: GSSAPI context establishment may require multiple round-trips. If the server returns `is_complete=false` with a challenge, the client must process that challenge
with `gss_init_sec_context()` and send the resulting token back. This continues until `is_complete=true`.
5. Receive Session Token: Upon successful authentication, the server extracts the client principal name from the security context and generates a nonce/session token. This nonce is stored
server-side (in TokenStore) mapped to the authenticated username, and returned to the client.
6. Execute Command: The client includes the session token in the gRPC metadata using the `"Authorization: Negotiate"` header and calls the Admin rpc to execute the requested command. The
server validates that the token exists in token storage, confirms that the associated username exists in the admin database, removes the session token from its storage and proceeds with command execution.

Each `cta-admin` command invocation repeats this entire process: negotiation, token exchange, and command execution.

The Kerberos implementation relies on the system's GSSAPI libraries (typically provided by `krb5-libs` or equivalent).

## Shared implementation

The APIs currently share frontend code and dependencies. This separation follows their responsibilities; it does not imply that the implementation split is complete.

See [Workflow Frontend Internals](workflow-frontend.md) for disk-system workflow requests.

## Add or change an administrative command

TODO: Document request definitions, command registration and dispatch, authorization, streaming responses, error mapping, and the relevant tests.
