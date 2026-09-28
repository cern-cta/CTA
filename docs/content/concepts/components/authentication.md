# Authentication

CTA supports multiple authentication methods: SSS (Simple Shared Secret), Kerberos, JWT (JSON Web Token), and mTLS (mutual TLS).
CTA distinguishes three authentication boundaries:

- The authentication between the disk buffer and the CTA frontend. The disk buffer will send workflow events to the frontend and needs to be authenticated in order to do so.
- The authentication between the tape daemons and the disk buffer. The tape daemons need to be able to read and write data directly through the disk buffer.
- The authentication of users interacting with CTA via the `cta-admin` tool.

## Authentication Methods by Interface

### Service APIs
The APIs have separate authentication boundaries:

- [Workflow Frontend](workflow-api.md): supports JWT and mTLS authentication for workflow events
(see [WFE Authentication Configuration](../../ops/configuration/authentication.md#mtls-authentication-wfe-only)).
- [Admin Frontend](admin-api.md): supports JWT and Kerberos authentication for `cta-admin` commands
(see [Admin Authentication Configuration](../../ops/configuration/authentication.md#kerberos-admin-api)).

### Tape Daemon
The tape daemon reads and writes file data through the disk system's data-transfer interface. The credentials for this connection are separate from frontend workflow and admin credentials.

#### EOS

The EOS integration uses SSS authentication for tape-daemon data transfers. See [EOS Configuration](../../ops/integrations/eos/configuration.md).

The diagram shows the three independent connections in the EOS integration. Arrows point from the component initiating the connection to the service authenticating it.

```mermaid
flowchart LR
    eosWorkflow["EOS<br/>Workflow requests"]
    workflow["CTA Workflow Frontend"]
    adminClient["Operator<br/>cta-admin"]
    admin["CTA Admin Frontend"]
    taped["CTA tape daemon"]
    eosData["EOS<br/>File transfers"]

    eosWorkflow -->|"gRPC · JWT or mTLS"| workflow
    adminClient -->|"gRPC · JWT or Kerberos"| admin
    taped -->|"XRootD · SSS"| eosData
```
