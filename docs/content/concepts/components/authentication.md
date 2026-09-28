# Authentication

CTA authenticates the services and operators using its interfaces. **Authentication** establishes an identity; **authorization** determines whether that identity may perform the requested operation.

The interfaces below cover workflow requests, operator commands and disk transfers. They are not a complete inventory of deployment trust boundaries: catalogue, scheduler and library-control connections also need appropriate access controls.

## Authentication Methods by Interface

The identity and credentials depend on the interface. A credential accepted on one connection does not automatically authorize the others.

### Service APIs

| Interface | Identity | Authentication methods |
| --- | --- | --- |
| [Workflow Frontend](workflow-frontend.md) | The registered disk instance submitting workflow events. | JSON Web Token (**JWT**) or mutual TLS (**mTLS**); each Workflow Frontend enables exactly one. |
| [Admin Frontend](admin-frontend.md) | The operator making administrative requests. | JWT and/or Kerberos; the identity must also be an authorized catalogue admin user. |

For workflow JWTs, the subject identifies the disk instance. With mTLS, the frontend maps the client's certificate identity to a disk instance. See [Authentication Configuration](../../ops/deploy-and-configure/configuration/authentication.md) for identity mapping and credential setup.

### Tape Daemon

The tape daemon needs permission to read archive sources and write retrieve destinations through the disk system's data interface. These credentials are separate from frontend credentials.

The mechanism depends on the integration; see [EOS Configuration](../../ops/deploy-and-configure/integrations/eos/configuration.md) or [dCache Integration](../../ops/deploy-and-configure/integrations/dcache.md).

#### EOS

The tape daemon reads and writes EOS replicas through XRootD using **Simple Shared Secret (SSS)** authentication. These credentials are separate from those EOS uses to submit workflow requests to the [Workflow Frontend](#service-apis).

See [EOS Configuration](../../ops/deploy-and-configure/integrations/eos/configuration.md) for credential setup.
