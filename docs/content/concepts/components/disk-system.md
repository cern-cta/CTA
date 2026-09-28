# Disk System

The **disk system** owns the client-facing namespace, manages disk replicas and sends workflow requests to CTA. It provides the disk storage used for transfers and handles their outcomes.

## Disk buffer

The **disk buffer** is the storage area holding files awaiting archival or retrieved from tape.

During archival, files are copied into the disk buffer, triggering an archival request to be sent to the [Workflow Frontend](workflow-frontend.md).
Eventually (minutes to hours later), this request will be processed by a tape daemon, which reads the file from
the disk buffer and writes it to tape. Once successful archival has been reported back to the disk system, it can evict the disk replica according to its retention policy while preserving the namespace entry.

To read a file back from tape, a retrieve request is sent to the Workflow Frontend. When the request is processed by a tape
daemon, the file is read from tape and written into a new file replica on the disk buffer (using the same metadata entry
created during archival).

## Reporting results

Accepting a workflow request does not mean the transfer has completed. CTA processes archive and retrieve requests asynchronously, and the disk system needs to learn their outcome before updating its own file or request state.

Requests can carry reporting URLs for completion and failure notifications. The scheduler tracks work awaiting reporting, and the [Maintenance Daemon](maintenance-daemon.md) sends those notifications to the disk system. Reporting is separate from transferring the file: a transfer can finish while its notification is still pending. Failed reporting attempts are handled by the scheduler's retry policy.

| Workflow | What the disk system needs to learn |
| --- | --- |
| Archival | Whether the required tape copies were created, before deciding that its source replica can be evicted. |
| Retrieval | Whether the destination replica is ready or the request failed. Success may be detected through the disk write or an explicit report, depending on the integration. |

The reporting protocol and how a notification updates the namespace are integration-specific. These callbacks are distinct from the workflow events that the disk system sends to the Workflow Frontend.

## Integration requirements

CTA was designed to be agnostic to the disk buffer technology.

An integration must provide:

- Workflow requests with file identities, policies and metadata CTA can validate.
- Source and destination URLs supported by CTA's data-transfer interface, with credentials allowing tape daemons to read and write them.
- Completion and failure handling that associates outcomes with the correct file or request.
- An unchanged, readable archive source until CTA no longer needs it, and sufficient buffer capacity for retrieved data.
- Disk and network throughput sufficient for the combined active drives. Storage technology should be chosen for the workload.

See [Disk Buffer Integration](../../ops/deploy-and-configure/integrations/index.md) for system-specific setup.

## EOS

[EOS](https://eos-docs.web.cern.ch/diopside/architecture/index.html) provides a namespace manager (**MGM**) and file storage servers (**FSTs**). CTA reads and writes EOS replicas and reports workflow outcomes to EOS.

See [Archival](../data-management/archival.md#eos-example), [Retrieval](../data-management/retrieval.md#eos-example), and [EOS Integration](../../ops/deploy-and-configure/integrations/eos/configuration.md).

## dCache

[dCache](https://www.dcache.org/) integrates through its [CTA plugin](https://github.com/dCache/dcache-cta). Check the plugin release's frontend protocol and authentication requirements against the CTA release being deployed; an integration's existence does not imply that every release combination is compatible.

See [dCache Integration](../../ops/deploy-and-configure/integrations/dcache.md).
