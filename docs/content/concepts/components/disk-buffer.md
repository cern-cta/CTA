# Disk Buffer

During archival, files are copied into the disk buffer, triggering an archival request to be sent to the [Workflow API](workflow-api.md).
Eventually (minutes to hours later), this request will be processed by a tape daemon, which reads the file from
the disk buffer and writes it to tape. Once successful archival has been reported back to the disk system, it can evict the disk replica according to its retention policy while preserving the namespace entry.

To read a file back from tape, a retrieve request is sent to the Workflow API. When the request is processed by a tape
daemon, the file is read from tape and written into a new file replica on the disk buffer (using the same metadata entry
created during archival).

## Reporting results

Accepting a workflow request does not mean the transfer has completed. CTA processes archive and retrieve requests asynchronously, and the disk system needs to learn their outcome before updating its own file or request state.

Requests can carry reporting URLs for completion and failure notifications. The scheduler tracks work awaiting reporting, and the [Maintenance Daemon](maintenance-daemon.md) sends those notifications to the disk system. Reporting is separate from transferring the file: a transfer can finish while its notification is still pending. Failed reporting attempts are handled by the scheduler's retry policy.

- **Archival:** a success report lets the disk system record that the required tape copies have been created. The disk system remains responsible for deciding when to evict its disk replica.
- **Retrieval:** the disk system must learn when the disk replica is ready, or when retrieval has failed, so it can update the waiting request. Depending on the integration, successful completion may be detected through the data-transfer operation itself or through an explicit report.

The reporting protocol and how a notification updates the namespace are integration-specific. These callbacks are distinct from the workflow events that the disk system sends to the Workflow API.

## Integration requirements

CTA was designed to be agnostic to the disk buffer technology. Disk systems should fulfill the following requirements
to integrate with CTA:

* The disk system must have a mechanism to trigger and send [Workflow Events](../data-management/index.md) to
  the Workflow API.
* The integration must provide a way to receive completion and failure results and associate them with the corresponding files or requests.
* The disk buffer needs to provide high throughput and low contention, in order to supply data to tape drives at a
  constant rate. Starving tape drives can trigger dismounting of tapes, which has a serious negative impact on system
  performance. This normally means using fast SSDs.

The sections below describe disk-system integrations separately from the core CTA responsibilities. Operational setup belongs under [Disk Buffer Integration](../../ops/integrations/index.md).

## EOS

[EOS Open Storage](https://eos-docs.web.cern.ch/) was created for the extreme computing requirements of the
Large Hadron Collider (LHC) at CERN. EOS architecture is explained in detail in the
[official EOS documentation](https://eos-docs.web.cern.ch/diopside/architecture/index.html).

For EOS, CTA's disk reporter uses `eosQuery://` reporting URLs to send workflow notifications to the EOS MGM through XRootD queries. EOS processes these notifications to update its own state; for example, an archive-success notification signals that the file has migrated to tape. See [Archival](../data-management/archival.md) and [Retrieval](../data-management/retrieval.md) for the EOS-specific workflows.

## dCache

See the [official dCache website](https://www.dcache.org/) and its [CTA plugin](https://github.com/dCache/dcache-cta).

Publications about dCache-CTA integration:

* [dCache integration with CERN Tape Archive](https://lss.fnal.gov/archive/2023/poster/fermilab-poster-23-041-csaid.pdf) (poster)
* [dCache-CTA integration: Status, Experience, Plans](https://cds.cern.ch/record/2803834) (presentation at EOS workshop, 2022)
* [A year with CTA](https://indico.desy.de/event/43473/contributions/165917/) (presentation at dCache workshop, 2024)
