---
hide:
  - toc
---

<!-- Should be sorted alphabetically -->

<!-- Not all of them need detailed explanations; it can be sufficient to link to the relevant section in some cases -->
# Glossary

Use these definitions to distinguish file identities, copies, requests and component roles. Follow each link for the authoritative concept or procedure.

**Jump to:** [A](#a) · [C](#c) · [D](#d) · [E](#e) · [F](#f) · [G](#g) · [J](#j) · [K](#k) · [L](#l) · [M](#m) · [O](#o) · [P](#p) · [R](#r) · [S](#s) · [T](#t) · [V](#v) · [W](#w) · [X](#x)

### A

**Admin Frontend** {#admin-frontend}
: CTA's interface for authorized operator requests. See [Admin Frontend](components/admin-frontend.md).

**Archive (verb)** {#archive}
: Create the required tape copies of a disk-system file. See [Archival](data-management/archival.md).

**Archive file ID** {#archive-id}
: CTA-assigned identity for an immutable archive file, shared by its tape copies. It is not a workflow request ID. See [Storage Model](data-management/storage-model.md#files-and-tape-copies).

**Archive metadata** {#archive-metadata}
: Optional JSON metadata accompanying archival, for example colocation hints. See [Archive Integration](../dev/guides/integrations/eos/archival.md).

**Archive route** {#archive-route}
: Mapping from storage class and copy number to a destination tape pool, with normal and repack route types. See [Storage Model](data-management/storage-model.md#storage-classes-tape-pools-and-archive-routes).

**ATRESYS** {#atresys}
: Automated Tape REpacking SYStem. See [Repack Automation](../ops/tools/repack-automation.md).

### C

**CASTOR** {#castor}
: CTA's predecessor at CERN; the CTA tape format inherits its AUL layout. See [CTA Tape Format](tape/media/format.md).

**Catalogue** {#catalogue}
: Persistent metadata for tape copies, resources and policies. It does not own the disk namespace. See [Catalogue](components/catalogue.md).

**Ceph** {#ceph}
: Distributed storage whose RADOS object store can persist CTA's objectstore scheduler backend. It is not the scheduler decision logic. See [Scheduler](components/scheduler.md).

**Copy number** {#copy-number}
: Identifies a required or recorded tape copy within an archive file, starting at 1. See [Storage Model](data-management/storage-model.md#files-and-tape-copies).

**CTA frontend** {#cta-frontend}
: Service configured for the [Workflow Frontend](components/workflow-frontend.md) or [Admin Frontend](components/admin-frontend.md) role, with distinct interfaces and authentication.

**CTA instance** {#cta-instance}
: A deployment of CTA services and their configured backends. See [Component Overview](components/index.md).

**`cta-admin`** {#cta-admin}
: Administrative command-line client of the Admin Frontend. See [cta-admin reference](../ops/tools/cta-admin.md).

### D

**Disk buffer** {#disk-buffer}
: Disk storage holding archive sources and retrieve destinations. The disk system also manages namespace and client access. See [Disk buffer](components/disk-system.md#disk-buffer).

**Disk instance** {#disk-instance}
: Identifier for a disk namespace connected to CTA. Combined with its disk file ID, it identifies a file in that namespace. See [Storage Model](data-management/storage-model.md#disk-instances-and-virtual-organisations).

**Disk replica** {#disk-replica}
: A file's data stored on disk, separate from its namespace entry and tape copies. See [File Workflows](data-management/index.md).

### E

**EOS** {#eos}
: Disk system providing namespace, replicas and client access; used with CTA at CERN. See [EOS documentation](https://eos-docs.web.cern.ch/).

**EOS instance** {#eos-instance}
: A deployment of EOS.

**Evict (verb)** {#evict}
: Remove a disk replica while keeping the namespace entry and tape copies. See [Deletion versus eviction](data-management/deletion.md#deletion-eviction-and-reclamation).

### F

**FST** {#file-storage-server}
: File Storage Server: EOS component that stores disk replicas. See [Disk System](components/disk-system.md#eos).

**FTS** {#fts}
: File Transfer Service: an external service that submits and tracks data transfers through storage-system interfaces. See [FTS](https://fts.web.cern.ch/fts/).

### G

**gRPC** {#grpc}
: Remote procedure call framework used by CTA's service APIs. See [Interfaces & Protocols](../dev/internals/interfaces.md) and [gRPC](https://grpc.io/).

### J {#j}

**JWT** {#jwt}
: JSON Web Token: used to authenticate a disk-instance or admin identity at the relevant frontend. See [Authentication](components/authentication.md).

### K

**Kerberos** {#kerberos}
: Network authentication protocol supported by the Admin Frontend. See [Authentication](components/authentication.md).

### L

**LBP** {#lbp}
: Logical block protection: per-block integrity checking, separate from whole-file checksums. See [Data Integrity](data-management/data-integrity.md#file-checksums-and-block-protection).

**Liquibase** {#liquibase}
: Tool used by CTA catalogue schema-migration procedures. See [Catalogue Schema Upgrades](../ops/run-and-maintain/upgrades/catalogue-schema/index.md).

**Logical library** {#logical-library}
: CTA grouping of tapes and compatible drives for mount selection. It need not match one hardware partition; disabling it blocks new mounts. See [Tape Libraries](tape/libraries.md#logical-libraries).

### M

**Maintenance daemon** {#maintenance-daemon}
: Processes queued disk-system reports, repack stages and scheduler recovery/cleanup. See [Maintenance Daemon](components/maintenance-daemon.md).

**Media changer daemon** {#media-changer-daemon}
: Handles cartridge movements through library robotics. See [Media Changer Daemon](components/media-changer-daemon.md).

**MGM** {#mgm}
: EOS namespace manager, coordinating file metadata and workflows. See [Disk System](components/disk-system.md#eos).

**mhVTL** {#mhvtl}
: Virtual tape-library software used in CTA testing. See [Virtual Tape Library](../dev/guides/tools-and-environment/virtual-tape-library.md).

**Mount policy** {#mount-policy}
: Named archive/retrieve priorities and minimum request ages used in mount selection; not a latency guarantee. See [Scheduling](data-management/scheduling.md).

**Mount rule** {#mount-rule}
: Maps a requester or requester group within a disk instance to a mount policy; retrieval can also match requester activities. See [Storage Model](data-management/storage-model.md#mount-policies-and-requesters).

**mTLS** {#mtls}
: Mutual TLS: certificate-based authentication of both peers. The Workflow Frontend maps its client's certificate identity to a disk instance. See [Authentication](components/authentication.md).

### O

**Objectstore** {#objectstore}
: CTA scheduler backend that persists work as objects, using storage such as Ceph RADOS or a filesystem. It is separate from the catalogue. See [Scheduler](components/scheduler.md).

**OStoreDB** {#ostoredb}
: Scheduler interface implementation for the objectstore backend. See [Scheduler internals](../dev/internals/components/scheduler/index.md).

### P

**Protobuf** {#protobuf}
: Protocol Buffers: serialization used in CTA interfaces. See [Interfaces & Protocols](../dev/internals/interfaces.md).

**Puppet** {#puppet}
: Deployment automation software; see the [project documentation](https://www.puppet.com/).

### R

**RAO** {#rao}
: Recommended Access Order: ordering a batch of reads on one tape to reduce positioning time. See [RAO](tape/rao.md).

**RelationalDB** {#relationaldb}
: Scheduler interface implementation for the PostgreSQL backend. See [PostgreSQL Scheduler](../dev/internals/components/scheduler/postgresql.md).

**Release (verb)** {#release}
: Indicate that a client no longer needs a staged disk replica. Other clients or retention policy may still keep it. See [Retrieval](data-management/retrieval.md#shared-requests-and-disk-replica-retention).

**Remote Media Changer Daemon** {#remote-media-changer-daemon}
: See [Media Changer Daemon](components/media-changer-daemon.md).

**Repack** {#repack}
: Replace or add catalogued tape copies through a temporary disk buffer. See [Repack](data-management/repack.md).

**Retrieve (verb)** {#retrieve}
: Read a recorded tape copy into the disk buffer. See [Retrieval](data-management/retrieval.md).

**RMCD** {#rmcd}
: See [Media Changer Daemon](components/media-changer-daemon.md).

**rsyslog** {#rsyslog}
: Log-processing software. See [Logging](../ops/run-and-maintain/monitoring/logging.md).

**Rundeck** {#rundeck}
: Job-automation software that sites may use for operational tasks; not a CTA workflow component.

### S

**Scheduler** {#scheduler}
: CTA logic for queueing work and selecting eligible tape mounts. See [Scheduler](components/scheduler.md) and [Scheduling](data-management/scheduling.md).

**SchedulerDB** {#schedulerdb}
: Interface to the persistent scheduler backend. Request state is shorter-lived than archive metadata but must survive restarts; it is not disposable. See [Scheduler](components/scheduler.md#backends).

**SSS** {#sss}
: Simple Shared Secret authentication, used for EOS tape-daemon data transfers. See [Authentication](components/authentication.md).

**Stage (verb)** {#stage}
: Request that a disk replica be made available for data on tape. See [Retrieval](data-management/retrieval.md).

**Storage class** {#storage-class}
: Named policy specifying the required number of tape copies for an archive file. See [Storage Model](data-management/storage-model.md#storage-classes-tape-pools-and-archive-routes).

### T

**Tape cartridge** {#tape-cartridge}
: Physical cartridge containing magnetic tape. See [Tape Media](tape/media/index.md).

**Tape copy / tape file** {#tape-copy}
: One recorded copy of an archive file, identified by copy number, VID and tape position. See [Storage Model](data-management/storage-model.md#files-and-tape-copies).

**Tape daemon** {#tape-daemon}
: Controls one drive, selects work through the scheduler, transfers data and reports outcomes to the scheduler. Queued disk-system notifications are sent by maintd. See [Tape Daemon](components/tape-daemon.md).

**Tape drive** {#tape-drive}
: Device that reads or writes one mounted cartridge. See [Tape Drives](tape/drives.md).

**Tape library** {#tape-library}
: Hardware housing cartridges, drives and robotics. See [Tape Libraries](tape/libraries.md).

**Tape mount** {#tape-mount}
: A read or write session using one cartridge in one drive. See [Scheduling](data-management/scheduling.md#queues-and-batching).

**Tape pool** {#tape-pool}
: Placement group of tapes belonging to a VO; archive routes select destination pools. See [Storage Model](data-management/storage-model.md#storage-classes-tape-pools-and-archive-routes).

**Tape server** {#tape-server}
: Host running one tape daemon per attached drive it operates. See [Tape Servers](tape/servers.md).

**Tape slot** {#tape-slot}
: Library location for a cartridge. See [Tape Libraries](tape/libraries.md).

**Transfer request** {#transfer-request}
: Work submitted to CTA that can generate multiple jobs, such as one archive job per required copy. See [File Workflows](data-management/index.md).

### V

**VID / VOLSER** {#vid}
: Volume identifier / volume serial: identifies a cartridge in CTA. Distinguish its six-character on-tape field from a physical barcode's media suffix. See [Tape Media](tape/media/index.md#identifiers-vidvolser).

**Virtual organisation (VO)** {#virtual-organization}
: Administrative owner of storage classes and tape pools, associated with a disk instance. Drive limits cap use rather than reserve drives. See [Storage Model](data-management/storage-model.md#disk-instances-and-virtual-organisations).

### W

**Workflow Frontend** {#workflow-frontend}
: CTA's gRPC interface for disk-system registration, archive, retrieve, cancel and delete events. See [Workflow Frontend](components/workflow-frontend.md).

### X

**XRootD** {#xrootd}
: Data-access framework used for EOS tape-daemon transfers. See [XRootD](https://xrootd.github.io/) and [Disk System](components/disk-system.md).
