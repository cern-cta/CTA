---
hide:
  - navigation
  - toc
---

<!-- Should be sorted alphabetically -->

<!-- Not all of them need detailed explanations; it can be sufficient to link to the relevant section in some cases -->

# Glossary

**Jump to:** [A](#a) · [C](#c) · [D](#d) · [E](#e) · [F](#f) · [G](#g) · [K](#k) · [L](#l) · [M](#m) · [O](#o) · [P](#p) · [R](#r) · [S](#s) · [T](#t) · [V](#v) · [X](#x)

### A

**Archive** (verb) {#archive}
: Write a file to tape. See also: [Retrieve](#retrieve).

**Archive ID** {#archive-id}
: Identifier for the archived file version; files are treated as immutable, so each file has one Archive ID.

**Archive metadata** {#archive-metadata}
: Optional `JSON` provided at archive time to supply colocation hints.

**ATRESYS** {#atresys}
: Automated Tape REpacking SYStem. See the [tools documentation](ops/tools/ops-utils/atresys.md).

### C

**CASTOR** {#castor}
: Predecessor to CTA at CERN.

**Catalogue** {#catalogue}
: Relational database holding the tape file namespace, permanent system data configuration, and state changes.

**Ceph** {#ceph}
: Open-source distributed storage used as one of the Scheduler backends in CTA.

**CTA Frontend** {#cta-frontend}
: Component that handles workflow events from the disk buffer and requests from `cta-admin`.

**CTA instance** {#cta-instance}
: A deployment of CTA.

**`cta-admin`** {#cta-admin}
: Administrative CLI used by operators; issues requests to the CTA Frontend.

**`cta-rmcd`** {#cta-rmcd}
: Remote media changer daemon process on tape servers that interacts with the media changer to mount/unmount tapes.

**`cta-taped`** {#cta-taped}
: Tape daemon process on tape servers that manages tape drive interactions.

### D

**Disk instance** {#disk-instance}
: Disk buffer deployment (e.g., `EOS` or `dCache`) used by CTA; may serve one or multiple VOs, with CERN operating dedicated instances for large experiments (e.g., `eosctaatlas`) and shared instances (e.g., `eosctapublic`, `eosctapublicdisk`).

### E

**EOS** {#eos}
: Disk buffer system used at CERN. See [EOS](https://eos-web.web.cern.ch/eos-web/).

**EOS instance** {#eos-instance}
: A deployment of EOS.

**Evict** (verb) {#evict}
: Ask for immediate removal of disk copy (could be same as release). See also: [Release](#release) and [Stage](#stage)

### F

**FST** {#file-storage-server}
: EOS component responsible for managing disk storage.

### G

**gRPC** {#grpc}
: High-performance RPC framework. See <https://grpc.io/>.

### K

**Kerberos** {#kerberos}
: Network authentication protocol.

### L

**Liquibase** {#liquibase}
: Database-independent change-management library used for CTA Catalogue schema migrations.

**Logical library** {#logical-library}
: Partition of a physical tape library (by library and drive type) that controls which drives can mount which tapes; disabling a logical library blocks new mounts on its drives.

### M

**Maintenance daemon** {#maintenance-daemon}
: Process that executes routines for e.g. reporting jobs, repack expansion and scheduler garbage collection.

**mhVTL** {#mhvtl}
: Virtual tape library used in CI. See <https://github.com/markh794/mhvtl>.

**Mount policy** {#mount-policy}
: Named set of parameters (e.g., priority, minimum request age) per transfer type used by the Scheduler to decide queue eligibility and trigger mounts.

**Mount rule** {#mount-rule}
: Tuple of disk-instance, requester (user/group), mount-policy name, and activity regex that selects a specific mount policy.

**mTLS** {#mtls}
: "Mutual TLS", a security protocol that ensures both the client and server authenticate each other using digital certificates during a secure connection.

### O

**ObjectStore** {#objectstore}
: NoSQL object store used as a metadata backend.

**OStoreDB** {#ostoredb}
: CTA API layer for the ObjectStore when used as the `SchedulerDB`.

### P

**Puppet** {#puppet}
: Deployment automation engine. See <https://www.puppet.com/>.

**Protobuf** {#protobuf}
: Mechanism for serializing structured data. See <https://github.com/protocolbuffers/protobuf>.

### R

**`readtp`** {#readtp}
: Operator tool to sequentially read a tape.

**RelationalDB** {#relationaldb}
: CTA API layer for a relational database (PostgreSQL) when used as the `SchedulerDB`.

**Release** (verb) {#release}
: Indicate that files previously staged are no longer required to have a disk copy. See also: [Stage](#stage) and [Evict](#evict)

**Repack** {#repack}
: Copying/moving data between tapes for media refresh, replication, or migration to newer generations.

**Remote Media Changer Daemon** {#remote-media-changer-daemon}
: CTA process that manages interaction with the tape-library robot arm.

**Retrieve** (verb) {#retrieve}
: Read a file from tape. See also: [Archive](#archive).

**RMCD** {#rmcd}
: See **Remote Media Changer Daemon**.

**rsyslog** {#rsyslog}
: System for log processing. See <https://www.rsyslog.com/>.

**Rundeck** {#rundeck}
: Job automation system used for operations.

### S

**Scheduler** {#scheduler}
: CTA component that decides when a tape should be mounted in a drive.

**SchedulerDB** {#schedulerdb}
: Scheduler’s metadata backend (object store, file system, or relational DB) holding transient transfer metadata and changes.

**SSS** {#sss}
: Simple Shared Secret used for authentication.

**Stage** (verb) {#stage}
: Request that a disk copy is made available for a file on tape. See also: [Release](#release) and [Evict](#evict)

### T

**Tape cartridge** {#tape-cartridge}
: Physical medium (cartridge) containing magnetic tape.

**Tape daemon** {#tape-daemon}
: Process on a tape server that schedules mounts and manages threads for tape I/O, and workflow reporting.

**Tape drive** {#tape-drive}
: Device that reads/writes tape; a tape server may operate multiple drives.

**Tape library** {#tape-library}
: Physical system housing magnetic tapes and drives.

**Tape mount** {#tape-mount}
: Assignment of a tape drive to a tape to execute queued transfers.

**Tape pool** {#tape-pool}
: Logical collection of tapes governing file ownership and physical placement.

**Tape server** {#tape-server}
: Server running processes to operate a tape library, perform mounts, and participate in read/write.

**Tape slot** {#tape-slot}
: Slot position for a cartridge in a tape library.

**Transfer request** {#transfer-request}
: Request that may include multiple transfer jobs (e.g., archiving with multiple copies).

### V

**Virtual Organization** {#virtual-organization}
: Grouping of users by experiment/project; used for quotas (e.g., dedicated drives) and aggregated usage statistics.

### X

**XRootD** {#xrootd}
: High-performance, scalable data access. See <https://xrootd.github.io/>.

**XRootD SSI** {#xrootd-ssi}
: XRootD plugin enabling SSS authentication; used by the CTA Frontend and being phased out in favor of gRPC.
