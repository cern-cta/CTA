---
title: CTA Concepts
---

# CTA Concepts

The [CERN Tape Archive (CTA)](https://cta.web.cern.ch/) is software for managing tape storage at exabyte scale. It schedules archival and retrieval, manages tape hardware, and tracks the files stored on tape. A separate disk system provides the file namespace, disk buffer, and client-facing interface.

This section introduces how CTA works and the terminology used throughout the documentation. For deployment and operating procedures, use [Operations](../ops/index.md). For implementation and contribution guides, use [Development](../dev/index.md).

CTA was developed at CERN, where the name also refers to the deployed tape service. [EOS](https://eos-docs.web.cern.ch/) and [dCache](https://www.dcache.org/) are examples of disk systems integrated with CTA; the core responsibilities described here are independent of the chosen disk system.

## From disk to tape and back

A file's namespace entry, disk replica, and tape copy are distinct. The namespace identifies the file, while the replicas and copies hold its data. A file can remain visible in the namespace even when its data is only on tape.

A typical file journey is:

1. A client writes a file into the disk system. The disk system submits an archival request to CTA.
2. CTA queues the request and schedules a tape mount. A tape daemon reads the file from the disk buffer and writes the required tape copy or copies, with their metadata recorded in the catalogue.
3. Once archival completes, the disk system can remove its disk replica according to its retention policy while keeping the namespace entry.
4. When a client needs a file whose data is no longer on disk, the disk system requests retrieval. CTA queues the work, reads a tape copy, and transfers the data back to the disk buffer. The client accesses the resulting disk replica through the disk system.

Archival and retrieval are asynchronous: accepting a request does not mean that the data transfer has completed. Tape drives are shared resources, and a cartridge must be mounted and positioned before its files can be read. CTA groups work into batches to reduce mounting and positioning overhead, balancing throughput against request latency.

## Responsibilities

The **disk system** owns the client-facing namespace, including paths and filenames. It provides disk replicas for transfers, submits workflow requests to CTA, and serves data to clients.

**CTA** owns the metadata describing tape copies and tape resources. It schedules archive and retrieve work, controls tape operations, and transfers data between the disk buffer and tape.

**Clients and transfer orchestrators** are external to CTA. They access the disk system and manage their own transfer workflows, including error handling and retries. FTS is one example used at CERN. Available client protocols and request behaviour depend on the selected disk-system integration.

## Design philosophy

CTA was designed for high archival throughput to handle the enormous volumes of data produced by the LHC experiments. This requirement shapes its emphasis on sustained transfers and efficient use of tape hardware, alongside control over competing workloads and visibility into the system's behaviour.

- **Use drive time for useful work.** Keep available drives serving eligible requests, and batch transfers to amortise the cost of mounting and positioning tapes. The aim is sustained data transfer with minimal idle time and unnecessary tape movement.
- **Keep tape streaming.** CTA is designed to archive at the tape drive’s native transfer rate, provided the disk buffer and network can sustain that throughput. A fast buffer decouples tape writes from individual client transfers, keeping drives supplied with data. During retrieval, the disk buffer and network must likewise absorb data fast enough to keep the drives streaming.
- **Balance throughput and waiting time.** Larger batches improve efficiency, but waiting to form them delays requests. CTA's scheduling policies are highly configurable, allowing operators to tune this balance and prioritise competing workloads while aiming to prevent eligible work from being indefinitely starved.
- **Be observable.** Request and resource state, logs, and metrics give operators the information they need to understand system behaviour, tune performance, and identify and investigate issues.

## Explore the concepts

- [Components](components/index.md) shows the architecture, request and data paths, and the roles of the APIs, daemons, catalogue, scheduler, and disk buffer.
- [Tape Infrastructure](tape/media/index.md) introduces tape media; continue with [drives](tape/drives.md), [libraries](tape/libraries.md), and [tape servers](tape/servers.md) to understand the hardware roles.
- Data Management covers [storage policies](data-management/storage-model.md), [file workflows](data-management/index.md), and [scheduling](data-management/scheduling.md).
- [Authentication](components/authentication.md) explains identities and access boundaries. Use the [Glossary](glossary.md) to look up unfamiliar terms.
