# Disk Buffer

During archival, files are copied into the disk buffer, triggering an archival request to be sent to the CTA Frontend. 
Eventually (usually several hours later), this request will be processed by a tape daemon, which reads the file from
the disk buffer and writes it to tape. When the operation is completed, the disk file replica is deleted but the file
metadata remains.

To read a file back from tape, a retrieve request is sent to the CTA Frontend. When the request is processed by a tape
daemon, the file is read from tape and written into a new file replica on the disk buffer (using the same metadata entry
created during archival).

CTA was designed to be agnostic to the disk buffer technology. Disk systems should fulfill the following requirements 
to integrate with CTA:

* The disk system must have a mechanism to trigger and send [Workflow Events](../file-lifecycle/introduction.md) to 
  the CTA Frontend.
* The disk buffer needs to provide high throughput and low contention, in order to supply data to tape drives at a 
  constant rate. Starving tape drives can trigger dismounting of tapes, which has a serious negative impact on system 
  performance. This normally means using fast SSDs.

Currently official integrations exist for two disk storage technologies, EOS and dCache.

## EOS

[EOS Open Storage](https://eos-docs.web.cern.ch/) was created for the extreme computing requirements of the 
Large Hadron Collider (LHC) at CERN. EOS architecture is explained in detail in the
[official EOS documentation](https://eos-docs.web.cern.ch/diopside/architecture/index.html).

## dCache

See the [official dCache website](https://www.dcache.org/) and its [CTA plugin](https://github.com/dCache/dcache-cta).

Publications about dCache-CTA integration:

* [dCache integration with CERN Tape Archive](https://lss.fnal.gov/archive/2023/poster/fermilab-poster-23-041-csaid.pdf) (poster)
* [dCache-CTA integration: Status, Experience, Plans](https://cds.cern.ch/record/2803834) (presentation at EOS workshop, 2022)
* [A year with CTA](https://indico.desy.de/event/43473/contributions/165917/) (presentation at dCache workshop, 2024)
