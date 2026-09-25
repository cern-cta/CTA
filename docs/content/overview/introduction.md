---
title: CTA technical overview
---

# CTA Overview

The CERN Tape Archive (CTA) is a software system designed to store scientific data at exabyte scale. CTA can refer 
both to the software and to the service operated at CERN. It is part of the trinity of storage and data management 
software services provided by CERN's IT department, alongside the disk storage system [EOS](https://eos-docs.web.cern.ch/)
and File Transfer System [FTS](https://fts3-docs.web.cern.ch/).

Although CTA was conceived as the tape back-end to EOS, it has evolved to be interoperable with other disk systems, 
notably [dCache](https://www.dcache.org/).

The work of data archival and retrival is divided between the disk and tape system as follows:

## Functions of the disk system (EOS or dCache)

- Managing the namespace for files stored on tape (disk file metadata, including path and filename)
- Providing a storage buffer for files during archival and retrieval
- Serving as the interface between external clients and CTA

## Functions of the tape system (CTA)

- Managing the tape file metadata for files stored on tape
- Providing scheduling functions for archival and retrieval requests
- Controlling data transfers between the disk buffer and the tape hardware

## Client software

The third system component is the client software, responsible for transferring files into and out of the disk system, 
and managing errors and retries. In most CERN use cases, data transfers are orchestrated using [FTS](https://fts.web.cern.ch/fts/).
However, any client which can communicate using the XRootD or HTTP protocol can be used in principle.
