# Deployment

A CTA deployment requires the following infrastructure:

- One or more machines to run the CTA Frontend. These can be virtual machines. For a High Availability setup, there 
  should be multiple independent frontends. For a production setup it is recommended to run a separate frontend for
  the administrative CLI to handle operator requests, automated scripts and monitoring.
- One or more machines with one or more tape drives attached. There should be one CTA Tape Daemon process per tape
  drive, to manage the request scheduling for that drive and the low-level SCSI interaction with the drive.
- A fast disk buffer system in front of CTA.
- A highly-available database system for the [CTA Catalogue](components/catalogue.md), the permanent metadata store
  for all tapes and tape files.
- A highly-available system for the [Scheduler Database](components/scheduler.md), the transient metadata store for
  all archive and retrieve requests and queues.

In addition, a production system will need to include monitoring and processing of automated scripts for tasks such as 
tape supply logic, managing problematic tapes or drives, and repack orchestration.
