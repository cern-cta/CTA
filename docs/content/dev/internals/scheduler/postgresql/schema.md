# PostgreSQL Backend DB Schema


The CTA commands `cta-scheduler-schema-drop`, `cta-scheduler-schema-create` will use the username from the connection string provided in the configuration file to connect to the DB backend and create the schema for scheduling operations. 

!!! tip

    In case a schema name different than `scheduler` is needed, one can pass it via the search_path option in the connection string and it will be taken into account (for PostgreSQL DB only), e.g. both `postgresql://user:pw@dbhost/mydb?options=-c search_path=myschemaname` and decoded versions `postgresql://user:pw@dbhost/mydb?options=-c%20search_path%3Dmyschemaname` are recognised. The search path is set for the account by default to go to `scheduler` (or alternatively the given schema name) for all future sessions automatically.


## Main Workflow Tables

Separate tables dedicated for each workflow type are being used:

* **`PENDING_QUEUE`**: tables contain all the jobs which are being queued by the frontend for processing and are waiting to be picked up by the mount. This processing step is write-heavy and needed this separation in order to stabilise performance as this table grows.
* **`ACTIVE_QUEUE`**: all jobs from the `PENDING_QUEUE` are being moved to the `ACTIVE_QUEUE` as soon as they were picked up for processing.
* **`FAILED_QUEUE`**: table type which serves as a graveyard for terminally failed jobs. 


Each of these 3 table types exist per workflow (`Archive/Retrieve/Repack`), e.g. for the `Archive` workflow, we have: `ARCHIVE_PENDING_QUEUE`,`ARCHIVE_ACTIVE_QUEUE` and `ARCHIVE_FAILED_QUEUE` tables. 


## Repack Request Statistics

Repack is a process of migrating files from one tape to another one. This is being done as a maitenance task in order to librerate slots in tape libraries as new media come to market with different density. Another usecase for Repack is to make additional copies of files and reclaim tape space after files were deleted from the CTA Catalogue.  

The following tables are used: 

* **`REPACK_REQUEST_TRACKING`**: holds general tracking information about tracking each queued repack request.
* **`REPACK_REQUEST_DESTINATION_STATISTICS`**: holds detailed information about the destination tapes where files are being migrated for each repack job.  


## Disk System Sleep Tracking

There is an **`DISK_SYSTEM_SLEEP_TRACKING`** table monitoring the disk space relevant for the queues being processed. It allows to hold back job processing in case the disk space is full. 

## Maintenance Tables

The **`MOUNT_QUEUE_LAST_FETCH`** table is tracking the timestamps of the last time a mount process has fetched jobs from the queue. This allows to identify dead mount processes (e.g. after a power cut) and update all the job in the respective scheduling tables accordingly. 