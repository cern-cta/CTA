# Scheduler Workflow Integration


```mermaid
flowchart TB

    start(( ))
    start -->|queue| PI

    subgraph ROW[" "]
        direction LR

        subgraph A[<b>ACTIVE_QUEUE</b>]
            direction TB
            AS["size: 10-100k"]
            AI["INSERT"]
            AU["UPDATE<br/>validate, fetch, report"]
            AD["DELETE<br/>successful job"]

            AI --> AU --> AD
        end

        subgraph F[<b>FAILED_QUEUE</b>]
            direction TB
            FS["size: 10-100k"]
            FI["INSERT"]
            FD["DELETE"]

            FI --> FD
        end

        subgraph P[<b>PENDING_QUEUE</b>]
            direction TB
            PS["size: 1-10M+"]
            PV["VIEW<br/>summary"]
            PD["DELETE"]
            PI["INSERT"]

            PV --> PD --> PI
        end

    end

    %% main flows
    PD -->|processing| AI
    AD -->|re-queue jobs / transfer failed| PI
    AD -->|move failed jobs| FI
    FD -.->|re-queue jobs by operator tools| PI

    %% ===== Styles =====
    classDef size fill:#f39c12,color:#ffffff,stroke:#333,stroke-width:1px;
    classDef action fill:#3498db,color:#ffffff,stroke:#333,stroke-width:1px;
    style A fill:#ff
    style F fill:#ff
    style P fill:#ff
    class AS,FS,PS size;
    class AI,AU,AD,FI,FD,PV,PD,PI action;
```


In the diagram you can see the the relative sizes of the tables (orange). By far the biggest table is the pending queue table type. For the moment (shall be changed with redesign of the scheduler logic) each tape server calls a `VIEW` on the full table summarising all the queued jobs in order to decide if a tape shall be mounted or not. Once it has been decided that there is enough work, the mount process will start deleting jobs from the `PENDING_QUEUE` while interting them to the `ACTIVE_QUEUE`. The `ACTIVE_QUEUE` is further partitioned by the tape identifier for retrieve or tape pool for archive which allows to keep the table size considerably smaller than the original `PENDING_QUEUE` (same for the `FAILED_QUEUE`). 

In the `ACTIVE_QUEUE` the most active queries are update and select statements necessary for the followup of the job status and its reporting to the disk system. If the job has been successfuly completed (file transferred and disk reporting has been successful) it will be deleted from this table. Several retries, depending on the workflow configuration, are possible. In such a case, the job that failed the file transfer will be moved back to the `PENDING_QUEUE` and picked up again either by the same or by another mount process (also depending on the specific workflow configuration). In case the reporting has failed it will be retried several times. In case of reporting did not succeed, the job will be moved to the `FAILED_QUEUE` table from which it can be moved back to the `PENDING_TABLE`.


