# Repack

Repacking retrieves files from a source tape into a repack buffer and archives them onto destination tapes. It is used for media migration, creating additional copies, and tape repair.

## Repack modes

- **Move:** migrate existing copies to other tapes.
- **Add copies:** create missing copies required by the storage class.
- **Move and add copies:** combine both operations.
- **Tape repair:** supply recovered files in the repack buffer.

## Repack workflow


```mermaid
sequenceDiagram
    participant Admin as cta-admin
    participant FE as CTA Frontend
    participant DB as Sheduler DB
    participant MD as Maintenance Daemon
    participant TD as Tape Daemon
    participant Buffer as Disk Buffer
    rect rgba(255,255,255,0.1)
    Note right of FE : Initial repack<br>request by the<br>operator
    Admin ->> FE : add repack request
    activate Admin
    FE ->> DB : queue repack request
    FE ->> Admin : ack
    deactivate Admin
    end
    rect rgba(255,255,255,0.1)
    Note right of DB: repack request<br>expansion
    activate MD
    MD ->> DB : dequeue repack request
    MD ->> MD : expand repack request
    loop for each file
    MD ->> DB : queue retrieve sub-request
    end
    deactivate MD
    end
    rect rgba(255,255,255,0.1)
    Note right of TD: Tape session pops<br>retrieve sub-requests<br>and reads file from<br/>tape to disk buffer
    activate TD
    TD ->> DB : select retrieve mount
    TD ->> TD : mount tape
    loop for each file
    TD ->> DB : dequeue retrieve sub-request
    TD ->> TD : read file from tape
    TD ->> Buffer : write file
    TD ->> DB : requeue successful and<br>failed retrieve sub-requests
    deactivate TD
    end
    end
    rect rgba(255,255,255,0.1)
    Note right of MD: retrieve sub-requests<br>reports are processed<br>and converted to<br>archive sub-requests
    activate MD
    loop for each files
    MD ->> DB : dequeue retrieve sub-requests
    MD ->> MD : update statistics and transform<br>into archive sub-request
    MD ->> DB : queue archive sub-request
    end
    deactivate MD
    end
    rect rgba(255,255,255,0.1)
    Note right of TD: Tape session pops<br>archive sub-requests<br>and writes file from<br/>disk buffer to tape
    activate TD
    TD ->> DB : select archive mount
    TD ->> TD : mount tape
    loop for each file
    TD ->> DB : dequeue retrieve sub-request
    TD ->> Buffer : read file
    TD ->> TD : write file to tape
    TD ->> DB : requeue successful and<br>failed archive sub-requests
    deactivate TD
    end
    end
    rect rgba(255,255,255,0.1)
    Note right of MD: archive sub-requests<br>reports are processed
    activate MD
    loop for each files
    MD ->> DB : dequeue archive sub-requests
    MD ->> MD : update statistics
    end
    Note right of MD: update repack status<br>after all sub-request have<br>succeeded or failed
    MD ->> DB : update repack request<br>with final status 
    deactivate MD
    end
```


## Recycle bin and reclamation

Metadata for replaced tape copies is retained in the [recycle bin](recycle-bin.md). Reclaiming the source tape is a separate operator action.

See [Repacking Tapes](../../ops/administration/repack.md) for prerequisites, commands, status checks, and cancellation.
