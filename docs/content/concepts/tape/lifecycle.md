# Tape Lifecycle

## Introduction

In CTA, each tape information is stored in the **CTA Catalogue** and are managed by the mean of `cta-admin` command.

## Different tape states and supported state transitions

![Tape state diagram](tape_state_diagram.svg "Tape state diagram")

### What can be done on each final state

State | Queue *user* read requests | Queue *user* write requests | Queue *repack* requests | Queue *repack* read sub-requests (\*) | Queue *repack* write sub-request(*) | Mountable | Reclaimable
---                 |---------------------------|-----------------------------|-------------------------|---------------------------------------|-------------------------------------|-----------| ---
ACTIVE              | YES                       | YES                         | NO                      | NO                                    | YES                                 | YES       | YES
DISABLED            | YES                       | YES                         | NO                      | NO                                    | YES                                 | NO        | YES
REPACKING           | NO                        | NO                          | YES                     | YES                                   | NO                                  | YES       | NO
REPACKING_DISABLED  | NO                        | NO                          | YES                     | YES                                   | NO                                  | NO        | NO
BROKEN              | NO                        | NO                          | NO                      | NO                                    | NO                                  | NO        | YES
EXPORTED            | NO                        | NO                          | NO                      | NO                                    | NO                                  | NO        | NO

(*) Repack sub-request queueing is handled by the maintenance daemon.<br>

### Tape states explained

!!! info
    Changing tapes from `ACTIVE`/`DISABLED` requires user operations to be enabled on the CTA Frontend. \
    Changing tapes from `REPACKING`/`REPACKING_DISABLED` requires repack operations to be enabled on the CTA Frontend.


- **ACTIVE:**
    - A tape that is `ACTIVE` is a tape that is in good condition to allow a user to read data from it or to write data to it. A newly added tape to CTA will be `ACTIVE` by default.
- **DISABLED:**
    - A tape that is `DISABLED` cannot be mounted, but can still have retrieve requests queued to it.
    - Can be set for the following reasons:
        - A tape daemon disabled it after encountering some issue that lead to failure (example: failed dismount).
        - Monitoring probes decided to disable it (too many errors over the past XX hours,...).
        - An operator think the tape is in bad shape and must be kept away from users while it is investigated.
    - A tape should not stay in this state for more than 1 week, person on rota to follow up on tapes disabled for longer than one week.
- **REPACKING:**
    - A tape should be moved to `REPACKING` state before the operator submits a repack request. Otherwise, the repack request won't be accepted.
    - Likewise, it's not possible to move out of `REPACKING` while a repack request is ongoing (except for the `REPACKING_DISABLED` state).
    - When a change from `ACTIVE`/`DISABLED` to `REPACKING` is requested, the tape will first move to the temporary state `REPACKING_PENDING`. Then, it waits for the maintenance daemon to clean all user requests on the tape queue, before finally moving it to `REPACKING` state. For more details see: [Queue Cleanup](../components/maintenance-daemon.md#queue-cleanup).
- **REPACKING_DISABLED:**
    - The state `REPACKING_DISABLED` is similar to `DISABLED`, but for repacking tapes (we can't move out of repacking states while a repack is ongoing).
    - A tape should not stay in this state for more than 1 week, person on rota to follow up on tapes disabled for longer than one week.
    - New repack requests can be queued on a `REPACKING_DISABLED` tape. However, no tape will be mounted while it's on this state.
- **BROKEN:**
    - A tape can stay in this state for long and it is very likely that it is its very last state.
    - When a change from `ACTIVE`/`DISABLED` to `BROKEN` is requested, the tape will first move to the temporary state `BROKEN_PENDING`. Then, it waits for the maintenance daemon to clean all user requests on the tape queue, before finally moving it to `BROKEN` state. This guarantees that all requests are properly disposed of.
    - Can be set for the following reasons:
        - A problematic (earlier `DISABLED`) tape that requires non-trivial efforts for its data to be recovered:
            - low level slow tape extract is needed
            - sent for data recovery to the vendor = the tape is not present in the library
            - all recovery attempts exhausted, the tape is permanently broken but experiment action is needed (delete the lost files from the catalogs)
        - In rare cases an `ACTIVE` tape that fall on the floor because of a gripper incident can move from `ACTIVE` straight to `BROKEN` as it must be physically put back in a slot.
    - Normally, very few tapes are in the BROKEN state.
    - No operations are allowed for a `BROKEN` tape.
- **EXPORTED:**
    - A tape can stay in this state for long. It means that the tape was removed from the tape library.
    - While we at CERN we do not remove tape cartridges from tape libraries, other sites do. Therefore, the `EXPORTED` state should help to distinguish that.
    - This state would behave very similarly to `BROKEN` state. The difference is on the error messages reported to the user.
