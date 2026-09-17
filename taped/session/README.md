# Transfer lifecycle and outcome logging

`DataTransferSession::execute()` begins tracking one acquired mount and resets the previous transfer's statistics, outcome, and completion flags.
Scheduling and standalone drive cleaning do not change transfer state.
The tracker has no transfer state before the first transfer starts and retains the last transfer's state until the next transfer begins.

The `transferState` log field describes progress independently of `DriveStatus`:

| State | Meaning |
|---|---|
| `Preparing` | Preparing jobs, workers, and tape access |
| `Transferring` | Processing tape transfer tasks |
| `Finalizing` | Cleaning up and completing outstanding reporting |
| `DrainingToDisk` | Retrieval tape work has ended while disk delivery remains active |
| `Finished` | The session owner has joined all started transfer and job-reporting workers |

An uninitialized transfer state is logged as JSON null.
`DrainingToDisk` returns to `Finalizing` when the last disk worker finishes; only the session owner establishes `Finished`.
Handled failures and empty mounts can finish without passing through every phase.

Periodic statistics use `status="in_progress"`, including when errors have already occurred; the error counters describe those failures.
The final `tape_session_finished` event uses `status="success"` or `status="failure"` and `transferState="Finished"`.
The existing automatic error and tape-alert classification is retained, including the explicit success override for an empty mount.
An explicit failure cannot be overwritten by a later success assignment.
Final statistics publication is attempted before the final event, so publication failure is included in its outcome.

Log consumers must migrate from `sessionState` to `transferState`, use the state vocabulary above, and accept `in_progress` in periodic statistics.
Final-event consumers should continue using `status` to distinguish success from failure.
Drive-status values and scheduling semantics are unchanged.

General exception-time shutdown of partially started workers and reporters remains a separate repair.
Escaping session exceptions record failure and preserve the last phase; they do not establish completion.
Stopping a reporter without `Finished` does not emit a final session event.
