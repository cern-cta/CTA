# Tape Daemon Troubleshooting

Use drive down reasons and the failure scenarios below to diagnose why the tape daemon stopped and how work can resume.
For session summaries, counters, and hardware statistics, see [Tape Session Diagnostics](../run-and-maintain/monitoring/tape-session-diagnostics.md).
For the underlying state and ownership model, see [Tape Daemon Concepts](../../concepts/components/tape-daemon.md).
Startup policies and preparation settings are described in [Tape Daemon Configuration](../deploy-and-configure/configuration/tape-daemon.md#drive-startup-and-preparation).
Hardware procedures remain in [Tapes, Drives, and Libraries](../run-and-maintain/administration/tapes-and-drives.md).

## Drive down reasons

When desired `Down` is set, the **down reason** records why the drive is unavailable.
The reason accompanies the desired state, so it can be recorded while the daemon is still finishing the transition to reported `Down`.
It can describe an operator's intent, such as maintenance, or a condition detected by the daemon.

Optional details may be appended to these reasons to describe the underlying problem.

| Reason | When produced |
| --- | --- |
| `[cta-taped] Daemon started` | Daemon started and put itself Down at startup. |
| `[cta-taped] Daemon shutting down` | The daemon receives a stop request or exits normally, and no existing operator or failure reason needs to be preserved. |
| `[cta-taped] Tape detected in drive` | During the Down-to-Up transition, the drive probe found a tape. Drive cleaning on Up (`drive.clean_on_up`) was disabled, so the daemon did not attempt to remove it. |
| `[cta-taped] Drive probe failed` | During the Down-to-Up transition with drive cleaning disabled (`drive.clean_on_up`), discovering, opening, or probing the drive fails before the daemon can confirm it is empty. |
| `[cta-taped] Drive access failed` | The daemon could not discover, locate, or open the drive for a tape session. |
| `[cta-taped] Drive cleanup failed` | Drive cleanup fails. For example, the daemon cannot access or reset the drive, unload the tape, or return it to the library. |
| `[cta-taped] Drive reuse unsafe` | A tape session ends without confirming that another tape can safely be mounted, but no specific access or cleanup failure is available to report. This is a fallback; known drive-access and cleanup failures produce the corresponding reasons above. |
| `[cta-taped] Unexpected failure` | An unexpected fatal error in the daemon or drive session prevents the daemon from running further tape sessions and causes it to exit. This is not a clean shutdown. |

An existing operator or failure reason is preserved instead of being replaced by startup or shutdown messages.
With automatic desired `Up` disabled, a previous clean-shutdown reason is replaced by the startup reason when the daemon starts again.
The configurable policy is described under [Drive startup and preparation](../deploy-and-configure/configuration/tape-daemon.md#drive-startup-and-preparation).
The current unsafe-worker failure path exits immediately without publishing a reason or changing drive state.

## Tape daemon failure recovery

**Operator action** marks recovery steps that require intervention, such as correcting configuration, addressing a hardware problem, or requesting desired `Up`.
Daemon restarts may be performed by service supervision; a restart alone is not marked as manual intervention.

| Scenario | Daemon behavior | How work can resume |
| --- | --- | --- |
| **The configured drive name belongs to another host or logical library** | Rejects registration and exits without accessing hardware or changing the existing drive's state. | **Operator action:** Correct the conflicting configuration or catalogue registration, then restart the daemon. |
| **Registration fails, for example because the catalogue is unavailable** | Exits before starting a drive session. It does not attempt shutdown state publication or hardware cleanup; registration updates already made may remain. | Restart once the registration problem is resolved. Startup checks the catalogue state again. |
| **The configured logical library does not exist** | Waits for the library entry without starting drive sessions or accessing hardware. Absence alone is not fatal. | **Operator action:** Create the library entry; the running daemon can then continue startup. |
| **The drive's catalogue entry disappears** | Exits without recreating the entry during that run. | A later daemon run registers the drive again and applies the startup policy. |
| **The drive cannot be opened, reset, or emptied at the start of a drive session** | Sets desired `Down` and reports `Down`, preserving a specific failure reason. Does not schedule tape sessions. | **Operator action:** Resolve the underlying problem and request desired `Up`. A new drive session retries preparation as configured. |
| **A recoverable scheduling error or timeout occurs** | Waits and retries within the current drive session, without repeating the initial drive preparation. | Scheduling continues when the scheduler can supply work. No operator state change is required. |
| **A tape session cannot access the drive or leaves it unusable** | Sets desired `Down` and reports `Down`, recording or preserving the failure reason. | **Operator action:** Resolve the problem and request desired `Up`. |
| **Mounting or transferring fails, but cleanup leaves the drive reusable** | Completes cleanup and reports the failed work. The drive session can continue if desired `Up` remains set. | Further work can be scheduled. Recovery of failed file-transfer jobs is separate from recovery of the drive. |
| **An unexpected tape session failure occurs after its transfer threads have stopped** | Attempts recovery cleanup while the drive session still owns the hardware, including when desired `Down` has been requested and regardless of the initial cleanup policy. | Successful recovery with continued desired `Up` permits scheduling to resume. If desired `Down` is observed, the drive session ends after cleanup. |
| **Cleanup fails, including a stuck cartridge** | Sets desired `Down` and reports `Down`; no further tape sessions are scheduled. A cartridge may remain loaded. | **Operator action:** Address the hardware or cleanup problem, then request desired `Up`. |
| **An exception escapes worker startup or joining** | Logs the failure and exits immediately without unwinding, recovery cleanup, or publishing desired/reported drive state. Worker termination is treated as uncertain; the last recorded state remains and a cartridge may be loaded. | The old process must end before a replacement takes control. Restart using the [startup recovery policy](../deploy-and-configure/configuration/tape-daemon.md#drive-startup-and-preparation); preparation follows the configured cleanup policy. |
| **The process crashes or is abruptly terminated before shutdown runs** | Cannot finish cleanup or publish shutdown state. It leaves the last successfully recorded desired state unchanged, which is normally desired `Up` during an active drive session. The reported state may be stale and a cartridge may remain loaded. | Restart the daemon. Resuming an existing desired `Up` requires startup recovery to be enabled; preparation follows the configured cleanup policy described in [Drive startup and preparation](../deploy-and-configure/configuration/tape-daemon.md#drive-startup-and-preparation). |

