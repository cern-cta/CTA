# CTA Tape Pool Supply mechanism

This tool moves tapes from the configured supply tape pools into various user pools.
The purpose of this is to allow for a set of common 'supply' pools to gradually feed into a set of pools actively used by the drives, thereby keeping the number of tapes being written to at any given time to a minimum, to reduce the amount of data spread.

## Installation

### Requirements

This tool is intended to be run periodically using an external scheduler such as Cron or Rundeck.

### Configuration

```yaml
tools:
  # -------------------------------
  # CTA Pool Supply
  # -------------------------------
  cta-ops-pool-supply:
    debug: false
    timeout: 10
    separator: ","
```

### Logic

The script is using the following two tape pool parameters:

* number of partial (i.e. not full) tapes (column: #partial)
* comma separated list of supply tape pools (column: supply)

Subsequently, the following rules apply (as per selected option 1 which is in use):

* CTA tape pools should have at least *N* partial tapes available for writing.
* Eligible partial tapes are those that are ACTIVE (i.e. not DISABLED nor BROKEN), not FULL, not FROM CASTOR and not in a DISABLED logical tape library.
* If a tape is not completely empty, but not yet FULL it is considered as eligible for writing so no new tape will be added until all non full tapes are consumed.
* This means that number of parallel write streams is controlled by two values: Number of available partial tapes, and maximum number of write drives per VO. However, the number of parallel write streams can not exceed the number of eligible available tapes even if the value of maximum write drives is higher.
* If the number of eligible partial tapes of a given tape pool falls below the configured limit, the pool needs to be re-supplied with fresh tapes.
* Fresh supply tapes are taken from tape pools defined in the "supply" column.
* There can be multiple supply tape pools and they can be separated by a separator (usually comma).
* There is no distinction between what is a supply pool and what is not, if a pool has a value in the "supply" column, tapes are taken from that pool. Because of this, irregularities, cyclical loops and other misconfiguration are possible - *please be careful*.
* This script is intended to run every 15 minutes using a scheduler such as Cron or Rundeck.

Considered but not used option 2 could do this:

* The eligible tapes selected by the above criteria are checked whether they already have some data (the following `cta-admin --json tape ls` output values: `occupancy`, `lastFseq`, `nbMasterFiles` and `masterDataInBytes` must all be equal to 0).
* If a tape is not completely empty, it is not considered as eligible so a new completely empty tape should instead be added to that tape pool.
* This means that is is enough to have the tape pool value of partial tapes set to 1 and then the new tapes will be added to that pool as it is being written to (with every invocation of the supply logic script).
* It also means that allocation of empty tapes into a tape pool will only stop when the MAX WRITE DRIVES VO + 1 value is reached.

### Known limitations of this system:

* The supply logic script does not consider the current queued load of a logical library as it is too dynamic versus the physical allocation of tape cartridges in tape libraries.
* If a tape pool is configured to use tapes from logical library X and all drives are currently busy, the writing will have to wait until all partial tapes are consumed. But even after they are consumed the automatic allocation of next free tape will not look at the current load, but only consider the supply pool as it is configured.
* In exceptional cases when this tape allocation mechanism is not providing enough tapes from tape libraries with enough free drives and the data that should be archived starts to pile up, manual re-allocation of tapes might be required.
* This approach does not allow any distinction between two tape pools owned by the same VO. One cannot assign more write output streams to one tape pool than the other because the maximum number of write drives is an attribute of a VO.
* If the single empty tape is somehow unavailable but still considered eligible, the supply mechanism is not able to detect that and will not assign additional tapes.
