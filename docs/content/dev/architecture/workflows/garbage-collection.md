!!! warning "Deprecated"
    This page is deprecated and may contain information that is no longer up to date.

# Garbage Collection

Tape files on archive/retrieve buffer disks are garbage collected using three different garbage collector mechanisms.
Each of these is encapsulated by a TGC.

In the config examples we assume that there is an archive buffer `default` and a separate retrieve buffer called either `retrieve` if using SSDs, or `spinners` if using HDDs.

## Archive File TGC

The archive file TGC cleans up files in the archive buffer after they have been successfully archived to tape.

### Configuration

Enable the AF TGC for the `default` space using:

```
eos space config default space.filearchivedgc=on
```

## MGM TGC

The MGM TGC is part of the EOS MGM daemon and deletes files once the available space is low, making sure that the buffer space never completely fills up.
Files to delete are selected by LRU order.
This data structure is effectively made persistent by the fact that it its reconstructed from QuarkDB when the the MGM daemon is restarted.
The reconstruction is not perfect but is an approximation of the LRU.
The MGM TGC is the main TGC to be used in EOS spaces where users wish to recall files but never explicitly evict them from disk.
It should normally only be configured to run against the `spinners` space.

### Configuration

The MGM TGC needs be configured in two places, namely within the static `/etc/xrd.cf.mgm` file and within the dynamic the EOS namespace.
The “switch it on” part of the configuration is within the `/etc/xrd.cf.mgm` file in order to protect a disk only EOS instance from accidentally having an MGM TGC running.

Both of the following lines need to be added to the `/etc/xrd.cf.mgm` configuration file of the instance:

```
mgmofs.tapeenabled true
mgmofs.tgc.enablespace spinners
```

Garbage collection of the `spinners` space should be configured within the EOS namespace as follows:

```
eos space config spinners space.tgc.availbytes=<X>TB
eos space config spinners space.tgc.qryperiodsecs=60
eos space config spinners space.tgc.totalbytes=<Y>TB
```

**space.tgc.availbytes:**
Specifies the threshold where the MGM TGC considers there to be enough free/available space.
The MGM TGC will not attempt to garbage collect any files if the actual amount of free/available space is above this number.
We recommend setting this value to something around 5% - 20% of the total available space.

**space.tgc.qryperiodsecs:**
Specifies the period at which the MGM TGC should query for statistics about the EOS space being managed.
This value should be twice the value of the `EOS_FST_DELETE_QUERY_INTERVAL` environment variable set within the `/etc/sysconfig/eos_env` file of each FST daemon and 5 seconds greater than the value of `publish.interval` of each EOS node/FST in the spinners space.

**space.tgc.totalbytes:**
Specifies the number of storage bytes that must be available to the MGM for read/write access before the MGM TGC can even begin to take action.
This parameter solves a “startup” problem:
Once an MGM XRootD process is started, the MGM TGC must not immediately start considering files for garbage collection because FSTs will not have had time to register their free/available space and the MGM TGC will see too little free/available space and will start prematurely and incorrectly garbage collecting.

## FST TGC

The FST TGC is slow and incredibly simple.
It has no LRU data structure in its memory.
In fact one could say it has no data structure to model the metadata of the files it is garbage collecting.
The FST TGC simply travels though the local disk of the FST daemon and if free space is too low then it tries to garbage collect any file it comes across that is deemed “too old”.
The meaning of too old is configured in the `/etc/cta/cta-fst-gcd.conf` file.
The main purpose of the FST CGD is to garbage collect files on individual eos filesystems for different use cases:

### GC the `retrieve` space of a *buffer-only* eoscta instance

In this case all the files recalled from tapes are written in the `retrieve` space, transferred out to their destination and finally evicted.
In theory after this workflow nothing should remain in the `retrieve` space.
In practice we regularly see forgotten files that have not been evicted and no one will transfer out of the buffer and evict.
In this case the FST GCD will remove forgotten files from the FSTs, ensuring that only actively transferring files are consuming space in the `retrieve` buffer.

### Supplement the MGM TGC

In the `spinners` space it will remove files missed by the MGM TGC.
In this case the FST TGC could be considered a safety net.

### Balance available free space between FS

The main purpose of the FST GCD is to make sure that every single disk used in the system has at least `eos_space_to_min_free_bytes` of free space.
In a given disk file distribution the following situation can happen: out of the huge amount of disks in the system only the MGM TGC could free up some space on a limited set of FS.
In this case only a very limited set of disks have some free space while all the other are full.
The consequence on the eoscta instance is really bad as users or tape would then only be able to write on the few disks that have some free space and the bandwidth would be highly affected.
The FST GCD should therefore be configured correctly with the MGM TGC to ensure that the MGM TGC is given an opportunity to garbage collect some files.

For example:

If your system counts 100 FS and FST TGC `eos_space_to_min_free_bytes` is set to 50GB, the combined action of all FST GCD will already free 5TB in the system. In this case is the MGM TGC is configured to free up anything bellow 5TB it will never kick in.

**MGM TGC should be configured to free at least 2 times the sum of `eos_space_to_min_free_bytes` on all the filesystems in the `spinners` space.**

### Configuration

The FST TGC should be configure using the following values in the `/etc/cta/cta-fst-gcd` configuration file:

```
[main]
log_file = /tmp/cta-fst-gcd.log ; Path of garbage collector log file
mgm_host = eosctaalice.cern.ch ; Fully qualified host name of EOS MGM
eos_spaces = spinners ; Space separated list of the names of the EOS spaces to be garbage collected
eos_space_to_min_free_bytes = spinners:20000000000 ; Minimum number of free bytes a filesystem should have -> 20GB
gc_age_secs = 604800 ; Age at which a file can be considered for garbage collection -> 7 days
absolute_max_age_secs = 315360000 ; Age at which a file will be considered for garbage collection no matter the amount of free space -> 10 years
query_period_secs = 310 ; Delay in seconds between free space queries to the local file systems
main_loop_period_secs = 300 ; Period in seconds of the main loop of the cta-fst-gcd daemon
xrdsecssskt = /etc/eos.keytab ; Path to simple shared secret to authenticate with EOS MGM
```

## Examples

### Garbage collection for the EOSCTA ALICE instance

#### AF TGC

```
# EOS namespace config
eos space config default space.filearchivedgc=on
```

#### MGM TGC

```
# /etc/xrd.cf.mgm
mgmofs.tapeenabled true
mgmofs.tgc.enablespace spinners
```

```
# EOS namespace config
eos space config spinners space.tgc.availbytes=20T
eos space config spinners space.tgc.qryperiodsecs=60
eos space config spinners space.tgc.totalbytes=100T
```

#### FST TGC

```
# /etc/cta/cta-fst-gcd
[main]
log_file = /tmp/cta-fst-gcd.log ; Path of garbage collector log file
mgm_host = eosctaalice.cern.ch ; Fully qualified host name of EOS MGM
eos_spaces = spinners ; Space separated list of the names of the EOS spaces to be garbage collected
eos_space_to_min_free_bytes = spinners:20000000000 ; Minimum number of free bytes a filesystem should have -> 20GB
gc_age_secs = 604800 ; Age at which a file can be considered for garbage collection -> 7 days
absolute_max_age_secs = 315360000 ; Age at which a file will be considered for garbage collection no matter the amount of free space -> 10 years
query_period_secs = 310 ; Delay in seconds between free space queries to the local file systems
main_loop_period_secs = 300 ; Period in seconds of the main loop of the cta-fst-gcd daemon
xrdsecssskt = /etc/eos.keytab ; Path to simple shared secret to authenticate with EOS MGM
```
