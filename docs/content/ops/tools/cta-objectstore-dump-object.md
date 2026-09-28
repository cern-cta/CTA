---
date: 2025-05-28
section: 1cta
title: CTA-OBJECTSTORE-DUMP_OBJECT
header: The CERN Tape Archive (CTA)
---
<!---
@project      The CERN Tape Archive (CTA)
@copyright    Copyright © 2020-2024 CERN
@license      This program is free software, distributed under the terms of the GNU General Public
              Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
              redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
              option) any later version.

              This program is distributed in the hope that it will be useful, but WITHOUT ANY
              WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
              PARTICULAR PURPOSE. See the GNU General Public License for more details.

              In applying this licence, CERN does not waive the privileges and immunities
              granted to it by virtue of its status as an Intergovernmental Organization or
              submit itself to any jurisdiction.
--->

# NAME

cta-objectstore-dump-object --- Fetch and print an object from the object store into the terminal

# SYNOPSIS

**cta-objectstore-dump-object** \[\--help] \[\--json] \[\--bodydump] \[objectstoreURL] *objectname*

# DESCRIPTION

**cta-objectstore-dump-object** is a command-line tool that will fetch an object from the object store and
print it in the terminal.

*objectname* is the reference to the object to be printed.

# OPTIONS

\--help

:   Display command options and exit.

\--json

:   Print the output in fully-compliant JSON format.

\--bodydump

:   Print only the contents of the object body (ignore header contents).

# EXIT STATUS

**cta-objectstore-dump-object** returns 0 on success.

# EXAMPLES

```bash
cta-objectstore-dump-object --json root
```

## Inspecting queues and requests

These examples apply to the objectstore scheduler. The tool uses the connection in `/etc/cta/cta-scheduler.conf` unless an explicit `objectstoreURL` is supplied. Run it with access to the intended backend.

`--json` includes the object header and decoded body. Add `--bodydump` to emit only the body for use with `jq`; no wrapper script is needed. Inspect the body before selecting fields, as different object types have different schemas.

To find the user retrieve queue for a tape:

```bash
vid='V12345'
cta-objectstore-dump-object --json --bodydump root |
  jq -r --arg vid "$vid" '.retrieveQueueToTransferForUserPointers[] | select(.vid == $vid) | .address'
```

Follow the returned queue address to its shards, then follow each shard to the retrieve requests:

```bash
queue='<queue-address>'
cta-objectstore-dump-object --json --bodydump "$queue" |
  jq -r '.retrievequeueshards[].address'

shard='<shard-address>'
cta-objectstore-dump-object --json --bodydump "$shard" |
  jq -r '.retrievejobs[].address'

request='<request-address>'
cta-objectstore-dump-object --json --bodydump "$request" |
  jq -r '.schedulerrequest.diskfileinfo.path'
```

To inspect work owned by an agent, follow the root's agent-register pointer:

```bash
cta-objectstore-dump-object --json --bodydump root |
  jq -r '.agentregisterpointer.address'

register='<agent-register-address>'
cta-objectstore-dump-object --json --bodydump "$register" | jq -r '.agents[]'

agent='<agent-address>'
cta-objectstore-dump-object --json --bodydump "$agent" | jq -r '.ownedobjects[]'
```

Agent names can help identify a drive process; inspect the selected agent before following its owned objects. Not every owned object is a retrieve request, so check its type before applying request-specific filters.

These reads do not lock the objects or provide a consistent snapshot of the whole store. An object may disappear between reads as work completes; a missing request during traversal does not by itself indicate corruption.

# SEE ALSO

CERN Tape Archive documentation [https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
