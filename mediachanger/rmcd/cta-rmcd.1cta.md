---
date: 2024-07-18
section: 1cta
title: CTA-RMCD
header: The CERN Tape Archive (CTA)
---
<!---
@project      The CERN Tape Archive (CTA)
@copyright    Copyright © 2020-2025 CERN
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

cta-rmcd --- CTA Remote Media Changer Daemon

# SYNOPSIS

systemctl start **cta-rmcd**\
systemctl stop **cta-rmcd**\
systemctl status **cta-rmcd**

# DESCRIPTION

**cta-rmcd** is the Remote Media Changer daemon, used to control SCSI-compatible tape libraries.

The **cta-taped** daemon requires that **cta-rmcd** is installed and running on the same tape
server as itself. **cta-rmcd** is usually started at system startup time by **systemd** or other
system service management software.

# CONFIGURATION

**cta-rmcd** reads its configuration from */etc/cta/cta-rmcd.toml*.
See */etc/cta/cta-rmcd.example.toml* for all available settings.

The media changer device is configured in the `media_changer` table:

> [media_changer]\
> device = "/dev/smc"

The RMC protocol listener is configured in the `rmc_server` table:

> [rmc_server]\
> port = 5014\
> listen_scope = "loopback"

The default port is 5014.
`listen_scope` may be set to `loopback` to accept connections only from the local host, or to `any` to accept connections from other hosts.
The RMC protocol has no authentication, so exposing it beyond the loopback interface requires appropriate network access controls.
Clients must be configured to use the same port as **cta-rmcd**.

# FILES

*/etc/cta/cta-rmcd.toml*

:   Main configuration file.

*/etc/cta/cta-rmcd.example.toml*

:   Example configuration documenting the available settings.

*/var/log/cta/cta-rmcd.log*

:   Log of error messages and statistical information. Log lines with
    code **RMC92** give information about the requestor: (uid,gid) and
    hostname. Log lines with code **RMC98** contain the command that was
    sent to the library. The exit status of each command is also logged.

# EXAMPLE

The packaged unit is installed as
*/usr/lib/systemd/system/cta-rmcd.service*. Local customizations should be
placed in */etc/systemd/system/cta-rmcd.service.d/*.conf* instead of editing
the packaged unit directly. The packaged unit contains:

    [Unit]
    Description=CERN Tape Archive (CTA) rmcd daemon
    Wants=network-online.target
    After=network-online.target

    [Service]
    Type=exec
    User=cta
    Group=tape
    RuntimeDirectory=cta/rmcd
    RuntimeDirectoryMode=0750
    LogsDirectory=cta cta/old
    LogsDirectoryMode=0755
    ExecStart=/usr/bin/cta-rmcd --config=/etc/cta/cta-rmcd.toml --config-strict --runtime-dir=/run/cta/rmcd --log-file=/var/log/cta/cta-rmcd.log
    LimitCORE=infinity
    OOMScoreAdjust=-10
    Restart=on-failure
    RestartSec=5
    TimeoutStopSec=60

    [Install]
    WantedBy=multi-user.target

Example excerpt from the **cta-rmcd** logfile:

    12/06 11:40:58  7971 rmc_srv_mount: RMC92 - mount request by 0,0 from tpsrv015.cern.ch
    12/06 11:40:58  7971 rmc_srv_mount: RMC98 - mount 000029/0 on drive 2
    12/06 11:41:08  7971 rmc_srv_mount: returns 0
    12/06 11:42:43  7971 rmc_srv_unmount: RMC92 - unmount request by 0,0 from tpsrv015.cern.ch
    12/06 11:42:43  7971 rmc_srv_unmount: RMC98 - unmount 000029 2 0
    12/06 11:42:48  7971 rmc_srv_unmount: returns 0

# SEE ALSO

**systemctl**(1)\
**cta-taped**(1cta)

CERN Tape Archive documentation [https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2025 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
