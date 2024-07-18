---
date: 2024-07-18
section: 1cta
title: CTA-RMCD
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

cta-rmcd --- CTA Remote Media Changer Daemon

# SYNOPSIS

systemctl start **cta-rmcd** *device_file*\
systemctl stop **cta-rmcd**\
systemctl status **cta-rmcd**

# DESCRIPTION

**cta-rmcd** is the Remote Media Changer daemon, used to control
SCSI-compatible tape libraries.

The **cta-taped** daemon requires that **cta-rmcd** is installed and
running on the same tape server as itself. **cta-rmcd** is usually
started at system startup time by **systemd** or other system service
management software.

# CONFIGURATION

The port number that **cta-rmcd** will listen on should be defined on
client hosts and on the tapeserver host. The default port number is
5014. It is possible to configure a different port number in
*/etc/services*:

> rmc 657/tcp \# CTA Remote Media Changer (cta-rmcd)\
> rmc 657/udp \# CTA Remote Media Changer (cta-rmcd)

This value can be overridden in */etc/cta/cta-rmcd.conf*:

> RMC PORT 5014

It can also be set in the **RMC_PORT** environment variable.

# ENVIRONMENT

**RMC_PORT**

:   Sets the port number on which **cta-rmcd** will listen.

# FILES

**/etc/cta/cta-rmcd.conf**

:   Configuration file. See **CONFIGURATION** above, and
    */etc/cta/cta-rmcd.conf.example*.

**/var/log/cta/cta-rmcd.log**

:   Log of error messages and statistical information. Log lines with
    code **RMC92** give information about the requestor: (uid,gid) and
    hostname. Log lines with code **RMC98** contain the command that was
    sent to the library. The exit status of each command is also logged.

# EXAMPLE

Example configuration of */etc/systemd/system/cta-rmcd.service*:

    [Unit]
    Description=CERN Tape Archive (CTA) rmcd daemon
    After=syslog.target network-online.target
   
    [Service]
    User=cta
    EnvironmentFile=-/etc/sysconfig/cta-rmcd
    ExecStart=/usr/bin/cta-rmcd ${CTA_RMCD_OPTIONS}
    LimitCORE=infinity
    Type=forking
    Restart=no
   
    [Install]
    WantedBy=default.target

Example configuration of */etc/sysconfig/cta-rmcd*:

    DAEMON_COREFILE_LIMIT=unlimited
    CTA_RMCD_OPTIONS=/dev/smc

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

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
