# Tape Server Setup

See [Tape Server Concepts](../../concepts/tape/servers.md) for the role of a tape server.

## Configuring a tape server

### Configuring correct fibre channel topology of the Emulex HBA

Some tape servers are equipped with Emulex HBA:

```
[root@tpsrv600 ~]# lspci -v | grep -i fibre
81:00.0 Fibre Channel: Emulex Corporation LPe31000/LPe32000 Series 16Gb/32Gb Fibre Channel Adapter (rev 01)
        Subsystem: Emulex Corporation LPe31002-M6 2-Port 16Gb Fibre Channel Adapter
81:00.1 Fibre Channel: Emulex Corporation LPe31000/LPe32000 Series 16Gb/32Gb Fibre Channel Adapter (rev 01)
        Subsystem: Emulex Corporation LPe31002-M6 2-Port 16Gb Fibre Channel Adapter
```

By default, these HBA are configured with _Point-to-Point (PTP)_ fibre channel topology. With our direct fibre optical cable between the HBA of the tape server and the tape drive, this needs to be changed to _Loop_ otherwise the tape server can not see the connected tape drives.

To do that _Emulex HBA Manager Core Application Kit (CLI) for Linux - RHEL on x86_64 Architecture_ is needed. At the time of writing this documentation (April 2024) it was available under this link: https://docs.broadcom.com/docs/elx_elxocmcore-rhel8-rhel9-14.2.673.33-1.tgz

From that TAR file, this RPM can be extracted:

```
-rw-r--r--. 1 root root 1335578 Nov 11 05:58 /root/elxocmcore-rhel8-rhel9-14.2.673.33-1/x86_64/rhel-9/elxocmcorekit-14.2.673.33-1.rhel9.x86_64.rpm
```

Once this RPM is installed on a given tape server with the Emulex HBA, use this command to see the current topology:

```
[root@tpsrv601 ~]# for i in $(cat /sys/class/fc_host/*/port_name); do wwn=$(echo ${i:2} | sed -e "s/[0-9A-Fa-f]\{2\}/&:/g" -e "s/:$//"); /opt/emulex/ocmanager/bin/hbacmd getfwparams $wwn; done

FW Params for 10:00:70:b7:e4:15:6d:11 

FX                 Param      Low     High      Def      Cur      Dyn
00:              FA-PWWN        0        1        0        0        5
01:              16G-FEC        0        1        1        1        1
02:        DYNAMIC-DPORT        0        1        1        1        1
08:             TOPOLOGY        2        3        3        3        5

Param Name           Value
FA-PWWN              {0=Disable, 1=Enable}
16G-FEC              {0=Disable, 1=Enable}
DYNAMIC-DPORT        {0=Disable, 1=Enable}
TOPOLOGY             {2=Loop, 3=PTP}


FW Params for 10:00:70:b7:e4:15:6d:12 

FX                 Param      Low     High      Def      Cur      Dyn
00:              FA-PWWN        0        1        0        0        5
01:              16G-FEC        0        1        1        1        1
02:        DYNAMIC-DPORT        0        1        1        1        1
08:             TOPOLOGY        2        3        3        3        5

Param Name           Value
FA-PWWN              {0=Disable, 1=Enable}
16G-FEC              {0=Disable, 1=Enable}
DYNAMIC-DPORT        {0=Disable, 1=Enable}
TOPOLOGY             {2=Loop, 3=PTP}
```

Notice the _Cur_ column, the current topology is set to _3=PTP_. To set it to _2=Loop_, use this command:

```
[root@tpsrv601 ~]# for i in $(cat /sys/class/fc_host/*/port_name); do wwn=$(echo ${i:2} | sed -e "s/[0-9A-Fa-f]\{2\}/&:/g" -e "s/:$//"); /opt/emulex/ocmanager/bin/hbacmd setfwparam $wwn TOPOLOGY 2; done

Set FW Parameter TOPOLOGY=2 for 10:00:70:b7:e4:15:6d:11
Reset adapter port to activate new firmware parameter setting.

Set FW Parameter TOPOLOGY=2 for 10:00:70:b7:e4:15:6d:12
Reset adapter port to activate new firmware parameter setting.
```

Once this is done, reboot the tape server and it should now be able to see the connected tape drives.

## Common tasks

### Mounting and Unmounting a tape


### Set up encryption

## Commissioning hardware

Commission tape servers, drives, and libraries for CTA.

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

### Prepare the hardware

Document prerequisites and checks before connecting hardware. See [Tape Server Setup](tape-servers.md).

### Register and configure

Document device discovery, logical-library and drive configuration, and catalogue registration.

### Validate and enable

Document read/write validation and the checks required before enabling production traffic.

### Multiple libraries and media changers

Document the mapping from each library control path through the media changer to CTA logical libraries and drive entries in TPCONFIG. Include drive ordinals, device discovery, and validation that mounts address the intended library. See the [media-changer option reference](../tools/service-manuals/cta-rmcd.md).

### Retrieval ordering

Include [RAO prerequisites and verification](../configuration/tape-daemon.md#recommended-access-order) when commissioning new drives and media types.
