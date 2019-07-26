#!/bin/bash
#
# This script configures SCSI mode pages of various tape drive models.
#
# See additional documentation at the end of this script.
#
# Author: Vladimir Bahyl - 3/2016
#
# NOTE: This script is CERN specific

echo "[INFO] Executing $0 ..."

# Check the number of command line arguments
if [ $# -ne 1 ]
then
  echo
  echo "[ERROR] Invalid number of arguments. Please specify which device to configure."
  echo
  echo "Usage: $0 <tape drive device>"
  echo
  exit -1
fi

# It happens that after a reboot, the device names are created only later so tapeserverd does not want to start
# This hook calls the device namer everytime
DEVICENAMER="/usr/local/sbin/tape-devices-namer"
if [ -x "$DEVICENAMER" ]
then
  echo "[INFO] Calling $DEVICENAMER to make sure the device links are created properly ..."
  $DEVICENAMER
fi

# Extract the name of the st driver device from the command line parameter
STDEVICE=`/bin/readlink --canonicalize $1 | /bin/sed -e 's;^/dev/nst;/dev/st;'`

if [ ! -c "$STDEVICE" ]
then
  echo "[ERROR] Specified device $1 does not resolve to a character device using the st driver. Resolved value: $STDEVICE"
  exit -1
fi

# Get the sg driver SCSI generic device to use and identify the tape drive model
MODELANDSGDEVICE=`/usr/bin/lsscsi --generic | /bin/grep tape | /bin/grep $STDEVICE | /bin/awk '{print $4,$7}'`

# Extract the sg driver device name
SGDEVICE=`/bin/echo $MODELANDSGDEVICE | /bin/cut -f2 -d' '`
#echo "[INFO] device: $SGDEVICE"
if [ ! -c "$SGDEVICE" ]
then
  echo "[ERROR] Specified device $1 does not resolve to a character tape device using the sg driver. Resolved value: $SGDEVICE"
  exit -1
fi

# Extract the tape drive model
MODEL=`/bin/echo $MODELANDSGDEVICE | /bin/cut -f1 -d' '`
#echo "[INFO] model: $MODEL"

echo "[INFO] Model: $MODEL - Device: $SGDEVICE"

# Depending on the tape drive model, configure different SCSI mode pages. Overwrite whole pages to ensure all tape drives have the same settings.
case "$MODEL" in
  ULT3580-TD8)
    echo "[INFO] LTO-8 (ULT3580-TD8) tape drive. Model: $MODEL"

    # Disable SkipSync - byte 4, bit 0 = 0
    # SCSI mode page 0x30, subpage 0x40
    /usr/bin/sg_wr_mode --page=0x30,0x40 --contents=f0,40,00,10,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Disabled SkipSync on LTO-8 (ULT3580-TD8) tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    ;;
  03592E08)
    echo "[INFO] IBM TS1150 (03592E08) tape drive. Model: $MODEL"

    # Set the compression ON
    # SCSI mode page 0x10
    /usr/bin/sg_wr_mode --page=0x10 --contents=90,0e,00,00,00,00,00,14,c0,00,18,00,00,00,01,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Compression enabled on IBM TS1150 (03592E08) tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    # Use all available space on the tape cartridge = write until the physical end of tape
    # Disable FastSync (i.e. Virtual Backhitch) - byte 8, bit 7 = 1
    # Disable Skipsync - byte 8, bit 6 = 1
    # SCSI mode page 0x25
    /usr/bin/sg_wr_mode --page=0x25 --contents=25,1e,00,07,00,01,00,00,c0,00,55,00,00,00,00,00,00,00,00,00,00,00,28,00,00,00,00,00,00,00,00,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Enabled writing until the physical end of tape on IBM TS1155 (03592E08) tape drive."
      echo "[INFO] Disabled FastSync (i.e. Virtual Backhitch) feature on IBM TS1150 (03592E08) tape drive."
      echo "[INFO] Disabled SkipSync feature on IBM TS1150 (03592E08) tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    # Disable Archive Unload (as of microcode version 488C)
    # IBM: Archive Unload mode default is ON. To turn off Archive mode, set the E_ARCHIVE bit of Mode Page 30h subpage 43h to 0b. Succinctly, set bit 1 of Byte 4 of mode page 30h subpage 43h to 0b.
    # SCSI mode page 0x30, subpage 0x43
    /usr/bin/sg_wr_mode --page=0x30,0x43 --contents=f0,43,00,10,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Disabled Archive Unload mode on IBM TS1150 (03592E08) tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    ;;
  0359255F)
    echo "[INFO] IBM TS1155 (0359255F) tape drive. Model: $MODEL"

    # Set the compression ON
    # SCSI mode page 0x10
    /usr/bin/sg_wr_mode --page=0x10 --contents=90,0e,00,00,00,00,00,14,c0,00,18,00,00,00,01,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Compression enabled on IBM TS1155 (0359255F) tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    # Use all available space on the tape cartridge = write until the physical end of tape
    # Disable FastSync (i.e. Virtual Backhitch) - byte 8, bit 7 = 1
    # Disable Skipsync - byte 8, bit 6 = 1
    # SCSI mode page 0x25
    /usr/bin/sg_wr_mode --page=0x25 --contents=25,1e,00,07,00,01,00,00,c0,00,55,00,00,00,00,00,00,00,00,00,00,00,28,00,00,00,00,00,00,00,00,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Enabled writing until the physical end of tape on IBM TS1155 (0359255F) tape drive."
      echo "[INFO] Disabled FastSync (i.e. Virtual Backhitch) feature on IBM TS1155 (0359255F) tape drive."
      echo "[INFO] Disabled SkipSync feature on IBM TS1155 (0359255F) tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    # Disable Archive Unload (as of microcode version 488C)
    # IBM: Archive Unload mode default is ON. To turn off Archive mode, set the E_ARCHIVE bit of Mode Page 30h subpage 43h to 0b. Succinctly, set bit 1 of Byte 4 of mode page 30h subpage 43h to 0b.
    # SCSI mode page 0x30, subpage 0x43
    /usr/bin/sg_wr_mode --page=0x30,0x43 --contents=f0,43,00,10,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Disabled Archive Unload mode on IBM TS1155 (0359255F) tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    ;;
  0359260F)
    echo "[INFO] IBM TS1160 (0359260F) tape drive. Model: $MODEL"

    # Set the compression ON
    # SCSI mode page 0x10
    /usr/bin/sg_wr_mode --page=0x10 --contents=90,0e,00,00,00,00,00,14,c0,00,18,00,00,00,01,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Compression enabled on IBM TS1160 (0359260F) tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    # Use all available space on the tape cartridge = write until the physical end of tape
    # Disable FastSync (i.e. Virtual Backhitch) - byte 8, bit 7 = 1
    # Disable Skipsync - byte 8, bit 6 = 1
    # SCSI mode page 0x25
    /usr/bin/sg_wr_mode --page=0x25 --contents=25,1e,00,07,00,01,00,00,c0,00,55,00,00,00,00,00,00,00,00,00,00,00,28,00,00,00,00,00,00,00,00,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Enabled writing until the physical end of tape on IBM TS1160 (0359260F) tape drive."
      echo "[INFO] Disabled FastSync (i.e. Virtual Backhitch) feature on IBM TS1160 (0359260F) tape drive."
      echo "[INFO] Disabled SkipSync feature on IBM TS1160 (0359260F) tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    ;;
  T10000D)
    #echo "[INFO] Oracle T10000D tape drive. Model: $MODEL"
    # Set the compression ON
    # SCSI mode page 0x10
    /usr/bin/sg_wr_mode --page=0x10 --contents=10,0e,00,00,00,00,00,64,50,00,18,00,00,00,01,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Compression enabled on Oracle T10000D tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    # Use all available space on the tape cartridge = write until the physical end of tape
    # Disable File Sync, Large File and Tape Application Accelerators
    # SCSI mode page 0x25
    /usr/bin/sg_wr_mode --page=0x25 --contents=25,1e,00,00,00,01,00,00,c0,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,00 $SGDEVICE
    if [ "$?" -eq 0 ]
    then
      echo "[INFO] Enabled writing until the physical end of tape on Oracle T10000D tape drive."
      echo "[INFO] Disabled File Sync, Large File and Tape Application Accelerators on Oracle T10000D tape drive."
    else
      echo "[ERROR] Execution of /usr/bin/sg_wr_mode command on $SGDEVICE failed."
      exit -1
    fi

    ;;
  *)
    echo "[ERROR] Unexpected tape drive model: $MODEL"
esac

exit 0 # Exit without error so the tapeserverd can start normally


# ********************************************************************************************************************
# * Explanation how to modify SCSI mode pages using /usr/bin/sg_wr_mode command (examples mostly use IBM tape drive) *
# ********************************************************************************************************************
#
# 1/ Use sdparm command to see the current/changeable/default/saved values of a page:
#
# [root@tpsrv ~]# sdparm --page=0x10 --hex /dev/tape
# Device configuration (SSC) [0x10] mode page:
#     Current:
#  00     90 0e 00 00 00 00 00 14  c0 00 18 00 00 00 01 00
#     Changeable:
#  00     90 0e 00 00 00 00 00 00  02 00 00 00 00 00 ff 27
#     Default:
#  00     90 0e 00 00 00 00 00 14  c0 00 18 00 00 00 01 00
#     Saved:
#  00     90 0e 00 00 00 00 00 14  c0 00 18 00 00 00 01 00
#
# 2/ The above 0x10 SCSI mode page controls tape drive compression. The changeable bytes are marked by the 'ff' characters in the Changeable section.
#
# 3/ To rewrite completely the 0x10 page (for example to ensure that all tape drives have the same settings), one can simply take the whole page and send it to the drive (for example using the values from the Default section):
# [root@tpsrv ~]# /usr/bin/sg_wr_mode --page=0x10 --contents=90,0e,00,00,00,00,00,14,c0,00,18,00,00,00,01,00 /dev/tape
#
# 4/ Overwriting anything else not marked with the 'ff' characters seems to be failing:
# [root@tpsrv ~]# /usr/bin/sg_wr_mode --page=0x10 --contents=91,0e,00,00,00,00,00,14,c0,00,18,00,00,00,01,00 /dev/tape
# contents page_code=0x11 but reference page_code=0x10
# [root@tpsrv ~]# /usr/bin/sg_wr_mode --page=0x10 --contents=90,0d,00,00,00,00,00,14,c0,00,18,00,00,00,01,00 /dev/tape
# bad field in MODE SELECT (10) command
# [root@tpsrv ~]# /usr/bin/sg_wr_mode --page=0x10 --contents=90,0e,00,00,00,00,00,14,c0,00,17,00,00,00,01,00 /dev/tape
# bad field in MODE SELECT (10) command
# [root@tpsrv ~]# /usr/bin/sg_wr_mode --page=0x10 --contents=90,0e,00,00,00,00,00,14,c1,00,18,00,00,00,01,00 /dev/tape
# bad field in MODE SELECT (10) command
#
# 5/ Alternatively, to change only the 'ff' byte (i.e. enable/disable compressin) without touching other values of the SCSI mode page, --mask option of the /usr/bin/sg_wr_mode command can be used:
# [root@tpsrv ~]# /usr/bin/sg_wr_mode --page=0x10 --contents=00,00,00,00,00,00,00,00,00,00,00,00,00,00,01,00 --mask=00,00,00,00,00,00,00,00,00,00,00,00,00,00,ff,00 /dev/tape
#
# 6/ The advantage of the above command (with the --mask option) is that it would work on both Oracle and IBM tape drives, because the bit that is controlling the compression is in the same position, while the other values are different.
#
# 7/ Example of the 0x10 SCSI mode page on Oracle tape drive (compression is disabled in this example):
# [root@tpsrv ~]# sdparm --page=0x10 --hex /dev/tape
# Device configuration (SSC) [0x10] mode page:
#     Current:
#  00     10 0e 00 00 00 00 00 64  50 00 10 00 00 00 00 00
#     Changeable:
#  00     10 0e 00 00 00 00 00 00  00 00 08 00 00 00 ff 00
#     Default:
#  00     10 0e 00 00 00 00 00 64  50 00 18 00 00 00 01 00
#
# 8/ Enabling the compression on an Oracle tape drive and checking afterwards:
# [root@tpsrv ~]# /usr/bin/sg_wr_mode --page=0x10 --contents=00,00,00,00,00,00,00,00,00,00,00,00,00,00,01,00 --mask=00,00,00,00,00,00,00,00,00,00,00,00,00,00,ff,00 /dev/tape
# [root@tpsrv ~]# sdparm --page=0x10 --hex /dev/tape
# Device configuration (SSC) [0x10] mode page:
#     Current:
#  00     10 0e 00 00 00 00 00 64  50 00 10 00 00 00 01 00
#     Changeable:
#  00     10 0e 00 00 00 00 00 00  00 00 08 00 00 00 ff 00
#     Default:
#  00     10 0e 00 00 00 00 00 64  50 00 18 00 00 00 01 00
#
# 9/ As explained in the man page of the sg_wr_mode command, if the --contents is shorter than the than the existing mode page then the remaining bytes are not modified. Example with mode page 0x25:
# [root@tpsrv ~]# sdparm --page=0x25 --hex /dev/tape
# [0x25] mode page:
#     Current:
#  00     25 1e 00 00 00 00 00 00  c0 00 00 00 00 00 00 00
#  10     00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00
#     Changeable:
#  00     25 1e 00 00 00 01 00 00  f0 00 00 00 00 00 00 00
#  10     00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00
#     Default:
#  00     25 1e 00 00 00 01 00 00  40 00 00 00 00 00 00 00
#  10     00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00
# [root@tpsrv ~]# /usr/bin/sg_wr_mode --page=0x25 --contents=00,00,00,00,00,01 --mask=00,00,00,00,00,0f /dev/tape
# [root@tpsrv ~]# sdparm --page=0x25 --hex /dev/tape
# [0x25] mode page:
#     Current:
#  00     25 1e 00 00 00 01 00 00  c0 00 00 00 00 00 00 00
#  10     00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00
#     Changeable:
#  00     25 1e 00 00 00 01 00 00  f0 00 00 00 00 00 00 00
#  10     00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00
#     Default:
#  00     25 1e 00 00 00 01 00 00  40 00 00 00 00 00 00 00
#  10     00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00

# Other interesting commands:
# * to check the compression settings (from Victor) - 01 = compression ON:
# /usr/bin/sg_raw --nosense --request 1k /dev/tape 1a 00 10 00 1c 00 2>&1 | /bin/grep "^ 10" | /bin/awk '{print $12;}'

# CERN Oracle T10000D settings of SCSI mode page 0x25
# AMC (Allow Maximum Capacity): byte 5, bit 0 set to 1 to enable = sdparm --page=0x25 --set=5:0:1=1 /dev/tape 
# DFSA (Disable File Sync Accelerator): byte 8, bit 7 set to 1 to disable = sdparm --page=0x25 --set=8:7:1=1 /dev/tape
# LFA (Large File Accelerator): byte 8, bit 6 set to 1 to disable = sdparm --page=0x25 --set=8:6:1=1 /dev/tape
# TAA (Tape Application Accelerator): byte 8, bites 5 & 4 set to 00 to disable = sdparm --page=0x25 --set=8:5:1=0 /dev/tape ; sdparm --page=0x25 --set=8:4:1=0 /dev/tape
