/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1995-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#define MAXSENSE 255

/* Values of the 'flags' field for function send_cmd */
#define SCSI_IN            1
#define SCSI_OUT           2
#define SCSI_IN_OUT        4
#define SCSI_NONE          8
#define SCSI_SEL_WITH_ATN 16
#define SCSI_SYNC         32
#define SCSI_WIDE         64

/* SCSI status values */
#define SCSI_STATUS_GOOD                       0x00
#define SCSI_STATUS_CHECK_CONDITION            0x02
#define SCSI_STATUS_CONDITION_MET_GOOD         0x04
#define SCSI_STATUS_BUSY                       0x08
#define SCSI_STATUS_INTERMEDIATE_GOOD          0x10
#define SCSI_STATUS_INTERMEDIATE_CONDITION_MET 0x14
#define SCSI_STATUS_RESERVATION_CONFLICT       0x18
#define SCSI_STATUS_COMMAND_TERMINATED         0x22
#define SCSI_STATUS_QUEUE_FULL                 0x28

/* Generic SCSI driver status */
#define CAM_OK       0 /* No error at the CAM level */
#define CAM_TIMEOUT  1 /* Command timeout */
#define CAM_ERROR    2 /* Error at the CAM level */
#define CAM_NODEVICE 3 /* Device doesn't respond (inexistant device) */

/* SCSI 3 Sense key */
#define SCSI_SENSEKEY_NO_SENSE         0
#define SCSI_SENSEKEY_RECOVERED_ERROR  1
#define SCSI_SENSEKEY_NOT_READY        2
#define SCSI_SENSEKEY_MEDIUM_ERROR     3
#define SCSI_SENSEKEY_HARDWARE_ERROR   4
#define SCSI_SENSEKEY_ILLEGAL_REQUEST  5
#define SCSI_SENSEKEY_UNIT_ATTENTION   6
#define SCSI_SENSEKEY_DATA_PROTECT     7
#define SCSI_SENSEKEY_BLANK_CHECK      8
#define SCSI_SENSEKEY_VENDOR_SPECIFIC  9
#define SCSI_SENSEKEY_COPY_ABORTED    10
#define SCSI_SENSEKEY_ABORTED_COMMAND 11
#define SCSI_SENSEKEY_EQUAL           12
#define SCSI_SENSEKEY_VOLUME_OVERFLOW 13
#define SCSI_SENSEKEY_MISCOMPARE      14
#define SCSI_SENSEKEY_RESERVED        15

/* SCSI 3 Peripheric device types */
#define SCSI_PERIPH_DIRECT_ACCESS     0
#define SCSI_PERIPH_SEQUENTIAL_ACCESS 1
#define SCSI_PERIPH_PRINTER           2
#define SCSI_PERIPH_PROCESSOR         3
#define SCSI_PERIPH_WORM              4
#define SCSI_PERIPH_CDROM             5
#define SCSI_PERIPH_SCANNER           6
#define SCSI_PERIPH_OPTICAL           7
#define SCSI_PERIPH_AUTOCHANGER       8
#define SCSI_PERIPH_COMMUNICATION     9
#define SCSI_PERIPH_ASC1             10
#define SCSI_PERIPH_ASC2             11
#define SCSI_PERIPH_RAID             12
#define SCSI_PERIPH_UNKNOWN          31
