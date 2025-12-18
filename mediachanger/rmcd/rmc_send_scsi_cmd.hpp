/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

enum RMCSendScsiCmdReturn {
  // Return value >= 0 is number of bytes transferred
  RMC_SEND_SCSI_ERR_OPEN = -1,    //!< Open/stat fails with errno (message fully formatted)
  RMC_SEND_SCSI_ERR_IOCTL = -2,   //!< ioctl fails with errno (serrno = errno)
  RMC_SEND_SCSI_ERR_CAM = -3,     //!< CAM error (serrno = EIO)
  RMC_SEND_SCSI_ERR_SCSI = -4,    //!< SCSI error (serrno = EIO)
  RMC_SEND_SCSI_ERR_NOTSUP = -5,  //!< Not supported on this platform (serrno = SEOPNOTSUP)
};

/**
 * Send a SCSI command to a device
 *
 * Finds the sg device for the (major,minor) device IDs of a tape device.
 *
 * The match is done by:
 *
 *  . identifying the tape's st device node
 *  . getting the device's unique ID from sysfs
 *  . searching the sg device with the same ID (in sysfs)
 *
 * If no match is found, the returned sg path will be an empty string.
 */
int rmc_send_scsi_cmd(const int tapefd,
                      const char* const path,
                      const int do_not_open,
                      const unsigned char* const cdb,
                      const int cdblen,
                      unsigned char* const buffer,
                      const int buflen,
                      char* const sense,
                      const int senselen,
                      const int flags,
                      int* const nb_sense_ret,
                      const char** const msgaddr);
