/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include <cstddef>
#include <cstdint>

namespace cta::mediachanger {

const size_t HOSTNAMEBUFLEN    = 256;
const int    LISTENBACKLOG     = 2;
const size_t SERVICENAMEBUFLEN = 256;

enum RequestType {
  RMC_GETGEOM  = 1,    // Get robot geometry
  RMC_FINDCART = 2,    // Find cartridge(s)
  RMC_READELEM = 3,    // Read element status
  RMC_MOUNT    = 4,    // Mount request
  RMC_UNMOUNT  = 5,    // Unmount request
  RMC_EXPORT   = 6,    // Export tape request
  RMC_IMPORT   = 7     // Import tape request
};

// SCSI media changer server reply types
enum MediaChangerReplyType {
  MSG_ERR  = 1,
  MSG_DATA = 2,
  RMC_RC   = 3
};

/*
 *------------------------------------------------------------------------
 * RMC (Remote SCSI media changer server) errors
 *------------------------------------------------------------------------
 */
#define ERMBASEOFF      2200            /* RMC error base offset        */
#define        ERMCNACT        ERMBASEOFF+1    /* Remote SCSI media changer server not active or service being drained */
#define        ERMCRBTERR      (ERMBASEOFF+2)  /* Remote SCSI media changer error */
#define        ERMCUNREC       ERMCRBTERR+1    /* Remote SCSI media changer unrec. error */
#define        ERMCSLOWR       ERMCRBTERR+2    /* Remote SCSI media changer error (slow retry) */
#define        ERMCFASTR       ERMCRBTERR+3    /* Remote SCSI media changer error (fast retry) */
#define        ERMCDFORCE      ERMCRBTERR+4    /* Remote SCSI media changer error (demount force) */
#define        ERMCDDOWN       ERMCRBTERR+5    /* Remote SCSI media changer error (drive down) */
#define        ERMCOMSGN       ERMCRBTERR+6    /* Remote SCSI media changer error (ops message) */
#define        ERMCOMSGS       ERMCRBTERR+7    /* Remote SCSI media changer error (ops message + retry) */
#define        ERMCOMSGR       ERMCRBTERR+8    /* Remote SCSI media changer error (ops message + wait) */
#define        ERMCUNLOAD      ERMCRBTERR+9    /* Remote SCSI media changer error (unload + demount) */
#define ERMMAXERR       ERMBASEOFF+11

/**
 * The default TCP/IP port on which the CASTOR rmcd daemon listens for incoming
 * TCP/IP connections from the tape server.
 */
const unsigned short RMC_PORT = 5014;

/**
 * The network timeout of rmc communications should be several minutes due
 * to the time it takes to mount and unmount tapes.
 */
const int RMC_NET_TIMEOUT = 600; // Timeout in seconds

/**
 * The maximum number of attempts a retriable RMC request should be issued.
 */
const int RMC_MAX_RQST_ATTEMPTS = 10;

/**
 * The magic number to identify rmcd messages.
 */
const uint32_t RMC_MAGIC = 0x120D0301;

} // namespace cta::mediachanger
