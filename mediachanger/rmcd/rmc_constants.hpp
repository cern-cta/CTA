/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2001-2025 CERN
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

//! maximum length for a VID
constexpr int CA_MAXVIDLEN = 6;

enum RMCConstant {
  RMC_CHECKI    = 5,             //!< max interval to check for work to be done
  RMC_PRTBUFSZ  = 180,
  RMC_REPBUFSZ  = 524288,        //!< must be >= max media changer server reply size
  RMC_REQBUFSZ  = 256,           //!< must be >= max media changer server request size
  RMC_MAGIC     = 0x120D0301,
  RMC_TIMEOUT   = 5,             //!< netread timeout while receiving a request
  RMC_RETRYI    = 60,
  RMC_LOGBUFSZ  = 1024,
  RMC_PORT      = 5014
};

#define REQ_DATA_SIZE (RMC_REQBUFSZ - 3 * LONGSIZE)   //!< Size of buffer pointed to by rqst_context.req_data

enum SMCExitCode {
  USERR   = 1,    //!< user error
  SYERR   = 2,    //!< system error 
  CONFERR = 4     //!< configuration error
};

enum RMCRequestType {
  RMC_GETGEOM        = 1,    //!< Get robot geometry
  RMC_FINDCART       = 2,    //!< Find cartridge(s)
  RMC_READELEM       = 3,    //!< Read element status
  RMC_MOUNT          = 4,    //!< Mount request
  RMC_UNMOUNT        = 5,    //!< Unmount request
  RMC_EXPORT         = 6,    //!< Export tape request
  RMC_IMPORT         = 7,    //!< Import tape request
  RMC_GENERICMOUNT   = 8,    //!< Generic (SCSI or ACS) mount request
  RMC_GENERICUNMOUNT = 9     //!< Generic (SCSI or ACS) mount request
};

enum SMCReplyType {
  MSG_ERR  = 1,
  MSG_DATA = 2,
  RMC_RC   = 3
};

//! SCSI media changer server messages
constexpr const char* RMC00 = "RMC00 - SCSI media changer server not available on %s\n";
constexpr const char* RMC01 = "RMC01 - robot parameter is mandatory\n";
constexpr const char* RMC02 = "RMC02 - %s error : %s\n";
constexpr const char* RMC03 = "RMC03 - illegal function %d\n";
constexpr const char* RMC04 = "RMC04 - error getting request, netread = %d\n";
constexpr const char* RMC05 = "RMC05 - cannot allocate enough memory\n";
constexpr const char* RMC06 = "RMC06 - invalid value for %s\n";
constexpr const char* RMC09 = "RMC09 - fatal configuration error: %s %s\n";
constexpr const char* RMC46 = "RMC46 - request too large (max. %d)\n";
constexpr const char* RMC92 = "RMC92 - %s request by %d,%d from %s\n";
constexpr const char* RMC98 = "RMC98 - %s\n";
