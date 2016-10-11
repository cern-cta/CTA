/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include <stdint.h>
#include <stdlib.h>

namespace cta {
namespace mediachanger     {
  	
const size_t HOSTNAMEBUFLEN    = 256;
const int    LISTENBACKLOG     = 2;
const size_t SERVICENAMEBUFLEN = 256;

enum ProtocolType {
  PROTOCOL_TYPE_NONE,
  PROTOCOL_TYPE_TAPE
};

enum ProtocolVersion {
  PROTOCOL_VERSION_NONE,
  PROTOCOL_VERSION_1
};

enum MsgType {
  /*  0 */ MSG_TYPE_NONE,
  /*  1 */ MSG_TYPE_EXCEPTION,
  /*  2 */ MSG_TYPE_FORKCLEANER,
  /*  3 */ MSG_TYPE_FORKDATATRANSFER,
  /*  4 */ MSG_TYPE_FORKLABEL,
  /*  5 */ MSG_TYPE_FORKSUCCEEDED,
  /*  6 */ MSG_TYPE_HEARTBEAT,
  /*  7 */ MSG_TYPE_MIGRATIONJOBFROMTAPEGATEWAY,
  /*  8 */ MSG_TYPE_MIGRATIONJOBFROMWRITETP,
  /*  9 */ MSG_TYPE_NBFILESONTAPE,
  /* 10 */ MSG_TYPE_PROCESSCRASHED,
  /* 11 */ MSG_TYPE_PROCESSEXITED,
  /* 12 */ MSG_TYPE_RECALLJOBFROMREADTP,
  /* 13 */ MSG_TYPE_RECALLJOBFROMTAPEGATEWAY,
  /* 14 */ MSG_TYPE_RETURNVALUE,
  /* 15 */ MSG_TYPE_STOPPROCESSFORKER,
  /* 16 */ MSG_TYPE_TAPEMOUNTEDFORMIGRATION,
  /* 17 */ MSG_TYPE_TAPEMOUNTEDFORRECALL,
  /* 20 */ MSG_TYPE_LABELERROR,
  /* 21 */ MSG_TYPE_ACSMOUNTTAPEREADONLY,
  /* 22 */ MSG_TYPE_ACSMOUNTTAPEREADWRITE,
  /* 23 */ MSG_TYPE_ACSDISMOUNTTAPE,
  /* 24 */ MSG_TYPE_ACSFORCEDISMOUNTTAPE,
  /* 25 */ MSG_TYPE_ADDLOGPARAMS,
  /* 26 */ MSG_TYPE_DELETELOGPARAMS,
  /* 27 */ MSG_TYPE_ARCHIVEJOBFROMCTA,
  /* 28 */ MSG_TYPE_RETRIEVEJOBFROMCTA
};

                        /* Request types */

#define RMC_GETGEOM        1 /* Get robot geometry */
#define RMC_FINDCART       2 /* Find cartridge(s) */
#define RMC_READELEM       3 /* Read element status */
#define RMC_MOUNT          4 /* Mount request */
#define RMC_UNMOUNT        5 /* Unmount request */
#define RMC_EXPORT         6 /* Export tape request */
#define RMC_IMPORT         7 /* Import tape request */
#define RMC_GENERICMOUNT   8 /* Generic (SCSI or ACS) mount request */
#define RMC_GENERICUNMOUNT 9 /* Generic (SCSI or ACS) mount request */

                        /* SCSI media changer server reply types */

#define MSG_ERR         1
#define MSG_DATA        2
#define RMC_RC          3

/**
 * The default TCP/IP port on which the CASTOR ACS daemon listens for incoming Zmq
 * connections from the tape server.
 */
const unsigned short ACS_PORT = 54521;

/**
 * The default TCP/IP port on which the CASTOR rmcd daemon listens for incoming
 * TCP/IP connections from the tape server.
 */
const unsigned short RMC_PORT = 5014;

/**
 * The maximum number of attempts a retriable RMC request should be issued.
 */
const int RMC_MAXRQSTATTEMPTS = 10;

/**
 * The magic number to identify taped messages.
 */
const uint32_t TPMAGIC = 0x141001;

/**
 * The magic number to identify rmcd messages.
 */
const uint32_t RMC_MAGIC = 0x120D0301;

} // namespace mediachanger
} // namespace cta


