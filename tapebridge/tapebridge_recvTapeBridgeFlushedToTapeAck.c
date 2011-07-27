/******************************************************************************
 *                 tapebridge/tapebridge_recvTapeBridgeFlushedToTapeAck.c
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "h/marshall.h"
#include "h/rtcp_constants.h"
#include "h/serrno.h"
#include "h/socket_timeout.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"
#include "h/tapebridge_recvTapeBridgeFlushedToTapeAck.h"

#include <errno.h>
#include <string.h>

/******************************************************************************
 * tapebridge_recvTapeBridgeFlushedToTapeAck
 *****************************************************************************/
int32_t tapebridge_recvTapeBridgeFlushedToTapeAck(const int socketFd,
  const int netReadWriteTimeout,
  tapeBridgeFlushedToTapeAckMsg_t *const msg) {
  int  nbBytesReceived = 0;
  char buf[TAPEBRIDGEFLUSHEDTOTAPEACKMSGBODY_SIZE];

  /* Check function parameters */
  if(0 > socketFd || NULL == msg) {
    serrno = EINVAL;
    return -1;
  }

  memset(buf, '\0', sizeof(buf));

  nbBytesReceived = netread_timeout(socketFd, buf,
    TAPEBRIDGEFLUSHEDTOTAPEACKMSGBODY_SIZE, netReadWriteTimeout);
  switch(nbBytesReceived) {
  case 0: /* Connection closed by remote end */
    serrno = ECONNABORTED;
    return -1;
  case -1: /* Operation failed */
    /* netread_timeout can return -1 with serrno set to 0 */
    if(serrno == 0) {
      serrno = SEINTERNAL;
    }
    return -1;
  default: /* Some bytes were read */
    if(nbBytesReceived != sizeof(buf)) {
      serrno = SEINTERNAL;
      return -1;
    }
  }

  /* Unmarshall the tape-bridge acknowledgement message */
  {
    char *p = buf;
    unmarshall_LONG(p, msg->magic  );
    unmarshall_LONG(p, msg->reqType);
    unmarshall_LONG(p, msg->status );

    /* Check the number of bytes unmarshalled */
    if((p - buf) != TAPEBRIDGEFLUSHEDTOTAPEACKMSGBODY_SIZE) {
      serrno = SEINTERNAL;
      return -1;
    }
  }

  /* Check the magic number and request type */
  if(RTCOPY_MAGIC != msg->magic || TAPEBRIDGE_FLUSHEDTOTAPE != msg->reqType) {
    serrno = EBADMSG;
    return -1;
  }

  return nbBytesReceived;
}
