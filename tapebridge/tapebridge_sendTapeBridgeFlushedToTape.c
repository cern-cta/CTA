/******************************************************************************
 *                 tapebridge/tapebridge_sendTapeBridgeFlushedToTape.c
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

#include "h/serrno.h"
#include "h/net.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"
#include "h/tapebridge_sendTapeBridgeFlushedToTape.h"

#include <errno.h>
#include <string.h>

/******************************************************************************
 * tapebridge_sendTapeBridgeFlushedToTape
 *****************************************************************************/
int32_t tapebridge_sendTapeBridgeFlushedToTape(const int socketFd,
  const int netReadWriteTimeout,
  const tapeBridgeFlushedToTapeMsgBody_t *const msgBody) {
  int  nbBytesMarshalled = 0;
  int  nbBytesSent       = 0;
  char buf[TAPEBRIDGE_MSGBUFSIZ];

  /* Check function parameters */
  if(0 > socketFd || NULL == msgBody) {
    serrno = EINVAL;
    return -1;
  }

  memset(buf, '\0', sizeof(buf));

  nbBytesMarshalled = tapebridge_marshallTapeBridgeFlushedToTapeMsg(buf,
    sizeof(buf), msgBody);
  if(-1 == nbBytesMarshalled) {
    return -1;
  }

  nbBytesSent = netwrite_timeout(socketFd, buf, nbBytesMarshalled,
    netReadWriteTimeout);

  if(-1 == nbBytesSent) {
    /* Dirty workaround for the fact netwrite_timeout may return -1 with */
    /* serrno set to 0                                                   */
    if(0 == serrno) {
      serrno = SEINTERNAL;
    }
    return -1;
  }

  if(0 == nbBytesSent) {
    serrno = ECONNABORTED;
    return -1;
  }

  if(nbBytesSent != nbBytesMarshalled) {
    serrno = SEINTERNAL;
    return -1;
  }

  return nbBytesSent;
}
