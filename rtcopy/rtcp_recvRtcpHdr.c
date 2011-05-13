/******************************************************************************
 *                 rtcopy/rtcp_recvRtcpHdr.c
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

#include <arpa/inet.h>
#include "h/marshall.h"
#include "h/net.h"
#include "h/rtcp_constants.h"
#include "h/rtcp_recvRtcpHdr.h"
#include "h/serrno.h"

#include <errno.h>
#include <string.h>

/******************************************************************************
 * rtcp_recvRtcpHdr
 *****************************************************************************/
int32_t rtcp_recvRtcpHdr(const int socketFd, rtcpHdr_t *const hdr,
  const int timeout) {
  ssize_t nbBytesRead = 0;
  char hdrBuf[3 * sizeof(uint32_t)]; /* magic, reqtype and len */
  char *p = NULL;
  uint32_t nbBytesUnmarshalled = 0;

  /* Check function-parameters */
  if(socketFd < 0 || hdr == NULL) {
    serrno = EINVAL;
    return -1;
  }

  memset(hdrBuf, '\0', sizeof(hdrBuf));
  nbBytesRead = netread_timeout(socketFd, hdrBuf, sizeof(hdrBuf), timeout);

  if(-1 == nbBytesRead) {
    return -1;
  }

  if(0 == nbBytesRead) {
    serrno = SECONNDROP;
    return -1;
  }

  if((size_t)nbBytesRead != sizeof(hdrBuf)) {
    serrno = SEINTERNAL;

    return -1;
  }

  memset(hdr, '\0', sizeof(*hdr));
  p = hdrBuf;
  unmarshall_LONG(p, hdr->magic);
  unmarshall_LONG(p, hdr->reqtype);
  unmarshall_LONG(p, hdr->len);

  nbBytesUnmarshalled = p - hdrBuf;

  if((size_t)nbBytesRead != nbBytesUnmarshalled) {
    serrno = SEINTERNAL;

    return -1;
  }

  return nbBytesRead;
}
