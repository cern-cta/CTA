/******************************************************************************
 *                rtcpd_recv_rtcpHdr.c
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

#include "rtcpd_recv_rtcpHdr.h"
#include "h/marshall.h"
#include "h/serrno.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>

/******************************************************************************
 * rtcpd_recv_rtcpHdr
 *****************************************************************************/
int rtcpd_recv_rtcpHdr(const int socketFd, rtcpHdr_t *const hdr,
  const int timeout) {
  ssize_t nbBytesRead = 0;
  char hdrBuf[3 * sizeof(uint32_t)]; /* magic, reqtype and len */
  char *p = NULL;

  memset(hdrBuf, '\0', sizeof(hdrBuf));

  if(socketFd < 0 || hdr == NULL) {
    serrno = EINVAL;
    return -1;
  }

  nbBytesRead = netread_timeout(socketFd, hdrBuf, sizeof(hdrBuf), timeout);

  if(nbBytesRead == -1) {
    return -1;
  }

  if(nbBytesRead == 0) {
    serrno = SECONNDROP;
    return -1;
  }

  p = hdrBuf;
  unmarshall_LONG(p, hdr->magic);
  unmarshall_LONG(p, hdr->reqtype);
  unmarshall_LONG(p, hdr->len);

  return 0;
}
