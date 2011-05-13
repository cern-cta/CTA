/******************************************************************************
 *                h/rtcpd_recv_rtcpHdr.h
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

#ifndef H_RTCPD_RECV_RTCPHDR_H
#define H_RTCPD_RECV_RTCPHDR_H 1

#include "h/Castor_limits.h"
#include "h/Cuuid.h"
#include "h/osdep.h"
#include "h/rtcp.h"

/**
 * Reads the header of an rtcpd message from the specified socket.  The
 * contents of the header are written into the specifed message header
 * structure.
 *
 * @param socketFd The file-descriptor of the socket to be read from.
 * @param hdr      Pointer to the message header structure to be written to.
 * @param timeout  Timeout in seconds to be used when reading from the
 *                 specified socket.  A value of 0 or a negative value is
 *                 interpreted as no timeout.
 * @return         0 on success and -1 on failure.  In the latter case serrno
 *                 is set accordingly.
 */
EXTERN_C int rtcpd_recv_rtcpHdr(const int socketFd, rtcpHdr_t *const hdr,
  const int timeout);

#endif /* H_RTCPD_RECV_RTCPHDR_H */
