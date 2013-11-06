/******************************************************************************
 *                h/rtcp_recvRtcpHdr.h
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

#ifndef H_RTCP_RECVRTCPHDR_H
#define H_RTCP_RECVRTCPHDR_H 1

#include "h/Castor_limits.h"
#include "h/Cuuid.h"
#include "h/osdep.h"
#include "h/rtcp.h"

#include <stdint.h>

/**
 * Reads the header of an rtcpd message from the specified socket.  The
 * contents of the header are un-marshalled and written into the specifed
 * message header structure.
 *
 * @param socketFd The file-descriptor of the socket to be read from.
 * @param hdr      Pointer to the message header structure to be written to.
 * @param timeout  Timeout in seconds to be used when reading from the
 *                 specified socket.  A value of 0 or a negative value is
 *                 interpreted as no timeout.
 * @return         If successful then the number of bytes read from the socket
 *                 On failure this function returns a negative value and sets
 *                 serrno accordingly.
 */
EXTERN_C int32_t rtcp_recvRtcpHdr(const int socketFd, rtcpHdr_t *const hdr,
  const int timeout);

#endif /* H_RTCP_RECVRTCPHDR_H */
