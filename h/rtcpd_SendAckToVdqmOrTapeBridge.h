/******************************************************************************
 *                 h/rtcpd_SendAckToVdqmOrTapeBridge.h
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

#ifndef H_RTCPD_SENDACKTOVDQMORTAPEBRIDGE_H
#define H_RTCPD_SENDACKTOVDQMORTAPEBRIDGE_H 1

#include "h/net.h"
#include "h/osdep.h"

#include <stdint.h>

/**
 * Sends an acknowledgement message using the specified connection socket.
 *
 * @param connSock   The connection socket.
 * @param netTimeout The timeout in seconds to be used for network operations.
 * @param reqType    The type of the request for which the acknowledgement is
 *                   to be sent.
 * @patam ackStatus  The status value of the acknowledgement.
 * @param ackMsg     The acknowledgement string to be sent in the case of an
 *                   error.  In other words when ackStatus is negative.  An
 *                   empty string  must be represent by an empty string as
 *                   opposed to a NULL pointer to char.
 * @param errBuf     The error buffer to be used reporting error strings.
 * @param errBufLen  The length in bytes of the error buffer.
 * @return           0 on success and -1 on failure.  In the latter case a
 *                   description of the error is copied into errBuf and serrno
 *                   is set accrodingly.
 */
EXTERN_C int rtcpd_SendAckToVdqmOrTapeBridge(
  const SOCKET   connSock,
  const int      netTimeout,
  const uint32_t reqType,
  const int32_t  ackStatus,
  char *const    ackMsg,
  char *const    errBuf,
  const size_t   errBufLen);

#endif /* H_RTCPD_SENDACKTOVDQMORTAPEBRIDGE_H */
