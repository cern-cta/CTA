/******************************************************************************
 *                h/rtcpd_GetClientInfo.h
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

#ifndef H_RTCPD_GETCLIENTINFO_H
#define H_RTCPD_GETCLIENTINFO_H 1

#include "h/net.h"
#include "h/osdep.h"
#include "h/rtcp.h"
#include "h/tapeBridgeClientInfo2MsgBody.h"

#include <stddef.h>

/**
 * Gets a client-info message from the specified connection socket and sends
 * the necessary reply.
 *
 * Please note that this function does not close the connection.
 *
 * @param connSock   The connection socket.
 * @param netTimeout The timeout in seconds to be used for network operations.
 * @param tapeReq    Out parameter: Tape request structure to be filled with
 *                   zeros and then partially populated with data from the
 *                   received client-info message.
 * @param fileReq    Out parameter: File request structure to be filled with
 *                   zeros and then partially populated with data from the
 *                   received client-info message.
 * @param client     Out parameter: Client-ifno structure to be filled with
 *                   zeros and then partially populated with data from the
 *                   received client-info message.
 * @param clientIsTapeBridge Out parameter: Set to true if the client is the
 *                   tape-bridge daemon else false.
 * @param tapeBridgeClientInfo2MsgBody Out parameter: If the client is the
 *                   tape-bridge daemon, then this function fills this message
 *                   body structure with the unmarshalled contents of the
 *                   received client-info message.
 * @param errBuf     The error buffer to be used reporting error strings.
 * @param errBufLen  The length in bytes of the error buffer.
 * @return           0 on success and -1 on failure.  In the latter case a
 *                   description of the error is copied into errBuf and serrno
 *                   is set accordingly.
 */
EXTERN_C int rtcpd_GetClientInfo(
  const int                             connSock,
  const int                             netTimeout,
  rtcpTapeRequest_t              *const tapeReq,
  rtcpFileRequest_t              *const fileReq,
  rtcpClientInfo_t               *const client,
  int                            *const clientIsTapeBridge,
  tapeBridgeClientInfo2MsgBody_t *const tapeBridgeClientInfo2MsgBody,
  char                           *const errBuf,
  const size_t                   errBufLen);

#endif /* H_RTCPD_GETCLIENTINFO_H */
