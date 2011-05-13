/******************************************************************************
 *                h/rtcp_marshallVdqmClientInfoMsg.h
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

#ifndef H_RTCP_MARSHALLVDQMCLIENTINFOMSG_H
#define H_RTCP_MARSHALLVDQMCLIENTINFOMSG_H 1

#include "h/osdep.h"
#include "h/vdqmClientInfoMsgBody.h"

#include <stddef.h>
#include <stdint.h>

/**
 * Returns the size in bytes of the specified VDQM_CLIENTINFO message-body if
 * it were marshalled.
 *
 * @return If successful then the size in bytes of the message-body if it were
 *         marshalled.  On failure (e.g. msgBody is NULL) this function returns
 *         a negative value and sets serrno accordingly.
 */
EXTERN_C int32_t rtcp_vdqmClientInfoMsgBodyMarshalledSize(
  vdqmClientInfoMsgBody_t *const msgBody);

/**
 * Marshalls the specified VDQM_CLIENTINFO message-body preceded by the
 * appropriate message-header to the specified buffer.
 *
 * @param buf     The buffer to which the message header and body are to be
 *                marshalled.
 * @param bufLen  The length of the buffer.
 * @param msgBody The message body.
 * @return        If successful then the length of the entire marshalled
 *                message, header plus body, in bytes.  On failure this
 *                function returns a negative value and sets serrno
 *                accordingly.
 */
EXTERN_C int32_t rtcp_marshallVdqmClientInfoMsg(char *const buf,
  const size_t bufLen, vdqmClientInfoMsgBody_t *const msgBody);

/**
 * Un-marshalls a VDQM_CLIENTINFO message-body from the specified buffer.
 *
 * @param buf     The buffer from which the message body is be un-marshalled.
 * @param bufLen  The length of the buffer.
 * @param msgBody The message body.
 * @return        If successful then the number of bytes unmarshalled.  On
 *                failure this function returns a negative value and sets
 *                serrno accordingly.
 */
EXTERN_C int32_t rtcp_unmarshallVdqmClientInfoMsgBody(char *const buf,
  const size_t bufLen, vdqmClientInfoMsgBody_t *const msgBody);

#endif /* H_RTCP_MARSHALLVDQMCLIENTINFOMSG_H */
