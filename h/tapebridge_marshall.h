/******************************************************************************
 *                h/tapebridge_marshall.h
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

#ifndef H_TAPEBRIDGE_MARSHALL_H
#define H_TAPEBRIDGE_MARSHALL_H 1

#include "h/osdep.h"
#include "h/tapeBridgeClientInfoMsgBody.h"
#include "h/tapeBridgeFlushedToTapeMsgBody.h"

#include <stddef.h>
#include <stdint.h>

/**
 * Returns the size in bytes of the specified TAPEBRIDGE_CLIENTINFO
 * message-body if it were marshalled.
 *
 * @return If successful then the size in bytes of the message-body if it were
 *         marshalled.  On failure (e.g. msgBody is NULL) this function returns
 *         a negative value and sets serrno accordingly.
 */
EXTERN_C int32_t tapebridge_tapeBridgeClientInfoMsgBodyMarshalledSize(
  const tapeBridgeClientInfoMsgBody_t *const msgBody);

/**
 * Marshalls the specified TAPEBRIDGE_CLIENTINFO message-body preceded by the
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
EXTERN_C int32_t tapebridge_marshallTapeBridgeClientInfoMsg(char *const buf,
  const size_t bufLen, const tapeBridgeClientInfoMsgBody_t *const msgBody);

/**
 * Un-marshalls a TAPEBRIDGE_CLIENTINFO message-body from the specified buffer.
 *
 * @param buf     The buffer from which the message body is be un-marshalled.
 * @param bufLen  The length of the buffer.
 * @param msgBody The message body.
 * @return        If successful then the number of bytes unmarshalled.  On
 *                failure this function returns a negative value and sets
 *                serrno accordingly.
 */
EXTERN_C int32_t tapebridge_unmarshallTapeBridgeClientInfoMsgBody(
  const char *const buf, const size_t bufLen,
  tapeBridgeClientInfoMsgBody_t *const msgBody);


/**
 * Marshalls a complete (header plus body) tape-bridge acknowledgement message
 * to the specified buffer.
 *
 * @param buf       The buffer to which the message header and body are to be
 *                  marshalled.
 * @param bufLen    The length of the buffer.
 * @param ackStatus The status value to marshalled into the message body.
 * @param ackErrMsg The error message to be marshalled into the message body.
 *                  This message should be an empty string if the value of
 *                  ackStatus is 0.
 * @return          If successful then the length of the entire marshalled
 *                  message, header plus body, in bytes.  On failure this
 *                  function returns a negative value and sets serrno
 *                  accordingly.
 */
EXTERN_C int32_t tapebridge_marshallTapeBridgeAck(char *const buf,
  const size_t bufLen, const uint32_t ackStatus, const char *const ackErrMsg);

/**
 * Marshalls a complete (header plus body) TAPEBRIDGE_TAPEFLUSHED message to
 * the specified buffer.
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
EXTERN_C int32_t tapebridge_marshallTapeBridgeFlushedToTapeMsg(char *const buf,
  const size_t bufLen, const tapeBridgeFlushedToTapeMsgBody_t *const msgBody);

#endif /* H_TAPEBRIDGE_MARSHALL_H */
