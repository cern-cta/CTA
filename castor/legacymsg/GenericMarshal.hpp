/******************************************************************************
 *                      castor/legacymsg/RmcMarshal.hpp
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
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/legacymsg/GenericReplyMsgBody.hpp"

#include <stdint.h>

namespace castor {
namespace legacymsg {

/**
 * Marshals the specified source message into the specified destination buffer.
 *
 * Please note that this method marshals the length of the message body as the
 * third field of the message header (message header = magic + reqType + len).
 *
 * @param dst        The destination message buffer.
 * @param dstLen     The length of the destination buffer.
 * @param srcMagic   The magic number of the source message.
 * @param srcReqType The request type of the source message.
 * @param srcBody    The body of the source message.
 *
 * @return         The total length of the message (header + body).
 */
size_t marshal(char *const dst, const size_t dstLen, const uint32_t srcMagic,
  const uint32_t srcReqType, const GenericReplyMsgBody &srcBody)
  ;

/**
 * Marshals the specified source message into the specified destination buffer.
 *
 * Please note that this method marshals the length of the message body as the
 * third field of the message header (message header = magic + reqType + len).
 *
 * @param dst        The destination message buffer.
 * @param srcMagic   The magic number of the source message.
 * @param srcReqType The request type of the source message.
 * @param srcBody    The body of the source message.
 * @return           The total length of the message (header + body).
 */
template<int n> size_t marshal(char (&dst)[n], const uint32_t srcMagic,
  const uint32_t srcReqType, const GenericReplyMsgBody &srcBody)
   {
  return marshal(dst, n, srcMagic, srcReqType, srcBody);
}

/**
 * Unmarshals a message body with the specified destination structure type
 * from the specified source buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the message body should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the
 * unmarshalled message body.
 * @param srcLen In/out parameter, before invocation is the length of the
 * source buffer from where the message body should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination message body structure.
 */
void unmarshal(const char * &src, size_t &srcLen, GenericReplyMsgBody &dst) ;

} // namespace legacymsg
} // namespace castor
