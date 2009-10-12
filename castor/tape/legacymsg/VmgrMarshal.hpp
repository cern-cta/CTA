/******************************************************************************
 *                      castor/tape/legacymsg/VmgrMarshal.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_LEGACYMSG_VMGRMARSHAL_HPP
#define CASTOR_TAPE_LEGACYMSG_VMGRMARSHAL_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/legacymsg/VmgrTapeInfoMsgBody.hpp"
#include "castor/tape/legacymsg/VmgrTapeInfoRqstMsgBody.hpp"

#include <errno.h>
#include <stdint.h>
#include <string>


namespace castor    {
namespace tape      {
namespace legacymsg {

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst    The destination message buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the message (header + body).
 */
size_t marshal(char *const dst, const size_t dstLen,
  const VmgrTapeInfoRqstMsgBody &src) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst The destination message buffer.
 * @param src The source structure.
 * @return    The total length of the message (header + body).
 */
template<int n> size_t marshal(char (&dst)[n],
  const VmgrTapeInfoRqstMsgBody &src) throw(castor::exception::Exception) {
  return marshal(dst, n, src);
}

/**
 * Unmarshals a message body with the specified destination structure type
 * from the specified source buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the message body should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the
 * unmarshalled message body.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the message body should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination message body structure.
 */
void unmarshal(const char * &src, size_t &srcLen,
  VmgrTapeInfoMsgBody &dst) throw(castor::exception::Exception);

} // namespace legacymsg
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_LEGACYMSG_VMGRMARSHAL_HPP
