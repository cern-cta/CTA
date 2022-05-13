/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "mediachanger/io.hpp"
#include "mediachanger/CommonMarshal.hpp"

#include <errno.h>
#include <iostream>
#include <string.h>

namespace cta {
namespace mediachanger {

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t marshal(char *const dst, const size_t dstLen, const MessageHeader &src) {

  if(dst == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to marshal MessageHeader"
      << ": Pointer to destination buffer is nullptr";
    throw ex;
  }

  // Calculate the length of the message header
  const uint32_t totalLen = 3 * sizeof(uint32_t);  // magic + reqType + len

  // Check that the message header buffer is big enough
  if(totalLen > dstLen) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to marshal MessageHeader"
      ": Buffer too small : required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshal the message header
  char *p = dst;
  marshalUint32(src.magic      , p);
  marshalUint32(src.reqType    , p);
  marshalUint32(src.lenOrStatus, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to marshal MessageHeader"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void unmarshal(const char * &src, size_t &srcLen, MessageHeader &dst)  {

  unmarshalUint32(src, srcLen, dst.magic);
  unmarshalUint32(src, srcLen, dst.reqType);
  unmarshalUint32(src, srcLen, dst.lenOrStatus);
}

} // namespace mediachanger
} // namespace cta
