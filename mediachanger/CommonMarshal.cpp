/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/CommonMarshal.hpp"

#include "mediachanger/io.hpp"

#include <errno.h>
#include <iostream>
#include <string.h>

namespace cta::mediachanger {

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t marshal(char* const dst, const size_t dstLen, const MessageHeader& src) {
  if (dst == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to marshal MessageHeader"
                    << ": Pointer to destination buffer is nullptr";
    throw ex;
  }

  // Calculate the length of the message header
  const uint32_t totalLen = 3 * sizeof(uint32_t);  // magic + reqType + len

  // Check that the message header buffer is big enough
  if (totalLen > dstLen) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to marshal MessageHeader"
                       ": Buffer too small : required="
                    << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshal the message header
  char* p = dst;
  marshalUint32(src.magic, p);
  marshalUint32(src.reqType, p);
  marshalUint32(src.lenOrStatus, p);

  // Calculate the number of bytes actually marshalled
  // Check that the number of bytes marshalled was what was expected
  if (const size_t nbBytesMarshalled = p - dst; totalLen != nbBytesMarshalled) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to marshal MessageHeader"
                       ": Mismatch between expected total length and actual"
                       ": expected="
                    << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void unmarshal(const char*& src, size_t& srcLen, MessageHeader& dst) {
  unmarshalUint32(src, srcLen, dst.magic);
  unmarshalUint32(src, srcLen, dst.reqType);
  unmarshalUint32(src, srcLen, dst.lenOrStatus);
}

}  // namespace cta::mediachanger
