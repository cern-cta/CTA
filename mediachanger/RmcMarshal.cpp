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

#include "mediachanger/Constants.hpp"
#include "mediachanger/io.hpp"
#include "mediachanger/RmcMarshal.hpp"

#include <string.h>

namespace cta::mediachanger {

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
void _marshalBody(const RmcMountMsgBody& src, char*& p, size_t& pLen) {
  pLen -= marshalUint32(src.uid, p);
  pLen -= marshalUint32(src.gid, p);
  marshalString(src.unusedLoader, p, pLen);
  marshalString(src.vid, p, pLen);
  pLen -= marshalUint16(src.side, p);
  pLen -= marshalUint16(src.drvOrd, p);
}

void _marshalBody(const RmcUnmountMsgBody& src, char*& p, size_t& pLen) {
  pLen -= marshalUint32(src.uid, p);
  pLen -= marshalUint32(src.gid, p);
  marshalString(src.unusedLoader, p, pLen);
  marshalString(src.vid, p, pLen);
  pLen -= marshalUint16(src.drvOrd, p);
  pLen -= marshalUint16(src.force, p);
}

template<typename T>
size_t _marshal(char* const dst, const size_t dstLen, const T& src, const std::string& task) {
  if(dst == nullptr) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Pointer to destination buffer is nullptr";
    throw ex;
  }

  // Calculate the total length of the message (header + body)
  const uint32_t totalLen =
    sizeof(uint32_t) + // magic
    sizeof(uint32_t) + // reqType
    sizeof(uint32_t) + // len
    src.bodyLen();

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshal message header
  char *p = dst;
  size_t pLen = dstLen; // available buffer size remaining
  try {
    const uint32_t magic = RMC_MAGIC;
    const uint32_t reqType = T::requestType;
    pLen -= marshalUint32(magic , p);
    pLen -= marshalUint32(reqType, p);
    pLen -= marshalUint32(totalLen, p);
  } catch(cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal header: " << ne.getMessage().str();
    throw ex;
  }

  // Marshal message body
  try {
    _marshalBody(src, p, pLen);
  } catch(cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal body: " << ne.getMessage().str();
    throw ex;
  }

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != dstLen-pLen) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Mismatch between expected total length and actual: expected="
                    << totalLen << " actual=" << dstLen-pLen;
    throw ex;
  }

  return totalLen;
}

size_t marshal(char* const dst, const size_t dstLen, const RmcMountMsgBody& src)  {
  return _marshal(dst, dstLen, src, "marshal RmcMountMsgBody");
}

size_t marshal(char* const dst, const size_t dstLen, const RmcUnmountMsgBody& src)  {
  return _marshal(dst, dstLen, src, "marshal RmcUnmountMsgBody");
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void unmarshal(const char * &src, size_t &srcLen, RmcMountMsgBody &dst)  {
  try {
    unmarshalUint32(src, srcLen, dst.uid);
    unmarshalUint32(src, srcLen, dst.gid);
    unmarshalString(src, srcLen, dst.unusedLoader);
    unmarshalString(src, srcLen, dst.vid);
    unmarshalUint16(src, srcLen, dst.side);
    unmarshalUint16(src, srcLen, dst.drvOrd);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal RmcMountMsgBody: " <<
      ne.getMessage().str();
    throw ex;
  }
}

void unmarshal(const char * &src, size_t &srcLen, RmcUnmountMsgBody &dst)  {
  try {
    unmarshalUint32(src, srcLen, dst.uid);
    unmarshalUint32(src, srcLen, dst.gid);
    unmarshalString(src, srcLen, dst.unusedLoader);
    unmarshalString(src, srcLen, dst.vid);
    unmarshalUint16(src, srcLen, dst.drvOrd);
    unmarshalUint16(src, srcLen, dst.force);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal RmcUnmountMsgBody: " <<
      ne.getMessage().str();
    throw ex;
  }
}

} // namespace cta::mediachanger
