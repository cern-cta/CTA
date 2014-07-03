/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/io/io.hpp"
#include "castor/legacymsg/GenericMarshal.hpp"
#include "castor/tape/utils/utils.hpp"

#include <string.h>

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const uint32_t srcMagic, const uint32_t srcReqType,
  const GenericReplyMsgBody &srcBody)  {
  const char *const task = "marshal GenericReplyMsgBody";

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t bodyLen =
    sizeof(srcBody.status) +
    strlen(srcBody.errorMessage) + 1;

  // Calculate the total length of the message (header + body)
  const uint32_t totalLen =
    sizeof(uint32_t) + // magic
    sizeof(uint32_t) + // reqType
    sizeof(uint32_t) + // len
    bodyLen;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshal message header
  char *p = dst;
  try {
    io::marshalUint32(srcMagic, p);
    io::marshalUint32(srcReqType, p);
    io::marshalUint32(bodyLen, p);
  } catch(castor::exception::Exception &ne) { 
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal header: "
      << ne.getMessage().str();
    throw ex;
  }

  // Marshal message body
  try {
    io::marshalUint32(srcBody.status, p);
    io::marshalString(srcBody.errorMessage, p);
  } catch(castor::exception::Exception &ne) { 
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal body: "
      << ne.getMessage().str();
    throw ex;
  }

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen,
  GenericReplyMsgBody &dst)  {
  try {
    io::unmarshalUint32(src, srcLen, dst.status);
    io::unmarshalString(src, srcLen, dst.errorMessage);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal GenericReplyMsgBody: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const uint32_t srcMagic, const uint32_t srcReqType,
  const GenericErrorReplyMsgBody &srcBody)  {
  const char *const task = "marshal GenericErrorReplyMsgBody";

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t bodyLen = strlen(srcBody.errorMessage) + 1;

  // Calculate the total length of the message (header + body)
  const uint32_t totalLen =
    sizeof(uint32_t) + // magic
    sizeof(uint32_t) + // reqType
    sizeof(uint32_t) + // len
    bodyLen;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshal message header
  char *p = dst;
  try {
    io::marshalUint32(srcMagic, p);
    io::marshalUint32(srcReqType, p);
    io::marshalUint32(bodyLen, p);
  } catch(castor::exception::Exception &ne) { 
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal header: "
      << ne.getMessage().str();
    throw ex;
  }

  // Marshal message body
  try {
    io::marshalString(srcBody.errorMessage, p);
  } catch(castor::exception::Exception &ne) { 
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal body: "
      << ne.getMessage().str();
    throw ex;
  }

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen,
  GenericErrorReplyMsgBody &dst)  {
  try {
    io::unmarshalString(src, srcLen, dst.errorMessage);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal GenericErrorReplyMsgBody: " <<
      ne.getMessage().str();
    throw ex;
  }
}
