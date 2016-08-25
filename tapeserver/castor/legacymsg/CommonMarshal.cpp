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
#include "castor/legacymsg/CommonMarshal.hpp"

#include <errno.h>
#include <iostream>
#include <string.h>


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const MessageHeader &src)  {

  if(dst == NULL) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to marshal MessageHeader"
      << ": Pointer to destination buffer is NULL";
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
  io::marshalUint32(src.magic      , p);
  io::marshalUint32(src.reqType    , p);
  io::marshalUint32(src.lenOrStatus, p);

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
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen,
  MessageHeader &dst)  {

  io::unmarshalUint32(src, srcLen, dst.magic);
  io::unmarshalUint32(src, srcLen, dst.reqType);
  io::unmarshalUint32(src, srcLen, dst.lenOrStatus);
}
