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
#include "castor/legacymsg/CupvMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Cupv.h"

#include <string.h>

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen, const CupvCheckMsgBody &src)  {

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal CupvCheckMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t totalLen =
    sizeof(uint32_t) + // Magic number
    sizeof(uint32_t) + // Request type
    sizeof(uint32_t) + // Length of message header + body
    sizeof(src.uid) +
    sizeof(src.gid) +
    sizeof(src.privUid) +
    sizeof(src.privGid) +
    strlen(src.srcHost) + 1 +
    strlen(src.tgtHost) + 1 +
    sizeof(src.priv);

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal CupvCheckMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall message header
  char *p = dst;
  io::marshalUint32(CUPV_MAGIC , p); // Magic number
  io::marshalUint32(CUPV_CHECK, p); // Request type
  io::marshalUint32(totalLen, p); // Length of message header + body

  // Marshall message body
  io::marshalUint32(src.uid, p);
  io::marshalUint32(src.gid, p);
  io::marshalUint32(src.privUid, p);
  io::marshalUint32(src.privGid, p);
  io::marshalString(src.srcHost, p);
  io::marshalString(src.tgtHost, p);
  io::marshalUint32(src.priv, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal CupvCheckMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, CupvCheckMsgBody &dst)  {
  io::unmarshalUint32(src, srcLen, dst.uid);
  io::unmarshalUint32(src, srcLen, dst.gid);
  io::unmarshalUint32(src, srcLen, dst.privUid);
  io::unmarshalUint32(src, srcLen, dst.privGid);
  io::unmarshalString(src, srcLen, dst.srcHost);
  io::unmarshalString(src, srcLen, dst.tgtHost);
  io::unmarshalUint32(src, srcLen, dst.priv);
}
