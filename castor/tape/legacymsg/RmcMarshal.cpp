/******************************************************************************
 *                      castor/tape/legacymsg/RmcMarshal.cpp
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

#include "castor/exception/Internal.hpp"
#include "castor/io/io.hpp"
#include "castor/tape/legacymsg/RmcMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rmc_constants.h"

#include <string.h>

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *const dst, const size_t dstLen, const RmcAcsMntMsgBody &src) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal RmcAcsMntMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    sizeof(src.uid) +
    sizeof(src.gid) +
    sizeof(src.acs) +
    sizeof(src.lsm) +
    sizeof(src.panel) +
    sizeof(src.transport) +
    strlen(src.vid) + 1;

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal RmcAcsMntMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall message header
  char *p = dst;
  io::marshalUint32(RMC_MAGIC , p); // Magic number
  io::marshalUint32(RMC_ACS_MOUNT, p); // Request type
  io::marshalUint32(len, p); // Length of message body

  // Marshall message body
  io::marshalUint32(src.uid, p);
  io::marshalUint32(src.gid, p);
  io::marshalUint32(src.acs, p);
  io::marshalUint32(src.lsm, p);
  io::marshalUint32(src.panel, p);
  io::marshalUint32(src.transport, p);
  io::marshalString(src.vid, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal RmcAcsMntMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << "actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src, size_t &srcLen, RmcAcsMntMsgBody &dst) throw(castor::exception::Exception) {
  io::unmarshalUint32(src, srcLen, dst.uid);
  io::unmarshalUint32(src, srcLen, dst.gid);
  io::unmarshalUint32(src, srcLen, dst.acs);
  io::unmarshalUint32(src, srcLen, dst.lsm);
  io::unmarshalUint32(src, srcLen, dst.panel);
  io::unmarshalUint32(src, srcLen, dst.transport);
  io::unmarshalString(src, srcLen, dst.vid);
}
