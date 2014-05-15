/******************************************************************************
 *                      castor/legacymsg/VmgrMarshal.cpp
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

#include "castor/io/io.hpp"
#include "castor/legacymsg/VdqmMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <iostream>
#include <string.h>

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst,
  const size_t dstLen, const VdqmDrvRqstMsgBody &src)
  throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Failed to marshal VdqmDrvRqstMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    sizeof(int32_t) + // status
    sizeof(int32_t) + // drvReqId
    sizeof(int32_t) + // volReqId
    sizeof(int32_t) + // jobId
    sizeof(int32_t) + // recvTime
    sizeof(int32_t) + // resetTime
    sizeof(int32_t) + // useCount
    sizeof(int32_t) + // errCount
    sizeof(int32_t) + // transfMB
    sizeof(int32_t) + // mode
    sizeof(uint64_t) + // totalMB
    strlen(src.volId) + 1 + // volId
    strlen(src.server) + 1 + // server
    strlen(src.drive) + 1 + // drive
    strlen(src.dgn) + 1 + // dgn
    strlen(src.dedicate) + 1; // dedicate

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex(EMSGSIZE);
    ex.getMessage() << "Failed to marshal VdqmDrvRqstMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall message header
  char *p = dst;
  io::marshalUint32(VDQM_MAGIC , p); // Magic number
  io::marshalUint32(VDQM_DRV_REQ, p); // Request type
  io::marshalUint32(len, p); // Length of message body

  // Marshall message body
  io::marshalUint32(src.status, p);
  io::marshalUint32(src.drvReqId, p);
  io::marshalUint32(src.volReqId, p);
  io::marshalUint32(src.jobId, p);
  io::marshalUint32(src.recvTime, p);
  io::marshalUint32(src.resetTime, p);
  io::marshalUint32(src.useCount, p);
  io::marshalUint32(src.errCount, p);
  io::marshalUint32(src.transfMB, p);
  io::marshalUint32(src.mode, p);
  io::marshalUint64(src.totalMB, p);
  io::marshalString(src.volId, p);
  io::marshalString(src.server, p);
  io::marshalString(src.drive, p);
  io::marshalString(src.dgn, p);
  io::marshalString(src.dedicate, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal VdqmDrvRqstMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, VdqmDrvRqstMsgBody &dst)
  throw(castor::exception::Exception) {
  io::unmarshalInt32(src, srcLen, dst.status);
  io::unmarshalInt32(src, srcLen, dst.drvReqId);
  io::unmarshalInt32(src, srcLen, dst.volReqId);
  io::unmarshalInt32(src, srcLen, dst.jobId);
  io::unmarshalInt32(src, srcLen, dst.recvTime);
  io::unmarshalInt32(src, srcLen, dst.resetTime);
  io::unmarshalInt32(src, srcLen, dst.useCount);
  io::unmarshalInt32(src, srcLen, dst.errCount);
  io::unmarshalInt32(src, srcLen, dst.transfMB);
  io::unmarshalInt32(src, srcLen, dst.mode);
  io::unmarshalUint64(src, srcLen, dst.totalMB);
  io::unmarshalString(src, srcLen, dst.volId);
  io::unmarshalString(src, srcLen, dst.server);
  io::unmarshalString(src, srcLen, dst.drive);
  io::unmarshalString(src, srcLen, dst.dgn);
  io::unmarshalString(src, srcLen, dst.dedicate);
}
