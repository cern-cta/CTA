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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/VmgrMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/vmgr.h"

#include <errno.h>
#include <iostream>
#include <string.h>


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const VmgrTapeInfoRqstMsgBody &src)  {

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal VmgrTapeInfoRqstMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    sizeof(uint32_t)   + // uid
    sizeof(uint32_t)   + // gid
    strlen(src.vid)    + // vid
    1                  + // 1 = the string termination character of the vid
    sizeof(uint16_t);    // side

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal VmgrTapeInfoRqstMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  io::marshalUint32(VMGR_MAGIC2 , p); // Magic number
  io::marshalUint32(VMGR_QRYTAPE, p); // Request type
  // Marshall the total length of the message.  Please note that this is
  // different from the RTCOPY legacy protocol which marshalls the length
  // of the message body.
  io::marshalUint32(totalLen    , p); // Total length (UNLIKE RTCPD)
  io::marshalUint32(src.uid     , p);
  io::marshalUint32(src.gid     , p);
  io::marshalString(src.vid     , p);
  io::marshalUint16(src.side    , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal VmgrTapeInfoRqstMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const VmgrTapeMountedMsgBody &src)  {

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal VmgrTapeMountedMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    sizeof(uint32_t)   + // uid
    sizeof(uint32_t)   + // gid
    strlen(src.vid)    + // vid
    1                  + // 1 = the string termination character of the vid
    sizeof(uint16_t)   + // mode
    sizeof(uint32_t);    // jid

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal VmgrTapeMountedMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  io::marshalUint32(VMGR_MAGIC2 , p); // Magic number
  io::marshalUint32(VMGR_TPMOUNTED, p); // Request type
  // Marshall the total length of the message.  Please note that this is
  // different from the RTCOPY legacy protocol which marshals the length
  // of the message body.
  io::marshalUint32(totalLen    , p); // Total length (UNLIKE RTCPD)
  io::marshalUint32(src.uid     , p);
  io::marshalUint32(src.gid     , p);
  io::marshalString(src.vid     , p);
  io::marshalUint16(src.mode    , p);
  io::marshalUint32(src.jid     , p);

  // Calculate the number of bytes actually marshaled
  const size_t nbBytesMarshaled = p - dst;

  // Check that the number of bytes marshaled was what was expected
  if(totalLen != nbBytesMarshaled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal VmgrTapeMountedMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshaled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, VmgrTapeMountedMsgBody &dst)
   {
  io::unmarshalUint32(src, srcLen, dst.uid);
  io::unmarshalUint32(src, srcLen, dst.gid);
  io::unmarshalString(src, srcLen, dst.vid);
  io::unmarshalUint16(src, srcLen, dst.mode);
  io::unmarshalUint32(src, srcLen, dst.jid);
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, VmgrTapeInfoRqstMsgBody &dst)
   {
  io::unmarshalUint32(src, srcLen, dst.uid);
  io::unmarshalUint32(src, srcLen, dst.gid);
  io::unmarshalString(src, srcLen, dst.vid);
  io::unmarshalUint16(src, srcLen, dst.side);
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, VmgrTapeInfoMsgBody &dst)
   {

  io::unmarshalString(src, srcLen, dst.vsn               );
  io::unmarshalString(src, srcLen, dst.library           );
  io::unmarshalString(src, srcLen, dst.dgn               );
  io::unmarshalString(src, srcLen, dst.density           );
  io::unmarshalString(src, srcLen, dst.labelType         );
  io::unmarshalString(src, srcLen, dst.model             );
  io::unmarshalString(src, srcLen, dst.mediaLetter       );
  io::unmarshalString(src, srcLen, dst.manufacturer      );
  io::unmarshalString(src, srcLen, dst.serialNumber      );
  io::unmarshalUint16(src, srcLen, dst.nbSides           );
  io::unmarshalUint64(src, srcLen, dst.eTime             );
  io::unmarshalUint16(src, srcLen, dst.side              );
  io::unmarshalString(src, srcLen, dst.poolName          );
  io::unmarshalUint32(src, srcLen, dst.estimatedFreeSpace);
  io::unmarshalUint32(src, srcLen, dst.nbFiles           );
  io::unmarshalUint32(src, srcLen, dst.rCount            );
  io::unmarshalUint32(src, srcLen, dst.wCount            );
  io::unmarshalString(src, srcLen, dst.rHost             );
  io::unmarshalString(src, srcLen, dst.wHost             );
  io::unmarshalUint32(src, srcLen, dst.rJid              );
  io::unmarshalUint32(src, srcLen, dst.wJid              );
  io::unmarshalUint64(src, srcLen, dst.rTime             );
  io::unmarshalUint64(src, srcLen, dst.wTime             );
  io::unmarshalUint32(src, srcLen, dst.status            );
}
