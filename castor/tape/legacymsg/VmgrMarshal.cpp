/******************************************************************************
 *                      castor/tape/legacymsg/VmgrMarshal.cpp
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

#include "castor/exception/Internal.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/VmgrMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/vmgr.h"

#include <errno.h>
#include <iostream>
#include <string.h>


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *const dst,
  const size_t dstLen, const VmgrTapeInfoRqstMsgBody &src)
  throw(castor::exception::Exception) {

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
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
    TAPE_THROW_CODE(EMSGSIZE,
      ": Buffer too small for VMGR tape information request message"
      ": Required size: " << totalLen <<
      ": Actual size: " << dstLen);
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  marshalUint32(VMGR_MAGIC2 , p); // Magic number
  marshalUint32(VMGR_QRYTAPE, p); // Request type
  // Marshall the total length of the message.  Please note that this is
  // different from the RTCOPY legacy protocol which marshalls the length
  // of the message body.
  marshalUint32(totalLen    , p); // Total length (UNLIKE RTCPD)
  marshalUint32(src.uid     , p);
  marshalUint32(src.gid     , p);
  marshalString(src.vid     , p);
  marshalUint16(src.side    , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Mismatch between the expected total length of the "
      "VMGR tape inforomation request message and the actual number of bytes "
      "marshalled"
      ": Expected: " << totalLen <<
      ": Marshalled: " << nbBytesMarshalled);
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, VmgrTapeInfoMsgBody &dst)
  throw(castor::exception::Exception) {

  unmarshalString(src, srcLen, dst.vsn               );
  unmarshalString(src, srcLen, dst.library           );
  unmarshalString(src, srcLen, dst.dgn               );
  unmarshalString(src, srcLen, dst.density           );
  unmarshalString(src, srcLen, dst.labelType         );
  unmarshalString(src, srcLen, dst.model             );
  unmarshalString(src, srcLen, dst.mediaLetter       );
  unmarshalString(src, srcLen, dst.manufacturer      );
  unmarshalString(src, srcLen, dst.serialNumber      );
  unmarshalUint16(src, srcLen, dst.nbSides           );
  unmarshalTime  (src, srcLen, dst.eTime             );
  unmarshalUint16(src, srcLen, dst.side              );
  unmarshalString(src, srcLen, dst.poolName          );
  unmarshalUint32(src, srcLen, dst.estimatedFreeSpace);
  unmarshalUint32(src, srcLen, dst.nbFiles           );
  unmarshalUint32(src, srcLen, dst.rCount            );
  unmarshalUint32(src, srcLen, dst.wCount            );
  unmarshalString(src, srcLen, dst.rHost             );
  unmarshalString(src, srcLen, dst.wHost             );
  unmarshalUint32(src, srcLen, dst.rJid              );
  unmarshalUint32(src, srcLen, dst.wJid              );
  unmarshalTime  (src, srcLen, dst.rTime             );
  unmarshalTime  (src, srcLen, dst.wTime             );
  unmarshalUint32(src, srcLen, dst.status            );
}
