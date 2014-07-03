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
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"
#include "h/Ctape.h"
#include "h/serrno.h"

#include <errno.h>
#include <string.h>

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const TapeStatRequestMsgBody &src)  {
  const char *const task = "marshal TapeStatRequestMsgBody";

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const size_t bodyLen =
    sizeof(src.uid) +
    sizeof(src.gid);

  // Calculate the total length of the message (header + body)
  const uint32_t totalLen =
    sizeof(uint32_t) + // Magic number
    sizeof(uint32_t) + // Request type
    sizeof(uint32_t) + // Length of message body
    bodyLen;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall message header
  char *p = dst;
  try {
    const uint32_t magic = TPMAGIC;
    const uint32_t reqType = TPSTAT;
    io::marshalUint32(magic , p);
    io::marshalUint32(reqType, p);
    io::marshalUint32(totalLen, p);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal header: "
      << ne.getMessage().str();
    throw ex;
  }

  // Marshal message body
  try {
    io::marshalUint32(src.uid, p);
    io::marshalUint32(src.gid, p);
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
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const TapeStatReplyMsgBody &body)  {
  const char *const task = "marshall TapeStatReplyMsgBody";

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  uint32_t bodyLen = sizeof(body.number_of_drives);
  for(uint16_t i = 0; i<body.number_of_drives; i++) {
    bodyLen +=
      sizeof(body.drives[i].uid)         +
      sizeof(body.drives[i].jid)         +
      strlen(body.drives[i].dgn) + 1     +
      sizeof(body.drives[i].up)          +
      sizeof(body.drives[i].asn)         +
      sizeof(body.drives[i].asn_time)    +
      strlen(body.drives[i].drive) + 1   +
      sizeof(body.drives[i].mode)        +
      strlen(body.drives[i].lblcode) + 1 +
      sizeof(body.drives[i].tobemounted) +
      strlen(body.drives[i].vid) + 1     +
      strlen(body.drives[i].vsn) + 1     +
      sizeof(body.drives[i].cfseq);
  }

  // Calculate the total length of the message (header + body)
  const uint32_t totalLen =
    sizeof(uint32_t) + // Magic number
    sizeof(uint32_t) + // Request type
    sizeof(uint32_t) + // Length of message body
    bodyLen;

  // Check that the message header buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshal message header
  char *p = dst;
  try {
    const uint32_t magic = TPMAGIC;
    const uint32_t reqType = MSG_DATA;
    io::marshalUint32(magic, p);
    io::marshalUint32(reqType, p);
    io::marshalUint32(bodyLen, p);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal header: "
      << ne.getMessage().str();
    throw ex;
  }

  // Marshal message body
  try {
    io::marshalUint16(body.number_of_drives, p);

    for(int i=0; i<body.number_of_drives; i++) {
      io::marshalUint32(body.drives[i].uid, p);
      io::marshalUint32(body.drives[i].jid, p);
      io::marshalString(body.drives[i].dgn, p);
      io::marshalUint16(body.drives[i].up, p);
      io::marshalUint16(body.drives[i].asn, p);
      io::marshalUint32(body.drives[i].asn_time, p);
      io::marshalString(body.drives[i].drive, p);
      io::marshalUint16(body.drives[i].mode, p);
      io::marshalString(body.drives[i].lblcode, p);
      io::marshalUint16(body.drives[i].tobemounted, p);
      io::marshalString(body.drives[i].vid, p);
      io::marshalString(body.drives[i].vsn, p);
      io::marshalUint32(body.drives[i].cfseq, p);
    }
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
    ex.getMessage() << "Failed to marshal TapeStatReplyMsgBody"
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
  const TapeConfigRequestMsgBody &src)  {
  const char *const task = "marshal TapeConfigRequestMsgBody";

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t bodyLen =
    sizeof(src.uid) +
    sizeof(src.gid) +
    strlen(src.drive) + 1 +
    sizeof(src.status);

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
    const uint32_t magic = TPMAGIC;
    const uint32_t reqType = TPCONF;
    io::marshalUint32(magic , p);
    io::marshalUint32(reqType, p);
    io::marshalUint32(totalLen, p);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal header: "
      << ne.getMessage().str();
    throw ex;
  } 

  // Marshal message body
  try {
    io::marshalUint32(src.uid, p);
    io::marshalUint32(src.gid, p);
    io::marshalString(src.drive, p);
    io::marshalUint16(src.status, p);
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
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const TapeLabelRqstMsgBody &src)  {
  const char *const task = "marshal TapeLabelRqstMsgBody";

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t bodyLen =
    sizeof(src.force) + // force
    sizeof(src.uid) + // uid
    sizeof(src.gid) + // gid
    strlen(src.vid) + 1 + // vid
    strlen(src.drive) + 1 + // drive
    strlen(src.dgn) + 1; // dgn

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
    const uint32_t magic = TPMAGIC;
    const uint32_t reqType = TPLABEL;
    io::marshalUint32(magic , p);
    io::marshalUint32(reqType, p);
    io::marshalUint32(totalLen, p);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal header: "
      << ne.getMessage().str();
    throw ex;
  }

  // Marshal message body
  try {
    io::marshalUint16(src.force, p);
    io::marshalUint32(src.uid, p);
    io::marshalUint32(src.gid, p);
    io::marshalString(src.vid, p);
    io::marshalString(src.drive, p);
    io::marshalString(src.dgn, p);
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
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen, const TapeUpdateDriveRqstMsgBody &src)  {
  const char *task = "marshal TapeUpdateDriveRqstMsgBody";

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t bodyLen =
    sizeof(uint32_t) + // event
    sizeof(uint32_t) + // mode
    sizeof(uint32_t) + // clientType
    strlen(src.vid) + 1 + // vid
    strlen(src.drive) + 1; // drive

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
    const uint32_t magic = TPMAGIC;
    const uint32_t reqType = UPDDRIVE;
    io::marshalUint32(magic, p);
    io::marshalUint32(reqType, p);
    io::marshalUint32(totalLen, p);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to marshal header: "
      << ne.getMessage().str();
    throw ex;
  }

  // Marshal message body
  try {
    io::marshalUint32(src.event, p);
    io::marshalUint32(src.mode, p);
    io::marshalUint32(src.clientType, p);   
    io::marshalString(src.vid, p);
    io::marshalString(src.drive, p);
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
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeStatReplyMsgBody &dst)  {
  try {
    io::unmarshalUint16(src, srcLen, dst.number_of_drives);
    for(int i=0; i<dst.number_of_drives; i++) {
      io::unmarshalUint32(src, srcLen, dst.drives[i].uid);
      io::unmarshalUint32(src, srcLen, dst.drives[i].jid);
      io::unmarshalString(src, srcLen, dst.drives[i].dgn);
      io::unmarshalUint16(src, srcLen, dst.drives[i].up);
      io::unmarshalUint16(src, srcLen, dst.drives[i].asn);
      io::unmarshalUint32(src, srcLen, dst.drives[i].asn_time);
      io::unmarshalString(src, srcLen, dst.drives[i].drive);
      io::unmarshalUint16(src, srcLen, dst.drives[i].mode);
      io::unmarshalString(src, srcLen, dst.drives[i].lblcode);
      io::unmarshalUint16(src, srcLen, dst.drives[i].tobemounted);
      io::unmarshalString(src, srcLen, dst.drives[i].vid);
      io::unmarshalString(src, srcLen, dst.drives[i].vsn);
      io::unmarshalUint32(src, srcLen, dst.drives[i].cfseq);
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal TapeStatReplyMsgBody: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeUpdateDriveRqstMsgBody &dst)  {
  try {
    io::unmarshalUint32(src, srcLen, dst.event);
    io::unmarshalUint32(src, srcLen, dst.mode);
    io::unmarshalUint32(src, srcLen, dst.clientType);
    io::unmarshalString(src, srcLen, dst.vid);
    io::unmarshalString(src, srcLen, dst.drive);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal TapeUpdateDriveRqstMsgBody: " << 
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeConfigRequestMsgBody &dst)  {
  try {
    io::unmarshalUint32(src, srcLen, dst.uid);
    io::unmarshalUint32(src, srcLen, dst.gid);
    io::unmarshalString(src, srcLen, dst.drive);
    io::unmarshalInt16(src, srcLen, dst.status);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal TapeConfigRequestMsgBody: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeLabelRqstMsgBody &dst)  {
  try {
    io::unmarshalUint16(src, srcLen, dst.force);
    io::unmarshalUint32(src, srcLen, dst.uid);
    io::unmarshalUint32(src, srcLen, dst.gid);
    io::unmarshalString(src, srcLen, dst.vid);
    io::unmarshalString(src, srcLen, dst.drive);
    io::unmarshalString(src, srcLen, dst.dgn);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal TapeLabelRqstMsgBody: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeStatRequestMsgBody &dst)  {
  try {
    io::unmarshalUint32(src, srcLen, dst.uid);
    io::unmarshalUint32(src, srcLen, dst.gid);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal TapeStatRequestMsgBody: " <<
      ne.getMessage().str();
    throw ex;
  }
}
