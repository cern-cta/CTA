/******************************************************************************
 *                 tapebridge/tapebridge_marshall.c
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "h/marshall.h"
#include "h/rtcp_constants.h"
#include "h/serrno.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"

#include <errno.h>
#include <string.h>

/******************************************************************************
 * tapebridge_tapeBridgeClientInfo2MsgBodyMarshalledSize
 *****************************************************************************/
int32_t tapebridge_tapeBridgeClientInfo2MsgBodyMarshalledSize(
  const tapeBridgeClientInfo2MsgBody_t *const msgBody) {

  /* Check function arguments */
  if(NULL == msgBody) {
    serrno = EINVAL;
    return -1;
  }

  return
    LONGSIZE  + /* volReqID                              */
    LONGSIZE  + /* bridgeCallbackPort                    */
    LONGSIZE  + /* bridgeClientCallbackPort              */
    LONGSIZE  + /* clientUID                             */
    LONGSIZE  + /* clientGID                             */
    LONGSIZE  + /* useBufferedTapeMarksOverMultipleFiles */
    HYPERSIZE + /* maxBytesBeforeFlush                   */
    HYPERSIZE + /* maxFilesBeforeFlush                   */
    strlen(msgBody->bridgeHost)       + 1 +
    strlen(msgBody->bridgeClientHost) + 1 +
    strlen(msgBody->dgn)              + 1 +
    strlen(msgBody->drive)            + 1 +
    strlen(msgBody->clientName)       + 1;
}

/******************************************************************************
 * tapebridge_marshallTapeBridgeClientInfo2Msg
 *****************************************************************************/
int32_t tapebridge_marshallTapeBridgeClientInfo2Msg(char *const buf,
  const size_t bufLen, const tapeBridgeClientInfo2MsgBody_t *const msgBody) {

  const int32_t msgHdrLen = 3 * LONGSIZE; /* magic + reqtype + msgBodyLen */
  int32_t msgBodyLen = 0;
  int32_t nbBytesMarshalled = 0;
  char *p = buf;

  /* Check function arguments */
  if(NULL == buf || TAPEBRIDGECLIENTINFO2MSGBODY_MAXSIZE > bufLen ||
    NULL == msgBody) {
    serrno = EINVAL;
    return -1;
  }

  msgBodyLen = tapebridge_tapeBridgeClientInfo2MsgBodyMarshalledSize(msgBody);
  if(msgBodyLen < 0) {
    return -1;
  }

  marshall_LONG  (p, RTCOPY_MAGIC_OLD0                );
  marshall_LONG  (p, TAPEBRIDGE_CLIENTINFO2           );
  marshall_LONG  (p, msgBodyLen                       );
  marshall_LONG  (p, msgBody->volReqId                );
  marshall_LONG  (p, msgBody->bridgeCallbackPort      );
  marshall_LONG  (p, msgBody->bridgeClientCallbackPort);
  marshall_LONG  (p, msgBody->clientUID               );
  marshall_LONG  (p, msgBody->clientGID               );
  marshall_LONG  (p, msgBody->tapeFlushMode           );
  marshall_HYPER (p, msgBody->maxBytesBeforeFlush     );
  marshall_HYPER (p, msgBody->maxFilesBeforeFlush     );
  marshall_STRING(p, msgBody->bridgeHost              );
  marshall_STRING(p, msgBody->bridgeClientHost        );
  marshall_STRING(p, msgBody->dgn                     );
  marshall_STRING(p, msgBody->drive                   );
  marshall_STRING(p, msgBody->clientName              );

  nbBytesMarshalled = p - buf;

  /* Check actual number of bytes marshalled equals expected value */
  if(nbBytesMarshalled != msgHdrLen + msgBodyLen) {
    serrno = SEINTERNAL;
    return -1;
  }

  return nbBytesMarshalled;
}

/******************************************************************************
 * tapebridge_unmarshallTapeBridgeClientInfo2MsgBody
 *****************************************************************************/
int32_t tapebridge_unmarshallTapeBridgeClientInfo2MsgBody(
  const char *const buf, const size_t bufLen,
  tapeBridgeClientInfo2MsgBody_t *const msgBody) {

  /* Nasty override of const because of unmarshall_STRINGN */
  char *p = (char *)buf;
  int32_t nbBytesUnmarshalled = 0;

  /* Check function arguments */
  if(NULL == buf || TAPEBRIDGECLIENTINFO2MSGBODY_MINSIZE > bufLen ||
    NULL == msgBody) {
    serrno = EINVAL;
    return -1;
  }

  unmarshall_LONG   (p, msgBody->volReqId                               );
  unmarshall_LONG   (p, msgBody->bridgeCallbackPort                     );
  unmarshall_LONG   (p, msgBody->bridgeClientCallbackPort               );
  unmarshall_LONG   (p, msgBody->clientUID                              );
  unmarshall_LONG   (p, msgBody->clientGID                              );
  unmarshall_LONG   (p, msgBody->tapeFlushMode                          );
  unmarshall_HYPER  (p, msgBody->maxBytesBeforeFlush                    );
  unmarshall_HYPER  (p, msgBody->maxFilesBeforeFlush                    );
  unmarshall_STRINGN(p, msgBody->bridgeHost      , CA_MAXHOSTNAMELEN + 1);
  unmarshall_STRINGN(p, msgBody->bridgeClientHost, CA_MAXHOSTNAMELEN + 1);
  unmarshall_STRINGN(p, msgBody->dgn             , CA_MAXDGNLEN      + 1);
  unmarshall_STRINGN(p, msgBody->drive           , CA_MAXUNMLEN      + 1);
  unmarshall_STRINGN(p, msgBody->clientName      , CA_MAXUSRNAMELEN  + 1);

  nbBytesUnmarshalled = p - buf;

  return nbBytesUnmarshalled;
}

/******************************************************************************
 * tapebridge_marshallTapeBridgeClientInfo2Ack
 *****************************************************************************/
int32_t tapebridge_marshallTapeBridgeClientInfo2Ack(char *const buf,
  const size_t bufLen, const uint32_t ackStatus, const char *const ackErrMsg) {
  char           *p           = NULL;
  const uint32_t msgHdrLen    = 3 * LONGSIZE; /* magic + reqType + msgBodyLen */
  uint32_t       msgBodyLen   = 0;
  uint32_t       msgTotalLen  = 0;
  uint32_t       ackErrMsgLen = 0;
  uint32_t       nbBytesMarshalled = 0;

  /* Check function arguments */
  if(NULL == buf || NULL == ackErrMsg) {
    serrno = EINVAL;
    return -1;
  }

  ackErrMsgLen = strlen(ackErrMsg);
  msgBodyLen   = LONGSIZE + ackErrMsgLen + 1; /* status + error message */
  msgTotalLen  = msgHdrLen + msgBodyLen;

  if(bufLen < msgTotalLen) {
    serrno = EINVAL;
    return -1;
  }

  p = buf;
  marshall_LONG  (p, RTCOPY_MAGIC_OLD0     ); /* Magic number */
  marshall_LONG  (p, TAPEBRIDGE_CLIENTINFO2); /* Request type */
  marshall_LONG  (p, msgBodyLen            );
  marshall_LONG  (p, ackStatus             );
  marshall_STRING(p, ackErrMsg             );
  nbBytesMarshalled = p - buf;
  if(nbBytesMarshalled != msgTotalLen) {
    serrno = SEINTERNAL;
    return -1;
  }

  return msgTotalLen;
}

/******************************************************************************
 * tapebridge_marshallTapeBridgeFlushedToTape
 *****************************************************************************/
int32_t tapebridge_marshallTapeBridgeFlushedToTapeMsg(char *const buf,
  const size_t bufLen, const tapeBridgeFlushedToTapeMsgBody_t *const msgBody) {
  char           *p           = NULL;
  const uint32_t msgHdrLen    = 3 * LONGSIZE; /* magic + reqType + msgBodyLen */
  const uint32_t msgBodyLen   = 2 * LONGSIZE; /* volReqId + tapeFseq */
  const uint32_t msgTotalLen  = msgHdrLen + msgBodyLen;
  uint32_t       nbBytesMarshalled = 0;

  /* Check function arguments */
  if(NULL == buf || TAPEBRIDGEFLUSHEDTOTAPEMSGBODY_SIZE > bufLen ||
    NULL == msgBody) {
    serrno = EINVAL;
    return -1;
  }

  if(bufLen < msgTotalLen) {
    serrno = EINVAL;
    return -1;
  }

  p = buf;
  marshall_LONG(p, RTCOPY_MAGIC            ); /* Magic number */
  marshall_LONG(p, TAPEBRIDGE_FLUSHEDTOTAPE); /* Request type */
  marshall_LONG(p, msgBodyLen              );
  marshall_LONG(p, msgBody->volReqId       );
  marshall_LONG(p, msgBody->tapeFseq       );
  nbBytesMarshalled = p - buf;
  if(nbBytesMarshalled != msgTotalLen) {
    serrno = SEINTERNAL;
    return -1;
  }

  return msgTotalLen;
}
