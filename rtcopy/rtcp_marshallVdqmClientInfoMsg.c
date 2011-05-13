/******************************************************************************
 *                 rtcopy/rtcp_marshallVdqmClientInfoMsg.c
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
#include "h/rtcp_marshallVdqmClientInfoMsg.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <string.h>

/******************************************************************************
 * rtcp_vdqmClientInfoMsgBodyMarshalledSize
 *****************************************************************************/
int32_t rtcp_vdqmClientInfoMsgBodyMarshalledSize(
  vdqmClientInfoMsgBody_t *const msgBody) {

  /* Check function arguments */
  if(msgBody == NULL) {
    serrno = EINVAL;
    return -1;
  }

  return
    LONGSIZE + /* volReqID           */
    LONGSIZE + /* clientCallbackPort */
    LONGSIZE + /* clientUID          */
    LONGSIZE + /* clientGID          */
    strlen(msgBody->clientHost) + 1 +
    strlen(msgBody->dgn)        + 1 +
    strlen(msgBody->drive)      + 1 +
    strlen(msgBody->clientName) + 1;
}

/******************************************************************************
 * rtcp_marshallVdqmClientInfoMsg
 *****************************************************************************/
int32_t rtcp_marshallVdqmClientInfoMsg(char *const buf,
  const size_t bufLen, vdqmClientInfoMsgBody_t *const msgBody) {

  const int32_t msgHdrLen = 3 * LONGSIZE; /* magic + reqtype + msgBodyLen */
  int32_t msgBodyLen = 0;
  int32_t nbBytesMarshalled = 0;
  char *p = buf;

  /* Check function arguments */
  if(buf == NULL || bufLen < VDQMCLIENTINFOMSGBODY_MAXSIZE ||
    msgBody == NULL) {
    serrno = EINVAL;
    return -1;
  }

  msgBodyLen = rtcp_vdqmClientInfoMsgBodyMarshalledSize(msgBody);
  if(msgBodyLen < 0) {
    return -1;
  }

  marshall_LONG  (p, RTCOPY_MAGIC_OLD0          );
  marshall_LONG  (p, VDQM_CLIENTINFO            );
  marshall_LONG  (p, msgBodyLen                 );
  marshall_LONG  (p, msgBody->volReqId          );
  marshall_LONG  (p, msgBody->clientCallbackPort);
  marshall_LONG  (p, msgBody->clientUID         );
  marshall_LONG  (p, msgBody->clientGID         );
  marshall_STRING(p, msgBody->clientHost        );
  marshall_STRING(p, msgBody->dgn               );
  marshall_STRING(p, msgBody->drive             );
  marshall_STRING(p, msgBody->clientName        );

  nbBytesMarshalled = p - buf;

  /* Check actual number of bytes marshalled equals expected value */
  if(nbBytesMarshalled != msgHdrLen + msgBodyLen) {
    serrno = SEINTERNAL;
    return -1;
  }

  return nbBytesMarshalled;
}

/******************************************************************************
 * rtcp_unmarshallVdqmClientInfoMsgBody
 *****************************************************************************/
int32_t rtcp_unmarshallVdqmClientInfoMsgBody(char *const buf,
  const size_t bufLen, vdqmClientInfoMsgBody_t *const msgBody) {

  char *p = buf;
  int32_t nbBytesUnmarshalled = 0;

  /* Check function arguments */
  if(buf == NULL || bufLen < VDQMCLIENTINFOMSGBODY_MINSIZE || msgBody == NULL) {
    serrno = EINVAL;
    return -1;
  }

  unmarshall_LONG(p, msgBody->volReqId);
  unmarshall_LONG(p, msgBody->clientCallbackPort);
  unmarshall_LONG(p, msgBody->clientUID);
  unmarshall_LONG(p, msgBody->clientGID);
  unmarshall_STRING(p, msgBody->clientHost);
  unmarshall_STRING(p, msgBody->dgn);
  unmarshall_STRING(p, msgBody->drive);
  unmarshall_STRING(p, msgBody->clientName);

  nbBytesUnmarshalled = p - buf;

  return nbBytesUnmarshalled;
}
