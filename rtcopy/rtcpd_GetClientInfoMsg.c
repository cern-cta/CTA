/******************************************************************************
 *                 rtcopy/rtcpd_GetClientInfoMsg.c
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

#include "h/rtcp_constants.h"
#include "h/rtcp_marshallVdqmClientInfoMsg.h"
#include "h/rtcp_recvRtcpHdr.h"
#include "h/rtcpd_GetClientInfoMsg.h"
#include "h/serrno.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"
#include "h/vdqm_constants.h"
#include "h/vdqmClientInfoMsgBody.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>

int rtcpd_GetClientInfoMsg(
  const SOCKET                   connSock,
  const int                      netTimeout,
  rtcpHdr_t                      *const msgHdr,
  rtcpTapeRequest_t              *const tapeReq,
  rtcpFileRequest_t              *const fileReq,
  rtcpClientInfo_t               *const client,
  int                            *const clientIsTapeBridge,
  tapeBridgeClientInfo2MsgBody_t *const tapeBridgeClientInfo2MsgBody,
  char                           *const errBuf,
  const size_t                   errBufLen) {
  int  rc                = 0;
  char *errmsg           = NULL;
  int  save_serrno       = 0;
  int  netread_serrno    = 0;
  char msgBuf[RTCP_MSGBUFSIZ];

  /* Client is not the tape-bridge until proven otherwise */
  *clientIsTapeBridge = FALSE;

  /* Check function-parameters */
  if(INVALID_SOCKET == connSock || NULL == tapeReq || NULL == fileReq ||
    NULL == client || NULL == errBuf) {
    snprintf(errBuf, errBufLen, "%s()"
      ": Invalid function-parameter",
      __FUNCTION__);
    errBuf[errBufLen - 1] = '\0';
    serrno = EINVAL;
    return -1;
  }

  /* Read in the header of the rtcpd message from the VDQM or tape-bridge */
  rc = rtcp_recvRtcpHdr(connSock, msgHdr, netTimeout);
  save_serrno = serrno;
  if (-1 == rc) {
    errmsg = sstrerror(save_serrno);
    if(NULL == errmsg) {
      errmsg = "Unknown error";
    }
    snprintf(errBuf, errBufLen, "%s()"
      ": rtcp_recvRtcpHdr()"
      ": %s",
      __FUNCTION__, errmsg);
    errBuf[errBufLen - 1] = '\0';
    serrno = save_serrno;
    return -1;
  }

  /* Check the magic number of the message header */
  if(RTCOPY_MAGIC_OLD0 != msgHdr->magic) {
    snprintf(errBuf, errBufLen, "%s()"
      ": Invalid client-info message-header"
      ": Invalid magic number"
      ": expected=0x%x actual=0x%x",
      __FUNCTION__, RTCOPY_MAGIC_OLD0, msgHdr->magic);
    errBuf[errBufLen - 1] = '\0';
    serrno = SEBADVERSION;
    return -1;
  }

  /* Check the request-type of the message-header */
  if(msgHdr->reqtype != VDQM_CLIENTINFO &&
    msgHdr->reqtype != TAPEBRIDGE_CLIENTINFO2) {
    snprintf(errBuf, errBufLen, "%s()"
      ": Invalid client-info message-header"
      ": Invalid request type"
      ": expected=0x%x or 0x%x actual=0x%x",
      __FUNCTION__, VDQM_CLIENTINFO, TAPEBRIDGE_CLIENTINFO2, msgHdr->reqtype);
    errBuf[errBufLen - 1] = '\0';
    serrno = EINVAL;
    return -1;
  }

  /* Check the message-body given in the message-header */
  if(!VALID_MSGLEN(msgHdr->len)) {
    snprintf(errBuf, errBufLen, "%s()"
      "Invalid client-info message-header"
      ": Invalid message-body length"
      ": actual=%d",
      __FUNCTION__, msgHdr->len);
    errBuf[errBufLen - 1] = '\0';
    serrno = SEUMSG2LONG;
    return -1;
  }

  /* Receive the body of the message from the VDQM or tape-bridge */
  memset(msgBuf, '\0', sizeof(msgBuf));
  rc = netread_timeout(connSock, msgBuf, msgHdr->len, RTCP_NETTIMEOUT);
  save_serrno = serrno;
  if(-1 == rc) {
    errmsg = sstrerror(netread_serrno);
    if(NULL == errmsg) {
      errmsg = "Unknown error";
    }
    snprintf(errBuf, errBufLen, "%s()"
      "Failed to receive client-into message-body"
      ": netread_timeout()"
      ": %s",
      __FUNCTION__, errmsg);
    errBuf[errBufLen - 1] = '\0';
    serrno = save_serrno;
    return(-1);
  }

  /* Un-marshall the body of the message received from the VDQM or */
  /* tape-bridge                                                   */
  memset(tapeReq, '\0', sizeof(*tapeReq));
  memset(fileReq, '\0', sizeof(*fileReq));
  memset(client,  '\0', sizeof(*client));
  switch(msgHdr->reqtype) {
  case VDQM_CLIENTINFO:
    {
      vdqmClientInfoMsgBody_t vdqmClientInfoMsgBody;

      memset(&vdqmClientInfoMsgBody, '\0', sizeof(vdqmClientInfoMsgBody));
      rc = rtcp_unmarshallVdqmClientInfoMsgBody(msgBuf, sizeof(msgBuf),
        &vdqmClientInfoMsgBody);
      save_serrno = serrno;
      if(0 > rc) {
        errmsg = sstrerror(save_serrno);
        if(NULL == errmsg) {
          errmsg = "Unknown error";
        }
        snprintf(errBuf, errBufLen, "%s()"
          ": rtcp_unmarshallVdqmClientInfoMsgBody()"
          ": %s",
          __FUNCTION__, errmsg);
        errBuf[errBufLen - 1] = '\0';
        serrno = save_serrno;
        return(-1);
      }

      /* Copy VDQM client information data into the necessary rtcpd */
      /* data-structures                                            */
      strncpy(client->clienthost, vdqmClientInfoMsgBody.clientHost,
        sizeof(client->clienthost));
      client->clienthost[sizeof(client->clienthost)-1] = '\0';
      client->clientport = vdqmClientInfoMsgBody.clientCallbackPort;
      client->VolReqID = vdqmClientInfoMsgBody.volReqId;
      client->uid = vdqmClientInfoMsgBody.clientUID;
      client->gid = vdqmClientInfoMsgBody.clientGID;
      strncpy(client->name, vdqmClientInfoMsgBody.clientName,
        sizeof(client->name));
      client->name[sizeof(client->name)-1] = '\0';
      strncpy(tapeReq->dgn, vdqmClientInfoMsgBody.dgn, sizeof(tapeReq->dgn));
      tapeReq->dgn[sizeof(tapeReq->dgn)-1] = '\0';
      strncpy(tapeReq->unit, vdqmClientInfoMsgBody.drive,
        sizeof(tapeReq->unit));
      tapeReq->unit[sizeof(tapeReq->unit)-1] = '\0';
    }
    break;
  case TAPEBRIDGE_CLIENTINFO2:
    {
      tapeBridgeClientInfo2MsgBody_t bridgeClientInfo2MsgBody;

      memset(&bridgeClientInfo2MsgBody, '\0', sizeof(bridgeClientInfo2MsgBody));
      rc = tapebridge_unmarshallTapeBridgeClientInfo2MsgBody(msgBuf,
        sizeof(msgBuf), &bridgeClientInfo2MsgBody);
      save_serrno = serrno;
      if(0 > rc) {
        errmsg = sstrerror(save_serrno);
        if(NULL == errmsg) {
          errmsg = "Unknown error";
        }
        snprintf(errBuf, errBufLen, "%s()"
          ": tapebridge_unmarshallTapeBridgeClientInfo2MsgBody()"
          ": %s",
          __FUNCTION__, errmsg);
        errBuf[errBufLen - 1] = '\0';
        serrno = save_serrno;
        return(-1);
      }

      /* Check the flushTapeMode field has a valid value */
      if(bridgeClientInfo2MsgBody.tapeFlushMode !=
           TAPEBRIDGE_N_FLUSHES_PER_FILE &&
        bridgeClientInfo2MsgBody.tapeFlushMode !=
           TAPEBRIDGE_ONE_FLUSH_PER_FILE &&
        bridgeClientInfo2MsgBody.tapeFlushMode !=
           TAPEBRIDGE_ONE_FLUSH_PER_N_FILES) {
        snprintf(errBuf, errBufLen, "%s()"
          ": Invalid tapeFlushMode"
          ": value=%d",
          __FUNCTION__, bridgeClientInfo2MsgBody.tapeFlushMode);
        errBuf[errBufLen - 1] = '\0';
        serrno = EBADMSG;
        return(-1);
      }

      /* Copy the client information data into the necessary rtcpd */
      /* data-structures                                           */
      strncpy(client->clienthost, bridgeClientInfo2MsgBody.bridgeHost,
        sizeof(client->clienthost));
      client->clienthost[sizeof(client->clienthost)-1] = '\0';
      client->clientport = bridgeClientInfo2MsgBody.bridgeCallbackPort;
      client->VolReqID = bridgeClientInfo2MsgBody.volReqId;
      client->uid = bridgeClientInfo2MsgBody.clientUID;
      client->gid = bridgeClientInfo2MsgBody.clientGID;
      strncpy(client->name, bridgeClientInfo2MsgBody.clientName,
        sizeof(client->name));
      client->name[sizeof(client->name)-1] = '\0';
      strncpy(tapeReq->dgn, bridgeClientInfo2MsgBody.dgn, sizeof(tapeReq->dgn));
      tapeReq->dgn[sizeof(tapeReq->dgn)-1] = '\0';
      strncpy(tapeReq->unit, bridgeClientInfo2MsgBody.drive,
        sizeof(tapeReq->unit));
      tapeReq->unit[sizeof(tapeReq->unit)-1] = '\0';

      /* Record the fact the client is the tape-bridge daemon */
      memcpy(tapeBridgeClientInfo2MsgBody, &bridgeClientInfo2MsgBody,
        sizeof(*tapeBridgeClientInfo2MsgBody));
      *clientIsTapeBridge = TRUE;
    }
    break;
  default:
    snprintf(errBuf, errBufLen, "%s()"
      ": Invalid client-info message-header"
      ": Invalid request type"
      ": expected=0x%x or 0x%x actual=0x%x",
      __FUNCTION__, VDQM_CLIENTINFO, TAPEBRIDGE_CLIENTINFO2, msgHdr->reqtype);
    errBuf[errBufLen - 1] = '\0';
    serrno = EINVAL;
    return -1;
  } /* switch(hdr.reqtype) */

  return 0;
}
