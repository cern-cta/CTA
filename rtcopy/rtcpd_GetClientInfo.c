/******************************************************************************
 *                 rtcopy/rtcpd_GetClientInfo.c
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

#include "h/rtcp_server.h"
#include "h/rtcpd_GetClientInfo.h"
#include "h/rtcpd_GetClientInfoMsg.h"
#include "h/rtcpd_SendAckToVdqmOrTapeBridge.h"
#include "h/serrno.h"
#include "h/tapebridge_constants.h"
#include "h/vdqm_api.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int rtcpd_GetClientInfo(
  const SOCKET                          connSock,
  const int                             netTimeout,
  rtcpTapeRequest_t              *const tapeReq,
  rtcpFileRequest_t              *const fileReq,
  rtcpClientInfo_t               *const client,
  int                            *const clientIsTapeBridge,
  tapeBridgeClientInfo2MsgBody_t *const tapeBridgeClientInfo2MsgBody,
  char                           *const errBuf,
  const size_t                   errBufLen) {

  int rc          = 0;
  int save_serrno = 0;
  rtcpHdr_t msgHdr;

  /* Check function-parameters */
  if(INVALID_SOCKET == connSock || NULL == tapeReq || NULL == fileReq ||
    NULL == client || NULL == client || NULL == clientIsTapeBridge ||
    NULL == tapeBridgeClientInfo2MsgBody) {
    char *const errMsg = "Invalid function-parameter";

    snprintf(errBuf, errBufLen, "%s()"
      ": %s",
      __FUNCTION__, errMsg);
    errBuf[errBufLen - 1] = '\0';
    serrno = EINVAL;
    return -1;
  }

  rc = rtcpd_GetClientInfoMsg(
    connSock,
    netTimeout,
    &msgHdr,
    tapeReq,
    fileReq,
    client,
    clientIsTapeBridge,
    tapeBridgeClientInfo2MsgBody,
    errBuf,
    errBufLen);
  save_serrno = serrno;
  if(0 != rc) {
    /* Fire and forget negative acknowledgement to VDQM or tape-bridge.     */
    /* The vdqm_AcknClientAddr() is used because it has not been determined */
    /* whether or not the client is the VDQM or the tape-bridge.            */
    vdqm_AcknClientAddr(connSock, -1, strlen(errBuf), errBuf);

    /* Leave the contents of errBuf as set by rtcpd_GetClientInfoMsg */
    serrno = save_serrno;
    return(-1);
  }

  /* We only allow connections from the VDQM, tape-bridge or authorised hosts */
  if(1 != rtcp_CheckConnect(connSock,NULL)) {
    char *const errMsg = "Connection from unauthorised host";

    snprintf(errBuf, errBufLen, "%s()"
      ": %s",
      __FUNCTION__, errMsg);
    errBuf[errBufLen - 1] = '\0';
    serrno = EPERM;
    return(-1);
  }

  /* Do not acknowledge the client-info message if local nomoretapes is set. */
  /* The VDQM will then requeue the request and put the assigned drive into  */
  /* UNKNOWN status.                                                         */
  {
    struct stat st;

    if(0 == stat(NOMORETAPES, &st)) {
      char *const errMsg = "Tape service momentarily interrupted";

      snprintf(errBuf, errBufLen, "%s()"
        ": %s",
        __FUNCTION__, errMsg);
      errBuf[errBufLen - 1] = '\0';
      serrno = ECANCELED;
      return(-1);
    }
  }

  /* Send a positive acknowledgement to the VDQM or tape-bridge */
  {
    char errMsg[1024];

    rc = rtcpd_SendAckToVdqmOrTapeBridge(connSock, netTimeout,
      msgHdr.reqtype, 0, "", errMsg, sizeof(errMsg));
    save_serrno = serrno;
    if(-1 == rc) {
      snprintf(errBuf, errBufLen, "%s()"
        ": Failed to send positive ack to VDQM or tape-bridge"
        ": %s",
        __FUNCTION__, errMsg);
      errBuf[errBufLen - 1] = '\0';
      serrno = save_serrno;
      return(-1);
    }
  }

  return 0;
}
