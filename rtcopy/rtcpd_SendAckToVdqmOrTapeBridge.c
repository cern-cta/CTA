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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "h/rtcpd_SendAckToVdqmOrTapeBridge.h"
#include "h/serrno.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_sendTapeBridgeClientInfo2Ack.h"
#include "h/vdqm_api.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <stdio.h>

int rtcpd_SendAckToVdqmOrTapeBridge(
  const int      connSock,
  const int      netTimeout,
  const uint32_t reqType,
  const int32_t  ackStatus,
  char *const    ackMsg,
  char *const    errBuf,
  const size_t   errBufLen) {
  int  rc          = 0;
  int  save_serrno = 0;
  const char *errmsg = NULL;

  /* Check function-parameters */
  if(connSock == -1 ||
    (reqType != VDQM_CLIENTINFO && reqType != TAPEBRIDGE_CLIENTINFO2) || ackMsg == NULL) {
    snprintf(errBuf, errBufLen, "%s()"
      ": Invalid function-parameter",
      __FUNCTION__);
    errBuf[errBufLen - 1] = '\0';
    serrno = EINVAL;
    return -1;
  }

  switch(reqType) {
  case VDQM_CLIENTINFO:
    rc = vdqm_AcknClientAddr(connSock, reqType, ackStatus, ackMsg);
    save_serrno = serrno;
    if(-1 == rc) {
      errmsg = sstrerror(save_serrno);
      if(NULL == errmsg) {
        errmsg = "Unknown error";
      }
      snprintf(errBuf, errBufLen, "%s()"
        ": vdqm_AcknClientAddr()"
        ": %s",
        __FUNCTION__, errmsg);
      errBuf[errBufLen - 1] = '\0';
      serrno = save_serrno;
      return -1;
    }
    break;
  case TAPEBRIDGE_CLIENTINFO2:
    rc = tapebridge_sendTapeBridgeClientInfo2Ack(connSock, netTimeout,
      ackStatus, ackMsg);
    save_serrno = serrno;
    if(-1 == rc) {
      errmsg = sstrerror(save_serrno);
      if(NULL == errmsg) {
        errmsg = "Unknown error";
      }
      snprintf(errBuf, errBufLen, "%s()"
        ": tapebridge_sendTapeBridgeClientInfo2Ack()"
        ": %s",
        __FUNCTION__, errmsg);
      errBuf[errBufLen - 1] = '\0';
      serrno = save_serrno;
      return -1;
    }
    break;
  }

  return 0;
}
