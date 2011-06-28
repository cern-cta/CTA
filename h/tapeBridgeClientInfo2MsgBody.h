/******************************************************************************
 *                h/tapeBridgeClientInfo2MsgBody.h
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

#ifndef H_TAPEBRIDGECLIENTINFO2MSGBODY_H
#define H_TAPEBRIDGECLIENTINFO2MSGBODY_H 1

#include "h/Castor_limits.h"
#include "h/osdep.h"

#include <stdint.h>

/**
 * The body of a TAPEBRIDGE_CLIENTINFO2 message.
 *
 * The TAPEBRIDGE_CLIENTINFO2 message has two more members than the
 * TAPEBRIDGE_CLIENTINFO_DEPRECATED message.  The two extra members are as
 * follows:
 *
 * maxBytesBeforeFlush The maximum number of bytes before the rtcpd daemon
 * should flush all data to tape.  Please note that the rtcpd daemon flushes
 * data to tape only when it has finished writing an entire file, this means
 * rtcpd will nearly always write a few more bytes than maxBytesBeforeFlush
 * before flushing to tape.
 *
 * maxFilesBeforeFlush The maximum number of files before the rtcpd daemon
 * should flush all data to tape.
 */
typedef struct {
  uint32_t volReqId;
  uint32_t bridgeCallbackPort;
  uint32_t bridgeClientCallbackPort;
  uint32_t clientUID;
  uint32_t clientGID;
  uint32_t useBufferedTapeMarksOverMultipleFiles;
  uint64_t maxBytesBeforeFlush;
  uint64_t maxFilesBeforeFlush;
  char     bridgeHost[CA_MAXHOSTNAMELEN+1];
  char     bridgeClientHost[CA_MAXHOSTNAMELEN+1];
  char     dgn[CA_MAXDGNLEN+1];
  char     drive[CA_MAXUNMLEN+1];
  char     clientName[CA_MAXUSRNAMELEN+1];
} tapeBridgeClientInfo2MsgBody_t;

#define TAPEBRIDGECLIENTINFO2MSGBODY_MAXSIZE (                           \
    LONGSIZE              + /* volReqID                              */ \
    LONGSIZE              + /* bridgeCallbackPort                    */ \
    LONGSIZE              + /* bridgeClientCallbackPort              */ \
    LONGSIZE              + /* clientUID                             */ \
    LONGSIZE              + /* clientGID                             */ \
    LONGSIZE              + /* useBufferedTapeMarksOverMultipleFiles */ \
    HYPERSIZE             + /* maxBytesBeforeFlush                   */ \
    HYPERSIZE             + /* maxFilesBeforeFlush                   */ \
    CA_MAXHOSTNAMELEN + 1 + /* bridgeHost                            */ \
    CA_MAXHOSTNAMELEN + 1 + /* bridgeClientHost                      */ \
    CA_MAXDGNLEN      + 1 + /* dgn                                   */ \
    CA_MAXUNMLEN      + 1 + /* drive                                 */ \
    CA_MAXUSRNAMELEN  + 1   /* clientName                            */)

#define TAPEBRIDGECLIENTINFO2MSGBODY_MINSIZE (                           \
    LONGSIZE              + /* volReqID                              */ \
    LONGSIZE              + /* bridgeCallbackPort                    */ \
    LONGSIZE              + /* bridgeClientCallbackPort              */ \
    LONGSIZE              + /* clientUID                             */ \
    LONGSIZE              + /* clientGID                             */ \
    LONGSIZE              + /* useBufferedTapeMarksOverMultipleFiles */ \
    HYPERSIZE             + /* maxBytesBeforeFlush                   */ \
    HYPERSIZE             + /* maxFilesBeforeFlush                   */ \
    1                     + /* bridgeHost                            */ \
    1                     + /* bridgeClientHost                      */ \
    1                     + /* dgn                                   */ \
    1                     + /* drive                                 */ \
    1                       /* clientName                            */)

#endif /* H_TAPEBRIDGECLIENTINFO2MSGBODY_H */
