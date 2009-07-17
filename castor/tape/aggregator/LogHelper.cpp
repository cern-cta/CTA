/******************************************************************************
 *                      castor/tape/aggregator/LogHelper.cpp
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

#include "castor/dlf/Dlf.hpp"
#include "castor/tape/aggregator/LogHelper.hpp"


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RcpJobRqstMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"       , volReqId            ),
    castor::dlf::Param("socketFd"       , socketFd            ),
    castor::dlf::Param("tapeRequestId"  , body.tapeRequestId  ),
    castor::dlf::Param("clientPort"     , body.clientPort     ),
    castor::dlf::Param("clientEuid"     , body.clientEuid     ),
    castor::dlf::Param("clientEgid"     , body.clientEgid     ),
    castor::dlf::Param("clientHost"     , body.clientHost     ),
    castor::dlf::Param("deviceGroupName", body.deviceGroupName),
    castor::dlf::Param("driveName"      , body.driveName      ),
    castor::dlf::Param("clientUserName" , body.clientUserName )};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RcpJobReplyMsgBody &body) throw() {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"    , volReqId         ),
    castor::dlf::Param("socketFd"    , socketFd         ),
    castor::dlf::Param("status"      , body.status      ),
    castor::dlf::Param("errorMessage", body.errorMessage)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, message_no, params);
}


//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpTapeRqstErrMsgBody &body) throw() {
/*
  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"    , volReqId         ),
    castor::dlf::Param("socketFd"    , socketFd         ),

    castor::dlf::Param("vid", vid);
    char vsn[CA_MAXVSNLEN+1];
    char label[CA_MAXLBLTYPLEN+1];
    char devtype[CA_MAXDVTLEN+1];
    char density[CA_MAXDENLEN+1];
    char unit[CA_MAXUNMLEN+1];
    uint32_t volReqId;      // VDQM volume request ID
    uint32_t jobId;         // Local RTCOPY server job ID
    uint32_t mode;          // WRITE_DISABLE or WRITE_ENABLE
    uint32_t start_file;    // Start file if mapped VID
    uint32_t end_file;      // End file if mapped VID
    uint32_t side;          // Disk side number
    uint32_t tprc;          // Return code from last Ctape
    uint32_t tStartRequest; // Start time of request (set by client)
    uint32_t tEndRequest;   // End time of request (set by client)
    uint32_t tStartRtcpd;   // Time when request is received by rtcpd server
    uint32_t tStartMount;   // Time when mount request is sent to Ctape
    uint32_t tEndMount;     // Time when mount request returns
    uint32_t tStartUnmount; // Time when unmount request is sent to Ctape
    uint32_t tEndUnmount;   // Time when unmount request returns
    Cuuid_t rtcpReqId;      // Unique request id assigned by RTCOPY


  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, message_no, params);
*/
}

//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpTapeRqstMsgBody &body) throw() {
}

//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpFileRqstErrMsgBody &body) throw() {
}

//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpFileRqstMsgBody &body) throw() {
}

//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const GiveOutpMsgBody &body) throw() {
}

//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpNoMoreRequestsMsgBody &body) throw() {
}

//-----------------------------------------------------------------------------
// logMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::LogHelper::logMsgBody(const Cuuid_t &cuuid,
  const int severity, const int message_no, const uint32_t volReqId,
  const int socketFd, const RtcpAbortMsgBody &body) throw() {
}
