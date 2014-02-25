/******************************************************************************
 *                      MountSession.cpp
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

#include "MountSession.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/exception/Exception.hpp"
#include "ClientInterface.hpp"
#include "log.h"
#include "stager_client_commandline.h"

using namespace castor::tape::server;
using namespace castor::log;

castor::tape::server::MountSession::MountSession(
    const legacymsg::RtcpJobRqstMsgBody& clientRequest, 
    castor::log::Logger& logger): 
    m_request(clientRequest), m_logger(logger), m_clientIf(clientRequest) {}

void castor::tape::server::MountSession::execute() 
throw (castor::tape::Exception) {
  // 1) Prepare the logging environment
  LogContext lc(m_logger);
  LogContext::ScopedParam sp01(lc, Param("clientHost", m_request.clientHost));
  LogContext::ScopedParam sp02(lc, Param("clientPort", m_request.clientPort));
  LogContext::ScopedParam sp03(lc, Param("mountTransactionId", m_request.volReqId));
  LogContext::ScopedParam sp04(lc, Param("volReqId", m_request.volReqId));
  LogContext::ScopedParam sp05(lc, Param("driveUnit", m_request.driveUnit));
  LogContext::ScopedParam sp06(lc, Param("dgn", m_request.dgn));
  // 2) Get initial information from the client
  try {
    ClientInterface::RequestReport reqReport;
    m_clientIf.fetchVolumeId(m_volInfo, reqReport);
    LogContext::ScopedParam sp07(lc, Param("tapebridgeTransId", reqReport.transactionId));
    LogContext::ScopedParam sp08(lc, Param("connectDuration", reqReport.connectDuration));
    LogContext::ScopedParam sp09(lc, Param("sendRecvDuration", reqReport.sendRecvDuration));
    LogContext::ScopedParam sp10(lc, Param("TPVID", m_volInfo.vid));
    LogContext::ScopedParam sp11(lc, Param("density", m_volInfo.vid));
    LogContext::ScopedParam sp12(lc, Param("label", m_volInfo.vid));
    LogContext::ScopedParam sp13(lc, Param("clientType", m_volInfo.vid));
    LogContext::ScopedParam sp14(lc, Param("mode", m_volInfo.vid));
    lc.log(LOG_INFO, "Got volume from client");
  } catch(ClientInterface::EndOfSession & eof) {
    ClientInterface::RequestReport reqReport;
    std::stringstream fullError("Failed to get volume from client");
    fullError << eof.what();
    lc.log(LOG_ERR, fullError.str());
    m_clientIf.reportEndOfSession(reqReport);
    LogContext::ScopedParam sp07(lc, Param("tapebridgeTransId", reqReport.transactionId));
    LogContext::ScopedParam sp08(lc, Param("connectDuration", reqReport.connectDuration));
    LogContext::ScopedParam sp09(lc, Param("sendRecvDuration", reqReport.sendRecvDuration));
    LogContext::ScopedParam sp10(lc, Param("ErrorMsg", fullError.str()));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return;
  }
  // Make the TPVID parameter permanent.
  LogContext::ScopedParam sp6(lc, Param("TPVID", m_request.dgn));
  
  // Depending on the type of session, branch into the right execution
  // TbC...
}
