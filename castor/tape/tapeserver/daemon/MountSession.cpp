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
#include "ClientProxy.hpp"
#include "log.h"
#include "stager_client_commandline.h"
#include "castor/tape/utils/utils.hpp"
#include "castor/System.hpp"
#include "h/serrno.h"
#include "castor/tape/tapeserver/SCSI/Device.hpp"

using namespace castor::tape;
using namespace castor::log;

castor::tape::tapeserver::daemon::MountSession::MountSession(
    const legacymsg::RtcpJobRqstMsgBody& clientRequest, 
    castor::log::Logger& logger, System::virtualWrapper & sysWrapper,
    const utils::TpconfigLines & tpConfig): 
    m_request(clientRequest), m_logger(logger), m_clientIf(clientRequest),
    m_sysWrapper(sysWrapper), m_tpConfig(tpConfig) {}

void castor::tape::tapeserver::daemon::MountSession::execute()
throw (castor::tape::Exception) {
  // 1) Prepare the logging environment
  LogContext lc(m_logger);
  LogContext::ScopedParam sp01(lc, Param("clientHost", m_request.clientHost));
  LogContext::ScopedParam sp02(lc, Param("clientPort", m_request.clientPort));
  LogContext::ScopedParam sp03(lc, Param("mountTransactionId", m_request.volReqId));
  LogContext::ScopedParam sp04(lc, Param("volReqId", m_request.volReqId));
  LogContext::ScopedParam sp05(lc, Param("driveUnit", m_request.driveUnit));
  LogContext::ScopedParam sp06(lc, Param("dgn", m_request.dgn));
  // 2a) Get initial information from the client
  ClientProxy::RequestReport reqReport;
  try {
    m_clientIf.fetchVolumeId(m_volInfo, reqReport);
  } catch(ClientProxy::EndOfSession & eof) {
    std::stringstream fullError("Received end of session from client when requesting Volume");
    fullError << eof.what();
    lc.log(LOG_ERR, fullError.str());
    m_clientIf.reportEndOfSession(reqReport);
    LogContext::ScopedParam sp07(lc, Param("tapebridgeTransId", reqReport.transactionId));
    LogContext::ScopedParam sp08(lc, Param("connectDuration", reqReport.connectDuration));
    LogContext::ScopedParam sp09(lc, Param("sendRecvDuration", reqReport.sendRecvDuration));
    LogContext::ScopedParam sp10(lc, Param("ErrorMsg", fullError.str()));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return;
  } catch (ClientProxy::UnexpectedResponse & unexp) {
    std::stringstream fullError("Received unexpected response from client when requesting Volume");
    fullError << unexp.what();
    lc.log(LOG_ERR, fullError.str());
    m_clientIf.reportEndOfSession(reqReport);
    LogContext::ScopedParam sp07(lc, Param("tapebridgeTransId", reqReport.transactionId));
    LogContext::ScopedParam sp08(lc, Param("connectDuration", reqReport.connectDuration));
    LogContext::ScopedParam sp09(lc, Param("sendRecvDuration", reqReport.sendRecvDuration));
    LogContext::ScopedParam sp10(lc, Param("ErrorMsg", fullError.str()));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return;
  }
  // 2b) ... and log.
  // Make the TPVID parameter permanent.
  LogContext::ScopedParam sp07(lc, Param("TPVID", m_request.dgn));
  {
    LogContext::ScopedParam sp08(lc, Param("tapebridgeTransId", reqReport.transactionId));
    LogContext::ScopedParam sp09(lc, Param("connectDuration", reqReport.connectDuration));
    LogContext::ScopedParam sp00(lc, Param("sendRecvDuration", reqReport.sendRecvDuration));
    LogContext::ScopedParam sp11(lc, Param("TPVID", m_volInfo.vid));
    LogContext::ScopedParam sp12(lc, Param("density", m_volInfo.density));
    LogContext::ScopedParam sp13(lc, Param("label", m_volInfo.labelObsolete));
    LogContext::ScopedParam sp14(lc, Param("clientType", utils::volumeClientTypeToString(m_volInfo.clientType)));
    LogContext::ScopedParam sp15(lc, Param("mode", utils::volumeModeToString(m_volInfo.volumeMode)));
    lc.log(LOG_INFO, "Got volume from client");
  }

  
  // Depending on the type of session, branch into the right execution
  switch(m_volInfo.volumeMode) {
  case tapegateway::READ:
    executeRead(lc);
    return;
  case tapegateway::WRITE:
    executeWrite(lc);
    return;
  case tapegateway::DUMP:
    executeDump(lc);
    return;
  }
}

void castor::tape::tapeserver::daemon::MountSession::executeRead(LogContext & lc) {
  // We are ready to start the session. In case of read there is no interest in
  // creating the machinery before getting the tape mounted, so do it now.
  // 1) Get hold of the drive and check it.
  utils::TpconfigLines::iterator i;
  for (i = m_tpConfig.begin(); i != m_tpConfig.end(); i++) {
    if (i->unitName == m_request.driveUnit && i->density == m_volInfo.density) {
      break;
    }
  }
  // If we did not find the drive in the tpConfig, we have a problem
  if (i == m_tpConfig.end()) {
    LogContext::ScopedParam sp08(lc, Param("density", m_volInfo.density));
    lc.log(LOG_ERR, "Drive unit not found");
    
    ClientProxy::RequestReport reqReport;
    std::stringstream errMsg("Drive unit not found");
    errMsg << lc;
    m_clientIf.reportEndOfSessionWithError("Drive unit not found", SEINTERNAL, reqReport);
    LogContext::ScopedParam sp09(lc, Param("tapebridgeTransId", reqReport.transactionId));
    LogContext::ScopedParam sp10(lc, Param("connectDuration", reqReport.connectDuration));
    LogContext::ScopedParam sp11(lc, Param("sendRecvDuration", reqReport.sendRecvDuration));
    LogContext::ScopedParam sp12(lc, Param("errorMessage", errMsg.str()));
    LogContext::ScopedParam sp13(lc, Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return;
  }
  // Actually find the drive.
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
}

void castor::tape::tapeserver::daemon::MountSession::executeWrite(LogContext & lc) {
}

void castor::tape::tapeserver::daemon::MountSession::executeDump(LogContext & lc) {
  // We are ready to start the session. In case of read there is no interest in
  // creating the machinery before getting the tape mounted, so do it now.
  // 1) Get hold of the drive and check it.
  
}
