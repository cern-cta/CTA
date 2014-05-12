/******************************************************************************
 *                      LabelSession.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#include <memory>

#include "castor/tape/tapeserver/daemon/LabelSession.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/exception/Exception.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "log.h"
#include "stager_client_commandline.h"
#include "castor/tape/utils/utils.hpp"
#include "castor/System.hpp"
#include "h/serrno.h"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include "RecallTaskInjector.hpp"
#include "RecallReportPacker.hpp"
#include "castor/tape/tapeserver/file/File.hpp"

using namespace castor::tape;
using namespace castor::log;

castor::tape::tapeserver::daemon::LabelSession::LabelSession(
    const legacymsg::TapeLabelRqstMsgBody& clientRequest, 
    castor::log::Logger& logger, System::virtualWrapper & sysWrapper,
    const utils::TpconfigLines & tpConfig, const bool force):     
    m_request(clientRequest), m_logger(logger),
    m_sysWrapper(sysWrapper), m_tpConfig(tpConfig), m_force(force) {}

void castor::tape::tapeserver::daemon::LabelSession::execute() throw (castor::tape::Exception) {
  
  // 1) Prepare the logging environment
  LogContext lc(m_logger);
  
  // Create a sticky thread name, which will be overridden by the other threads
  lc.pushOrReplace(Param("thread", "mainThread"));
  LogContext::ScopedParam sp01(lc, Param("uid", m_request.uid));
  LogContext::ScopedParam sp02(lc, Param("gid", m_request.gid));
  LogContext::ScopedParam sp03(lc, Param("vid", m_request.vid));
  
  executeLabel(lc);
}

void castor::tape::tapeserver::daemon::LabelSession::executeLabel(LogContext & lc) {
  
  // Get hold of the drive and check it.
  utils::TpconfigLines::const_iterator configLine;
  for (configLine = m_tpConfig.begin(); configLine != m_tpConfig.end(); configLine++) {
    if (configLine->unitName == m_request.drive && configLine->density == m_request.density) {
      break;
    }
  }
  
  // If we did not find the drive in the tpConfig, we have a problem
  if (configLine == m_tpConfig.end()) {
    lc.log(LOG_ERR, "Drive unit not found in TPCONFIG");
    std::stringstream errMsg;
    errMsg << "Drive unit not found in TPCONFIG" << lc;
    LogContext::ScopedParam sp12(lc, Param("errorMessage", errMsg.str()));
    LogContext::ScopedParam sp13(lc, Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "End session with error");
    return;
  }
  
  // Actually find the drive.
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
  castor::tape::SCSI::DeviceInfo driveInfo;
  try {
    driveInfo = dv.findBySymlink(configLine->devFilename);
  } catch (castor::tape::SCSI::DeviceVector::NotFound & e) {
    // We could not find this drive in the system's SCSI devices
    LogContext::ScopedParam sp09(lc, Param("devFilename", configLine->devFilename));
    lc.log(LOG_ERR, "Drive not found on this path");
    std::stringstream errMsg;
    errMsg << "Drive not found on this path" << lc;
    LogContext::ScopedParam sp13(lc, Param("errorMessage", errMsg.str()));
    LogContext::ScopedParam sp14(lc, Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "End session with error");
    return;
  } catch (castor::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    LogContext::ScopedParam sp09(lc, Param("devFilename", configLine->devFilename));
    LogContext::ScopedParam sp10(lc, Param("errorMessage", e.getMessageValue()));
    lc.log(LOG_ERR, "Error looking to path to tape drive");
    std::stringstream errMsg;
    errMsg << "Error looking to path to tape drive: " << lc;
    LogContext::ScopedParam sp14(lc, Param("errorMessage", errMsg.str()));
    LogContext::ScopedParam sp15(lc, Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "End session with error");
    return;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    LogContext::ScopedParam sp09(lc, Param("devFilename", configLine->devFilename));
    lc.log(LOG_ERR, "Unexpected exception while looking for drive");
    std::stringstream errMsg;
    errMsg << "Unexpected exception while looking for drive" << lc;
    LogContext::ScopedParam sp13(lc, Param("errorMessage", errMsg.str()));
    LogContext::ScopedParam sp14(lc, Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "End session with error");
    return;
  }
  
  // Instantiate the drive object
  std::auto_ptr<castor::tape::drives::DriveInterface> drive;
  try {
    drive.reset(castor::tape::drives::DriveFactory(driveInfo, m_sysWrapper));
  } catch (castor::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    LogContext::ScopedParam sp09(lc, Param("devFilename", configLine->devFilename));
    LogContext::ScopedParam sp10(lc, Param("errorMessage", e.getMessageValue()));
    lc.log(LOG_ERR, "Error opening tape drive");
    std::stringstream errMsg;
    errMsg << "Error opening tape drive" << lc;
    LogContext::ScopedParam sp14(lc, Param("errorMessage", errMsg.str()));
    LogContext::ScopedParam sp15(lc, Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "End session with error");
    return;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    LogContext::ScopedParam sp09(lc, Param("devFilename", configLine->devFilename));
    lc.log(LOG_ERR, "Unexpected exception while opening drive");
    std::stringstream errMsg;
    errMsg << "Unexpected exception while opening drive" << lc;
    LogContext::ScopedParam sp13(lc, Param("errorMessage", errMsg.str()));
    LogContext::ScopedParam sp14(lc, Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "End session with error");
    return;
  }
  
  // We can now start labeling
  std::auto_ptr<castor::tape::tapeFile::LabelSession> ls;
  try {
    ls.reset(new castor::tape::tapeFile::LabelSession(*drive, m_request.vid, m_force));
    lc.log(LOG_INFO, "Tape label session session successfully completed");
  } catch (castor::exception::Exception & ex) {
    lc.log(LOG_ERR, "Failed tape label session");
  }
}
