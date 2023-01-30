/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "tapeserver/castor/tape/tapeserver/daemon/TapeSessionReporter.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"

#include <unistd.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//-----------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
TapeSessionReporter::TapeSessionReporter(cta::tape::daemon::TapedProxy &tapeserverProxy,
                                       const cta::tape::daemon::TpconfigLine &driveConfig, const std::string &hostname,
                                       cta::log::LogContext lc)
:
  m_threadRunning(false),
  m_tapeserverProxy(tapeserverProxy),
  m_lc(lc),
  m_server(hostname),
  m_unitName(driveConfig.unitName),
  m_logicalLibrary(driveConfig.logicalLibrary),
  m_sessionPid(getpid()) {
  //change the thread's name in the log
  m_lc.pushOrReplace(cta::log::Param("thread", "TapeSessionReporter"));
}

//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------
void TapeSessionReporter::finish() {
  m_fifo.push(nullptr);
}

//------------------------------------------------------------------------------
//startThreads
//------------------------------------------------------------------------------   
void TapeSessionReporter::startThreads() {
  start();
  m_threadRunning = true;
}

//------------------------------------------------------------------------------
//waitThreads
//------------------------------------------------------------------------------     
void TapeSessionReporter::waitThreads() {
  try {
    wait();
    m_threadRunning = false;
  } catch (const std::exception& e) {
    cta::log::ScopedParamContainer sp(m_lc);
    sp.add("what", e.what());
    m_lc.log(cta::log::ERR, "error caught while waiting");
  } catch (...) {
    m_lc.log(cta::log::ERR, "unknown error while waiting");
  }
}

//------------------------------------------------------------------------------
//reportState
//------------------------------------------------------------------------------  
void TapeSessionReporter::reportState(cta::tape::session::SessionState state,
                                     cta::tape::session::SessionType type) {
  m_fifo.push(new ReportStateChange(state, type));
}

//------------------------------------------------------------------------------
//run
//------------------------------------------------------------------------------  
void TapeSessionReporter::run() {
  while (true) {
    std::unique_ptr<Report> currentReport(m_fifo.pop());
    if (nullptr == currentReport) {
      break;
    }
    try {
      currentReport->execute(*this);
    } catch (const std::exception& e) {
      cta::log::ScopedParamContainer sp(m_lc);
      sp.add("what", e.what());
      m_lc.log(cta::log::ERR, "TapeSessionReporter error caught");
    }
  }
}

//------------------------------------------------------------------------------
// ReportStateChange::bailout())
//------------------------------------------------------------------------------
void TapeSessionReporter::bailout() {
  // Send terminating event to the queue
  finish();
  // Consume queue and exit
  run();
}

//------------------------------------------------------------------------------
// ReportStateChange::ReportStateChange())
//------------------------------------------------------------------------------
TapeSessionReporter::ReportStateChange::ReportStateChange(cta::tape::session::SessionState state,
                                                         cta::tape::session::SessionType type) : m_state(state), m_type(type) {}

//------------------------------------------------------------------------------
// ReportStateChange::execute())
//------------------------------------------------------------------------------  
void TapeSessionReporter::ReportStateChange::execute(TapeSessionReporter & parent) {
  parent.m_tapeserverProxy.reportState(m_state, m_type, parent.m_volume.vid);
}

}
}
}
} // namespace castor::tape::tapeserver::daemon

