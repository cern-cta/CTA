/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/daemon/TapeSessionReporter.hpp"

#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"

#include <unistd.h>

namespace castor::tape::tapeserver::daemon {

//-----------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
TapeSessionReporter::TapeSessionReporter(cta::tape::daemon::TapedProxy& tapeserverProxy,
                                         const cta::tape::daemon::DriveConfigEntry& driveConfig,
                                         std::string_view hostname,
                                         const cta::log::LogContext& lc)
    : m_tapeserverProxy(tapeserverProxy),
      m_lc(lc),
      m_server(hostname),
      m_unitName(driveConfig.unitName),
      m_logicalLibrary(driveConfig.logicalLibrary) {
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
void TapeSessionReporter::reportState(cta::tape::session::SessionState state, cta::tape::session::SessionType type) {
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
                                                          cta::tape::session::SessionType type)
    : m_state(state),
      m_type(type) {}

//------------------------------------------------------------------------------
// ReportStateChange::execute())
//------------------------------------------------------------------------------
void TapeSessionReporter::ReportStateChange::execute(TapeSessionReporter& parent) {
  parent.m_tapeserverProxy.reportState(m_state, m_type, parent.m_volume.vid);
}

}  // namespace castor::tape::tapeserver::daemon
