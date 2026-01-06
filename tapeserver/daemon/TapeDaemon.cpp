/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "tapeserver/daemon/TapeDaemon.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/CmdLineParams.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/process/ProcessCap.hpp"
#include "common/telemetry/TelemetryInit.hpp"
#include "common/utils/utils.hpp"
#include "tapeserver/daemon/DriveConfigEntry.hpp"
#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/daemon/ProcessManager.hpp"
#include "tapeserver/daemon/SignalHandler.hpp"

#include <google/protobuf/service.h>
#include <limits.h>
#include <sys/prctl.h>

namespace cta::tape::daemon {

TapeDaemon::TapeDaemon(const cta::common::CmdLineParams& commandLine,
                       log::Logger& log,
                       const common::TapedConfiguration& globalConfig)
    : cta::server::Daemon(log),
      m_globalConfiguration(globalConfig) {}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
void TapeDaemon::run() {
  setDumpable();
  // Create the log context
  log::LogContext lc(m_log);
  // Set process name
  const auto processName = m_globalConfiguration.constructProcessName(lc, "taped");
  prctl(PR_SET_NAME, processName.c_str());
  const DriveConfigEntry dce {m_globalConfiguration.driveName.value(),
                              m_globalConfiguration.driveLogicalLibrary.value(),
                              m_globalConfiguration.driveDevice.value(),
                              m_globalConfiguration.driveControlPath.value()};

  DriveHandler driveHandler(m_globalConfiguration, dce);
  driveHandler.run();
  lc.log(log::INFO, "cta-taped exiting.");
  ::exit(ret);
}

//------------------------------------------------------------------------------
// setDumpable
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeDaemon::setDumpable() {
  cta::utils::setDumpableProcessAttribute(true);
  const bool dumpable = cta::utils::getDumpableProcessAttribute();
  std::list<cta::log::Param> params = {cta::log::Param("dumpable", dumpable ? "true" : "false")};
  m_log(log::INFO, "Got dumpable attribute of process", params);
  if (!dumpable) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to set dumpable attribute of process to true";
    throw ex;
  }
}
}  // namespace cta::tape::daemon
