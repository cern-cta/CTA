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
      m_globalConfiguration(globalConfig) {
  setCommandLineHasBeenParsed(commandLine.foreground);
}

TapeDaemon::~TapeDaemon() {
  google::protobuf::ShutdownProtobufLibrary();
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int TapeDaemon::mainImpl() {
  try {
    exceptionThrowingMain();
  } catch (cta::exception::NoSuchObject& ex) {
    m_log(log::ERR, "Aborting cta-taped. Not starting because: " + ex.getMessage().str());
    return EXIT_FAILURE;
  } catch (cta::exception::Exception& ex) {
    // Log the error
    m_log(log::ERR,
          "Aborting cta-taped on uncaught exception. Stack trace follows.",
          {
            {"exceptionMessage", ex.getMessage().str()}
    });
    log::LogContext lc(m_log);
    lc.logBacktrace(log::INFO, ex.backtrace());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeDaemon::exceptionThrowingMain() {
  daemonizeIfNotRunInForeground();
  setDumpable();
  mainEventLoop();
}

//------------------------------------------------------------------------------
// mainEventLoop
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeDaemon::mainEventLoop() {
  // Create the log context
  log::LogContext lc(m_log);
  // Set process name
  const auto processName = m_globalConfiguration.constructProcessName(lc, "parent");
  prctl(PR_SET_NAME, processName.c_str());
  // Initialise telemetry only after the process name is available
  cta::telemetry::reinitTelemetry(lc);
  // Create the process manager and signal handler
  ProcessManager pm(lc);
  auto sh = std::make_unique<SignalHandler>(pm);
  pm.addHandler(std::move(sh));
  // Create the drive handler
  const DriveConfigEntry dce {m_globalConfiguration.driveName.value(),
                              m_globalConfiguration.driveLogicalLibrary.value(),
                              m_globalConfiguration.driveDevice.value(),
                              m_globalConfiguration.driveControlPath.value()};
  auto dh = std::make_unique<DriveHandler>(m_globalConfiguration, dce, pm);
  pm.addHandler(std::move(dh));

  // And run the process manager
  int ret = pm.run();
  {
    log::ScopedParamContainer param(lc);
    param.add("returnValue", ret);
  }
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
