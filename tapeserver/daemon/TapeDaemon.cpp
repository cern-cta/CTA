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
#include <limits.h>
#include <sys/prctl.h>

#include <google/protobuf/service.h>

#include "catalogue/Catalogue.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "common/utils/utils.hpp"
#include "tapeserver/daemon/CommandLineParams.hpp"
#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/daemon/MaintenanceHandler.hpp"
#include "tapeserver/daemon/ProcessManager.hpp"
#include "tapeserver/daemon/SignalHandler.hpp"
#include "tapeserver/daemon/TapeDaemon.hpp"
#include "tapeserver/daemon/DriveConfigEntry.hpp"

namespace cta::tape::daemon {

TapeDaemon::TapeDaemon(const cta::daemon::CommandLineParams & commandLine,
    log::Logger& log,
    const common::TapedConfiguration& globalConfig):
    cta::server::Daemon(log),
    m_globalConfiguration(globalConfig),
    m_hostName(getHostName()) {
  setCommandLineHasBeenParsed(commandLine.foreground);
}

TapeDaemon::~TapeDaemon() {
  google::protobuf::ShutdownProtobufLibrary();
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int TapeDaemon::main() {
  try {
    exceptionThrowingMain();
  } catch (cta::exception::NoSuchObject &ex) {
    m_log(log::ERR, "Aborting cta-taped. Not starting because: " + ex.getMessage().str());
    return EXIT_FAILURE;
  } catch (cta::exception::Exception &ex) {
    // Log the error
    m_log(log::ERR, "Aborting cta-taped on uncaught exception. Stack trace follows.", {{"exceptionMessage", ex.getMessage().str()}});
    log::LogContext lc(m_log);
    lc.logBacktrace(log::INFO, ex.backtrace());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

//------------------------------------------------------------------------------
// getHostName
//------------------------------------------------------------------------------
std::string cta::tape::daemon::TapeDaemon::getHostName() const {
  char nameBuf[HOST_NAME_MAX + 1];
  if(gethostname(nameBuf, sizeof(nameBuf)))
    throw cta::exception::Errnum("Failed to get host name");
  return nameBuf;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
void  cta::tape::daemon::TapeDaemon::exceptionThrowingMain()  {
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
  // Create the process manager and signal handler
  ProcessManager pm(lc);
  auto sh = std::make_unique<SignalHandler>(pm);
  pm.addHandler(std::move(sh));
  // Create the drive handler
  const DriveConfigEntry dce{m_globalConfiguration.driveName.value(),
                             m_globalConfiguration.driveLogicalLibrary.value(),
                             m_globalConfiguration.driveDevice.value(),
                             m_globalConfiguration.driveControlPath.value()};

  const auto shortName = cde.getShortUnitName();
  // Used in the process name, which can be a max of 16 bytes
  // We need at least 2 characters for the suffix and 1 for the null character
  if (shortName.size() > 13) {
    lc.log(log::WARN,
           "DriveConfigEntry::DriveConfigEntry - short unitName '" + shortName + "' exceeds max length of " +
             std::to_string(maxShortDriveNameLen) + " (got " + std::to_string(shortName.size()) + ")");
  }
  std::string processName = dce.getShortUnitName() + "-parent";
  prctl(PR_SET_NAME, processName.c_str());
  auto dh = std::make_unique<DriveHandler>(m_globalConfiguration,
                                           dce,
                                           pm);
  pm.addHandler(std::move(dh));

  // Create the garbage collector
  if(!isMaintenanceProcessDisabled()){
    auto gc = std::make_unique<MaintenanceHandler>(m_globalConfiguration, pm);
    pm.addHandler(std::move(gc));
  } else {
    lc.log(log::INFO,"In TapeDaemon::mainEventLoop, the Maintenance process is disabled from the configuration. Will not run it.");
  }
  // And run the process manager
  int ret=pm.run();
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
  std::list<cta::log::Param> params = {
    cta::log::Param("dumpable", dumpable ? "true" : "false")};
  m_log(log::INFO, "Got dumpable attribute of process", params);
  if(!dumpable) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to set dumpable attribute of process to true";
    throw ex;
  }
}

bool cta::tape::daemon::TapeDaemon::isMaintenanceProcessDisabled() const{
  return m_globalConfiguration.useMaintenanceProcess.value() == "no";
}

} // namespace cta::tape::daemon
