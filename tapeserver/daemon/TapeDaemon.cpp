/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "TapeDaemon.hpp"
#include "common/exception/Errnum.hpp"
#include "common/utils/utils.hpp"
#include "tapeserver/daemon/CommandLineParams.hpp"
#include "ProcessManager.hpp"
#include "SignalHandler.hpp"
#include "DriveHandler.hpp"
#include <google/protobuf/service.h>
#include <limits.h>

namespace cta { namespace tape { namespace daemon {

TapeDaemon::TapeDaemon(const cta::daemon::CommandLineParams & commandLine, 
    log::Logger& log, 
    const TapedConfiguration& globalConfig, 
    cta::server::ProcessCap& capUtils): 
    cta::server::Daemon(log),
    m_globalConfiguration(globalConfig), m_capUtils(capUtils),
    m_programName("cta-taped"), m_hostName(getHostName()) { }

TapeDaemon::~TapeDaemon() {
  google::protobuf::ShutdownProtobufLibrary();
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int TapeDaemon::main() {
  try {
    exceptionThrowingMain();
  } catch (cta::exception::Exception &ex) {
    // Log the error
    m_log(log::ERR, "Aborting", {{"Message", ex.getMessage().str()}});
    return 1;
  }
  return 0;
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
  if(m_globalConfiguration.driveConfigs.empty())
    throw cta::exception::Exception("No drive found in configuration");

  // Process must be able to change user now and should be permitted to perform
  // raw IO in the future
  setProcessCapabilities("cap_setgid,cap_setuid+ep cap_sys_rawio+p");

  const bool runAsStagerSuperuser = true;
  daemonizeIfNotRunInForeground(runAsStagerSuperuser);
  setDumpable();

  // There is no longer any need for the process to be able to change user,
  // however the process should still be permitted to perform raw IO in the
  // future
  setProcessCapabilities("cap_sys_rawio+p");
  
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
  std::unique_ptr<SignalHandler> sh;
  pm.addHandler(std::move(sh));
  // Create the drive handlers
  for (auto & d: m_globalConfiguration.driveConfigs) {
    std::unique_ptr<DriveHandler> dh(new DriveHandler(m_globalConfiguration, d.second.value(), pm));
    pm.addHandler(std::move(dh));
  }
  // TODO: add the garbage collector once implemented
  // And run the process manager
  ::exit(pm.run());
  throw cta::exception::Exception("cta::tape::daemon::TapeDaemon::mainEventLoop: not implemented");
//  while (handleIOEvents() && handleTick() && handlePendingSignals()) {
//  }
}

//------------------------------------------------------------------------------
// setDumpable
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeDaemon::setDumpable() {
  cta::utils::setDumpableProcessAttribute(true);
  const bool dumpable = cta::utils::getDumpableProcessAttribute();
  std::list<log::Param> params = {
    log::Param("dumpable", dumpable ? "true" : "false")};
  m_log(log::INFO, "Got dumpable attribute of process", params);
  if(!dumpable) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to set dumpable attribute of process to true";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeDaemon::setProcessCapabilities(
  const std::string &text) {
  try {
    m_capUtils.setProcText(text);
    m_log(log::INFO, "Set process capabilities", 
      {{"capabilities", m_capUtils.getProcText()}});
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to set process capabilities to '" << text <<
      "': " << ne.getMessage().str();
    throw ex;
  }
}

}}} // namespace cta::tape::daemon
