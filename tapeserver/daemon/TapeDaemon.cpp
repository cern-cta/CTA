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

#include "common/exception/Errnum.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/utils/utils.hpp"
#include "tapeserver/daemon/CommandLineParams.hpp"
#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/daemon/MaintenanceHandler.hpp"
#include "tapeserver/daemon/ProcessManager.hpp"
#include "tapeserver/daemon/SignalHandler.hpp"
#include "tapeserver/daemon/TapeDaemon.hpp"
#include "DriveHandlerProxy.hpp"

namespace cta { namespace tape { namespace daemon {

TapeDaemon::TapeDaemon(const cta::daemon::CommandLineParams & commandLine,
    log::Logger& log,
    const TapedConfiguration& globalConfig,
    cta::server::ProcessCap& capUtils):
    cta::server::Daemon(log),
    m_globalConfiguration(globalConfig), m_capUtils(capUtils),
    m_programName("cta-taped"), m_hostName(getHostName()) {
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
    m_log(log::ERR, "Aborting cta-taped on uncaught exception. Stack trace follows.", {{"Message", ex.getMessage().str()}});
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
  if (m_globalConfiguration.driveConfigs.empty())
    throw cta::exception::NoSuchObject("No drive found in configuration");

  // Process must be able to change user now and should be permitted to perform
  // raw IO in the future
  setProcessCapabilities("cap_setgid,cap_setuid+ep cap_sys_rawio+p");

  const std::string userName = m_globalConfiguration.daemonUserName.value();
  const std::string groupName = m_globalConfiguration.daemonGroupName.value();
  daemonizeIfNotRunInForegroundAndSetUserAndGroup(userName, groupName);
  setDumpable();

  // There is no longer any need for the process to be able to change user,
  // however the process should still be permitted to perform raw IO in the
  // future
  setProcessCapabilities("cap_sys_rawio+p");

  // Set the name of the (unique) thread for easy process identification.
  prctl(PR_SET_NAME, "cta-tpd-master");
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
  std::unique_ptr<SignalHandler> sh(new SignalHandler(pm));
  pm.addHandler(std::move(sh));
  // Create the drive handlers
  for (auto & d: m_globalConfiguration.driveConfigs) {
    std::unique_ptr<cta::server::SocketPair> socketPair = std::make_unique<cta::server::SocketPair>();
    DriveHandlerProxy handlerProxy(*socketPair);
    std::unique_ptr<DriveHandler> dh(
      new DriveHandler(m_globalConfiguration, d.second.value(), pm, handlerProxy, *socketPair));
    pm.addHandler(std::move(dh));
  }
  // Create the garbage collector
  if(!isMaintenanceProcessDisabled()){
    std::unique_ptr<MaintenanceHandler> gc(new MaintenanceHandler(m_globalConfiguration, pm));
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

bool cta::tape::daemon::TapeDaemon::isMaintenanceProcessDisabled() const{
  return m_globalConfiguration.useMaintenanceProcess.value() == "no";
}

}}} // namespace cta::tape::daemon
