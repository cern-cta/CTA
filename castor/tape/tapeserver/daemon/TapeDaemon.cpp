/******************************************************************************
 *         castor/tape/tapeserver/daemon/TapeDaemon.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/exception/Errnum.hpp"
#include "castor/exception/BadAlloc.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapeserver/daemon/AdminAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/MountSessionAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/MountSession.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"
#include "castor/tape/tapeserver/daemon/VdqmAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/TapeMessageHandler.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/tape/tapeserver/daemon/LabelSession.hpp"
#include "castor/utils/utils.hpp"
#include "castor/common/CastorConfiguration.hpp"
#include "h/Ctape.h"
#include "h/rtcp_constants.h"
#include "h/rtcpd_constants.h"

#include <algorithm>
#include <limits.h>
#include <memory>
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeDaemon::TapeDaemon(
  const int argc,
  char **const argv,
  std::ostream &stdOut,
  std::ostream &stdErr,
  log::Logger &log,
  const utils::DriveConfigMap &driveConfigs,
  legacymsg::VdqmProxy &vdqm,
  legacymsg::VmgrProxy &vmgr,
  legacymsg::RmcProxyFactory &rmcFactory,
  messages::TapeserverProxyFactory &tapeserverFactory,
  legacymsg::NsProxyFactory &nsFactory,
  reactor::ZMQReactor &reactor,
  CapabilityUtils &capUtils) throw(castor::exception::Exception):
  castor::server::Daemon(stdOut, stdErr, log),
  m_argc(argc),
  m_argv(argv),
  m_driveConfigs(driveConfigs),
  m_vdqm(vdqm),
  m_vmgr(vmgr),
  m_rmcFactory(rmcFactory),
  m_tapeserverFactory(tapeserverFactory),
  m_nsFactory(nsFactory),
  m_reactor(reactor),
  m_capUtils(capUtils),
  m_programName("tapeserverd"),
  m_hostName(getHostName()) {
}

//------------------------------------------------------------------------------
// getHostName
//------------------------------------------------------------------------------
std::string
  castor::tape::tapeserver::daemon::TapeDaemon::getHostName()
  const  {
  char nameBuf[81];
  if(gethostname(nameBuf, sizeof(nameBuf))) {
    char errBuf[100];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get host name: " << errBuf;
    throw ex;
  }

  return nameBuf;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeDaemon::~TapeDaemon() throw() {
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::TapeDaemon::main() throw() {
  try {

    exceptionThrowingMain(m_argc, m_argv);

  } catch (castor::exception::Exception &ex) {
    std::ostringstream msg;
    msg << "Aborting: Caught an unexpected exception: " <<
      ex.getMessage().str();
    m_stdErr << std::endl << msg.str() << std::endl << std::endl;

    log::Param params[] = {
      log::Param("Message", msg.str()),
      log::Param("Code"   , ex.code())};
    m_log(LOG_ERR, msg.str(), params);

    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
void  castor::tape::tapeserver::daemon::TapeDaemon::exceptionThrowingMain(
  const int argc, char **const argv)  {
  logStartOfDaemon(argc, argv);
  parseCommandLine(argc, argv);
  m_driveCatalogue.populateCatalogue(m_driveConfigs);

  // Process must be able to change user now and should be permitted to perform
  // raw IO in the future
  try {
    m_capUtils.capSetProcText("cap_setgid,cap_setuid+ep cap_sys_rawio+p");
    log::Param params[] =
      {log::Param("capabilities", m_capUtils.capGetProcText())};
    m_log(LOG_INFO, "Set process capabilities before daemonizing",
      params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set process capabilities before daemonizing: "
      << ne.getMessage().str();
    throw ex;
  }

  const bool runAsStagerSuperuser = true;
  daemonizeIfNotRunInForeground(runAsStagerSuperuser);

  castor::utils::setDumpableProcessAttribute(true);
  {
    const bool dumpable = castor::utils::getDumpableProcessAttribute();
    log::Param params[] = {
      log::Param("dumpable", dumpable ? "true" : "false")};
    m_log(LOG_INFO, "Got dumpable attribute of process", params);
  }

  // There is no longer a need for the process to be able to change user,
  // however the process should still be permitted to perform raw IO in the
  // future
  try {
    m_capUtils.capSetProcText("cap_sys_rawio+p");
    log::Param params[] =
      {log::Param("capabilities", m_capUtils.capGetProcText())};
    m_log(LOG_INFO,
      "Set process capabilities after switching to the stager superuser",
      params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set process capabilities after switching to"
      " the stager superuser: " << ne.getMessage().str();
    throw ex;
  }

  blockSignals();
  setUpReactor();
  registerTapeDrivesWithVdqm();
  mainEventLoop();
}

//------------------------------------------------------------------------------
// logStartOfDaemon
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::logStartOfDaemon(
  const int argc, const char *const *const argv) throw() {
  const std::string concatenatedArgs = argvToString(argc, argv);
  std::ostringstream msg;
  msg << m_programName << " started";

  log::Param params[] = {
    log::Param("argv", concatenatedArgs)};
  m_log(LOG_INFO, msg.str(), params);
}

//------------------------------------------------------------------------------
// argvToString
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::TapeDaemon::argvToString(
  const int argc, const char *const *const argv) throw() {
  std::string str;

  for(int i=0; i < argc; i++) {
    if(i != 0) {
      str += " ";
    }

    str += argv[i];
  }
  return str;
}

//------------------------------------------------------------------------------
// blockSignals
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::blockSignals() const
   {
  sigset_t sigs;
  sigemptyset(&sigs);
  // The signals that should not asynchronously disturb the daemon
  sigaddset(&sigs, SIGHUP);
  sigaddset(&sigs, SIGINT);
  sigaddset(&sigs, SIGQUIT);
  sigaddset(&sigs, SIGPIPE);
  sigaddset(&sigs, SIGTERM);
  sigaddset(&sigs, SIGUSR1);
  sigaddset(&sigs, SIGUSR2);
  sigaddset(&sigs, SIGCHLD);
  sigaddset(&sigs, SIGTSTP);
  sigaddset(&sigs, SIGTTIN);
  sigaddset(&sigs, SIGTTOU);
  sigaddset(&sigs, SIGPOLL);
  sigaddset(&sigs, SIGURG);
  sigaddset(&sigs, SIGVTALRM);
  castor::exception::Errnum::throwOnNonZero(
    sigprocmask(SIG_BLOCK, &sigs, NULL),
    "Failed to block signals: sigprocmask() failed");
}

//------------------------------------------------------------------------------
// registerTapeDrivesWithVdqm
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::registerTapeDrivesWithVdqm()
   {
  const std::list<std::string> unitNames = m_driveCatalogue.getUnitNames();

  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor != unitNames.end(); itor++) {
    registerTapeDriveWithVdqm(*itor);
  }
}

//------------------------------------------------------------------------------
// registerTapeDriveWithVdqm
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::registerTapeDriveWithVdqm(
  const std::string &unitName)  {
  const DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(unitName);
  const utils::DriveConfig &driveConfig = drive.getConfig();

  std::list<log::Param> params;
  params.push_back(log::Param("server", m_hostName));
  params.push_back(log::Param("unitName", unitName));
  params.push_back(log::Param("dgn", driveConfig.dgn));

  switch(drive.getState()) {
  case DriveCatalogueEntry::DRIVE_STATE_DOWN:
    params.push_back(log::Param("state", "down"));
    m_log(LOG_INFO, "Registering tape drive in vdqm", params);
    m_vdqm.setDriveDown(m_hostName, unitName, driveConfig.dgn);
    break;
  case DriveCatalogueEntry::DRIVE_STATE_UP:
    params.push_back(log::Param("state", "up"));
    m_log(LOG_INFO, "Registering tape drive in vdqm", params);
    m_vdqm.setDriveUp(m_hostName, unitName, driveConfig.dgn);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to register tape drive in vdqm"
        ": server=" << m_hostName << " unitName=" << unitName << " dgn=" <<
        driveConfig.dgn << ": Invalid drive state: state=" <<
        DriveCatalogueEntry::drvState2Str(drive.getState());
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// setUpReactor
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setUpReactor()
   {
  createAndRegisterVdqmAcceptHandler();
  createAndRegisterAdminAcceptHandler();
  createAndRegisterTapeMessageHandler();
}

//------------------------------------------------------------------------------
// createAndRegisterVdqmAcceptHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::createAndRegisterVdqmAcceptHandler()  {
  castor::utils::SmartFd listenSock;
  try {
    listenSock.reset(io::createListenerSock(TAPE_SERVER_VDQM_LISTENING_PORT));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex(ne.code());
    ex.getMessage() << "Failed to create socket to listen for vdqm connections"
      ": " << ne.getMessage().str();
    throw ex;
  }
  {
    log::Param params[] = {
      log::Param("listeningPort", TAPE_SERVER_VDQM_LISTENING_PORT)};
    m_log(LOG_INFO, "Listening for connections from the vdqmd daemon", params);
  }

  std::auto_ptr<VdqmAcceptHandler> vdqmAcceptHandler;
  try {
    vdqmAcceptHandler.reset(new VdqmAcceptHandler(listenSock.get(), m_reactor,
      m_log, m_driveCatalogue));
    listenSock.release();
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() <<
      "Failed to create event handler for accepting vdqm connections"
      ": " << ba.what();
    throw ex;
  }
  m_reactor.registerHandler(vdqmAcceptHandler.get());
  vdqmAcceptHandler.release();
}

//------------------------------------------------------------------------------
// createAndRegisterAdminAcceptHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::createAndRegisterAdminAcceptHandler()  {
  castor::utils::SmartFd listenSock;
  try {
    listenSock.reset(io::createListenerSock(TAPE_SERVER_ADMIN_LISTENING_PORT));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex(ne.code());
    ex.getMessage() <<
      "Failed to create socket to listen for admin command connections"
      ": " << ne.getMessage().str();
    throw ex;
  }
  {
    log::Param params[] = {
      log::Param("listeningPort", TAPE_SERVER_ADMIN_LISTENING_PORT)};
    m_log(LOG_INFO, "Listening for connections from the admin commands",
      params);
  }

  std::auto_ptr<AdminAcceptHandler> adminAcceptHandler;
  try {
    adminAcceptHandler.reset(new AdminAcceptHandler(listenSock.get(), m_reactor,
      m_log, m_vdqm, m_driveCatalogue, m_hostName));
    listenSock.release();
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() <<
      "Failed to create event handler for accepting admin connections"
      ": " << ba.what();
    throw ex;
  }
  m_reactor.registerHandler(adminAcceptHandler.get());
  adminAcceptHandler.release();
}


//------------------------------------------------------------------------------
// createAndRegisterMountSessionAcceptHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::createAndRegisterTapeMessageHandler()  {

  std::auto_ptr<TapeMessageHandler> tapeMessageHandler;
  try {
    tapeMessageHandler.reset(new TapeMessageHandler(m_reactor, m_log,m_driveCatalogue,m_hostName,m_vdqm,m_vmgr));
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() <<
      "Failed to create event handler for communicating with forked sessions"
      ": " << ba.what();
    throw ex;
  }
  m_reactor.registerHandler(tapeMessageHandler.get());
  tapeMessageHandler.release();
}

//------------------------------------------------------------------------------
// mainEventLoop
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::mainEventLoop() {
  while(handleEvents()) {
    forkMountSessions();
    forkLabelSessions();
  }
}

//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::TapeDaemon::handleEvents()
   {
  // With our current understanding we see no reason for an exception from the
  // reactor to be used as a reason to stop the tapeserverd daemon.
  //
  // In the future, one or more specific exception types could be introduced
  // with their own catch clauses that do in fact require the tapeserverd daemon
  // to be stopped.
  try {
    const int timeout = 100; // 100 milliseconds
    m_reactor.handleEvents(timeout);
  } catch(castor::exception::Exception &ex) {
    // Log exception and continue
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "Unexpected exception thrown when handling an I/O event",
      params);
  } catch(std::exception &se) {
    // Log exception and continue
    log::Param params[] = {log::Param("message", se.what())};
    m_log(LOG_ERR, "Unexpected exception thrown when handling an I/O event",
      params);
  } catch(...) {
    // Log exception and continue
    m_log(LOG_ERR,
      "Unexpected and unknown exception thrown when handling an I/O event");
  }

  return handlePendingSignals();
}

//------------------------------------------------------------------------------
// handlePendingSignals
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::TapeDaemon::handlePendingSignals()
  throw() {
  bool continueMainEventLoop = true;
  int sig = 0;
  sigset_t allSignals;
  siginfo_t sigInfo;
  sigfillset(&allSignals);
  struct timespec immedTimeout = {0, 0};

  // While there is a pending signal to be handled
  while (0 < (sig = sigtimedwait(&allSignals, &sigInfo, &immedTimeout))) {
    switch(sig) {
    case SIGINT: // Signal number 2
      m_log(LOG_INFO, "Stopping gracefully because SIGINT was received");
      continueMainEventLoop = false;
      break;
    case SIGTERM: // Signal number 15
      m_log(LOG_INFO, "Stopping gracefully because SIGTERM was received");
      continueMainEventLoop = false;
      break;
    case SIGCHLD: // Signal number 17
      reapZombies();
      break;
    default:
      {
        log::Param params[] = {log::Param("signal", sig)};
        m_log(LOG_INFO, "Ignoring signal", params);
      }
      break;
    }
  }

  return continueMainEventLoop;
}

//------------------------------------------------------------------------------
// reapZombies
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::reapZombies() throw() {
  pid_t sessionPid = 0;
  int waitpidStat = 0;

  while (0 < (sessionPid = waitpid(-1, &waitpidStat, WNOHANG))) {
    postProcessReapedSession(sessionPid, waitpidStat);
  }
}

//------------------------------------------------------------------------------
// postProcessReapedSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::postProcessReapedSession(
  const pid_t sessionPid, const int waitpidStat) throw() {
  logSessionProcessTerminated(sessionPid, waitpidStat);

  std::list<log::Param> params;
  params.push_back(log::Param("sessionPid", sessionPid));

  DriveCatalogueEntry drive;
  try {
    drive = m_driveCatalogue.findConstDrive(sessionPid);
    const utils::DriveConfig &driveConfig = drive.getConfig();
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("sessionType",
      DriveCatalogueEntry::sessionType2Str(drive.getSessionType())));

    dispatchReapedSessionPostProcessor(drive.getSessionType(), sessionPid,
      waitpidStat);
  } catch(castor::exception::Exception &ne) {
    params.push_back(log::Param("message", ne.getMessage().str()));
    m_log(LOG_ERR, "Failed to perform post processing of reaped session",
      params);
    return;
  }
}

//------------------------------------------------------------------------------
// logSessionProcessTerminated
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::logSessionProcessTerminated(
  const pid_t sessionPid, const int waitpidStat) throw() {
  std::list<log::Param> params;
  params.push_back(log::Param("sessionPid", sessionPid));

  if(WIFEXITED(waitpidStat)) {
    params.push_back(log::Param("WEXITSTATUS", WEXITSTATUS(waitpidStat)));
  }

  if(WIFSIGNALED(waitpidStat)) {
    params.push_back(log::Param("WTERMSIG", WTERMSIG(waitpidStat)));
  }

  if(WCOREDUMP(waitpidStat)) {
    params.push_back(log::Param("WCOREDUMP", "true"));
  } else {
    params.push_back(log::Param("WCOREDUMP", "false"));
  }

  if(WIFSTOPPED(waitpidStat)) {
    params.push_back(log::Param("WSTOPSIG", WSTOPSIG(waitpidStat)));
  }

  if(WIFCONTINUED(waitpidStat)) {
    params.push_back(log::Param("WIFCONTINUED", "true"));
  } else {
    params.push_back(log::Param("WIFCONTINUED", "false"));
  }

  m_log(LOG_INFO, "Session child-process terminated", params);
}

//------------------------------------------------------------------------------
// dispatchReapedSessionPostProcessor
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  dispatchReapedSessionPostProcessor(
  const DriveCatalogueEntry::SessionType sessionType,
  const pid_t sessionPid,
  const int waitpidStat) {

  switch(sessionType) {
  case DriveCatalogueEntry::SESSION_TYPE_DATATRANSFER:
    return postProcessReapedDataTransferSession(sessionPid, waitpidStat);
  case DriveCatalogueEntry::SESSION_TYPE_LABEL:
    return postProcessReapedLabelSession(sessionPid, waitpidStat);
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to dispatch reaped-session post processor"
        ": Unexpected session type: sessionType=" << sessionType;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// postProcessReapedDataTransferSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::postProcessReapedDataTransferSession(
  const pid_t sessionPid, const int waitpidStat) {
  try {
    std::list<log::Param> params;
    params.push_back(log::Param("sessionPid", sessionPid));
    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(sessionPid);
    const utils::DriveConfig &driveConfig = drive.getConfig();

    if(WIFEXITED(waitpidStat) && 0 == WEXITSTATUS(waitpidStat)) {
      const std::string vid = drive.getVid();
      drive.sessionSucceeded();
      m_log(LOG_INFO, "Mount session succeeded", params);
      requestVdqmToReleaseDrive(driveConfig, sessionPid);
      notifyVdqmTapeUnmounted(driveConfig, vid, sessionPid);
    } else {
      drive.sessionFailed();
      m_log(LOG_INFO, "Mount session failed", params);
      setDriveDownInVdqm(sessionPid, drive.getConfig());
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to post process reaped data transfer session: " << 
    ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// requestVdqmToReleaseDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::requestVdqmToReleaseDrive(
  const utils::DriveConfig &driveConfig, const pid_t sessionPid) {
  std::list<log::Param> params;
  try {
    const bool forceUnmount = true;

    params.push_back(log::Param("sessionPid", sessionPid));
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("dgn", driveConfig.dgn));
    params.push_back(log::Param("forceUnmount", forceUnmount));

    m_vdqm.releaseDrive(m_hostName, driveConfig.unitName, driveConfig.dgn,
      forceUnmount, sessionPid);
    m_log(LOG_INFO, "Requested vdqm to release drive", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to request vdqm to release drive: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// notifyVdqmTapeUnmounted
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::notifyVdqmTapeUnmounted(
  const utils::DriveConfig &driveConfig, const std::string &vid,
  const pid_t sessionPid) {
  try {
    std::list<log::Param> params;
    params.push_back(log::Param("sessionPid", sessionPid));
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("vid", vid));
    params.push_back(log::Param("dgn", driveConfig.dgn));

    m_vdqm.tapeUnmounted(m_hostName, driveConfig.unitName, driveConfig.dgn,
      vid);
    m_log(LOG_INFO, "Notified vdqm that a tape was unmounted", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed notify vdqm that a tape was unmounted: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDriveDownInVdqm
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setDriveDownInVdqm(
  const pid_t sessionPid, const utils::DriveConfig &driveConfig) {
  std::list<log::Param> params;
  params.push_back(log::Param("sessionPid", sessionPid));

  try {
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("dgn", driveConfig.dgn));

    m_vdqm.setDriveDown(m_hostName, driveConfig.unitName, driveConfig.dgn);
    m_log(LOG_INFO, "Set tape-drive down in vdqm", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set tape-drive down in vdqm: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// postProcessReapedLabelSession 
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::postProcessReapedLabelSession(
  const pid_t sessionPid, const int waitpidStat) {
  try {
    std::list<log::Param> params;
    params.push_back(log::Param("sessionPid", sessionPid));

    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(sessionPid);

    if(WIFEXITED(waitpidStat) && 0 == WEXITSTATUS(waitpidStat)) {
      drive.sessionSucceeded();
      m_log(LOG_INFO, "Label session succeeded", params);
    } else {
      drive.sessionFailed();
      m_log(LOG_INFO, "Label session failed", params);
      setDriveDownInVdqm(sessionPid, drive.getConfig());
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to post process reaped label session: " <<
    ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// forkMountSessions
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkMountSessions() throw() {
  const std::list<std::string> unitNames = m_driveCatalogue.getUnitNames(
    DriveCatalogueEntry::DRIVE_STATE_WAITFORKTRANSFER);

  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor != unitNames.end(); itor++) {
    const std::string unitName = *itor;
    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(unitName);
    forkMountSession(drive);
  }
}

//------------------------------------------------------------------------------
// forkMountSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkMountSession(
  DriveCatalogueEntry &drive) throw() {
  const utils::DriveConfig &driveConfig = drive.getConfig();

  std::list<log::Param> params;
  params.push_back(log::Param("unitName", driveConfig.unitName));

  m_log.prepareForFork();

  pid_t forkRc = fork();

  // If fork failed
  if(0 > forkRc) {
    // Log an error message and return
    char errBuf[100];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    params.push_back(log::Param("message", errBuf));
    m_log(LOG_ERR, "Failed to fork mount session for tape drive", params);
    return;

  // Else if this is the parent process
  } else if(0 < forkRc) {
    drive.forkedMountSession(forkRc);
    return;

  // Else this is the child process
  } else {
    params.push_back(log::Param("sessionPid", getpid()));

    // Clear the reactor which in turn will close all of the open
    // file-descriptors owned by the event handlers
    m_reactor.clear();

    try {
      m_vdqm.assignDrive(m_hostName, driveConfig.unitName,
        drive.getVdqmJob().dgn, drive.getVdqmJob().volReqId, getpid());
      m_log(LOG_INFO, "Assigned the drive in the vdqm", params);
    } catch(castor::exception::Exception &ex) {
      params.push_back(log::Param("message", ex.getMessage().str()));
      m_log(LOG_ERR,
        "Mount session could not be started: Failed to assign drive in vdqm",
        params);
    }

    runMountSession(drive);
  }
}

//------------------------------------------------------------------------------
// runMountSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::runMountSession(
  const DriveCatalogueEntry &drive) throw() {
  const utils::DriveConfig &driveConfig = drive.getConfig();
  const pid_t sessionPid = getpid();

  std::list<log::Param> params;
  params.push_back(log::Param("sessionPid", sessionPid));
  params.push_back(log::Param("unitName", driveConfig.unitName));

  m_log(LOG_INFO, "Mount-session child-process started", params);
  
  try {
    MountSession::CastorConf castorConf;
    // This try bloc will allow us to send a failure notification to the client
    // if we fail before the MountSession has an opportunity to do so.
    std::auto_ptr<MountSession> mountSession;
    castor::tape::System::realWrapper sysWrapper;
    std::auto_ptr<legacymsg::RmcProxy> rmc;
    std::auto_ptr<messages::TapeserverProxy> tapeserver;
    try {
      common::CastorConfiguration &config =
        common::CastorConfiguration::getConfig();
      castorConf.rtcopydBufsz = config.getConfEntInt(
        "RTCOPYD", "BUFSZ", (uint32_t)RTCP_BUFSZ, &m_log);
      castorConf.rtcopydNbBufs = config.getConfEntInt(
        "RTCOPYD", "NB_BUFS", (uint32_t)NB_RTCP_BUFS, &m_log);
      castorConf.tapeBadMIRHandlingRepair = config.getConfEntString(
        "TAPE", "BADMIR_HANDLING", "CANCEL", &m_log);
      castorConf.tapebridgeBulkRequestMigrationMaxBytes = config.getConfEntInt(
        "TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXBYTES",
        (uint64_t)tapebridge::TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES, &m_log);
      castorConf.tapebridgeBulkRequestMigrationMaxFiles = config.getConfEntInt(
        "TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXFILES",
        (uint64_t)tapebridge::TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES, &m_log);
      castorConf.tapebridgeBulkRequestRecallMaxBytes = config.getConfEntInt(
        "TAPEBRIDGE", "BULKREQUESTRECALLMAXBYTES",
        (uint64_t)tapebridge::TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES, &m_log);
      castorConf.tapebridgeBulkRequestRecallMaxFiles = config.getConfEntInt(
        "TAPEBRIDGE", "BULKREQUESTRECALLMAXFILES",
        (uint64_t)tapebridge::TAPEBRIDGE_BULKREQUESTRECALLMAXFILES, &m_log);
      castorConf.tapebridgeMaxBytesBeforeFlush = config.getConfEntInt(
        "TAPEBRIDGE", "MAXBYTESBEFOREFLUSH",
        (uint64_t)tapebridge::TAPEBRIDGE_MAXBYTESBEFOREFLUSH, &m_log);
      castorConf.tapebridgeMaxFilesBeforeFlush = config.getConfEntInt(
        "TAPEBRIDGE", "MAXFILESBEFOREFLUSH",
        (uint64_t)tapebridge::TAPEBRIDGE_MAXFILESBEFOREFLUSH, &m_log);
      castorConf.tapeserverdDiskThreads = config.getConfEntInt(
        "RTCPD", "THREAD_POOL", (uint32_t)RTCPD_THREAD_POOL, &m_log);
      
      rmc.reset(m_rmcFactory.create());
      try{
        tapeserver.reset(m_tapeserverFactory.create(MountSession::ctx()));
      }
      catch(const std::exception& e){
        m_log(LOG_ERR, "Failed to connect ZMQ/REQ socket in MountSession");
      }
      mountSession.reset(new MountSession (
        m_argc,
        m_argv,
        m_hostName,
        drive.getVdqmJob(),
        m_log,
        sysWrapper,
        driveConfig,
        *(rmc.get()),
        *(tapeserver.get()),
        m_capUtils,
        castorConf
      ));
    } catch (castor::exception::Exception & ex) {
      try {
        client::ClientProxy cl(drive.getVdqmJob());
        client::ClientInterface::RequestReport rep;
        cl.reportEndOfSessionWithError(ex.getMessageValue(), ex.code(), rep);
      } catch (...) {
        params.push_back(log::Param("errorMessage", ex.getMessageValue()));
        params.push_back(log::Param("errorCode", ex.code()));
        m_log(LOG_ERR, "Failed to notify the client of the failed session"
          " when setting up the mount session", params);
      }
      throw;
    } 
    catch (...) {
      try {
        m_log(LOG_ERR, "got non castor exception error while construction mount session", params);
        client::ClientProxy cl(drive.getVdqmJob());
        client::ClientInterface::RequestReport rep;
        cl.reportEndOfSessionWithError(
         "Non-Castor exception when setting up the mount session", SEINTERNAL,
         rep);
      } catch (...) {
        params.push_back(log::Param("errorMessage",
          "Non-Castor exception when setting up the mount session"));
        m_log(LOG_ERR, "Failed to notify the client of the failed session"
          " when setting up the mount session", params);
      }
      throw;
    }
    m_log(LOG_INFO, "Going to execute Mount Session");
    int result = mountSession->execute();
//    MountSession::ctx().close();
    exit(result);
  } catch(castor::exception::Exception & ex) {
    params.push_back(log::Param("message", ex.getMessageValue()));
    m_log(LOG_ERR,
      "Aborting mount session: Caught an unexpected CASTOR exception", params);
    castor::log::LogContext lc(m_log);
    lc.logBacktrace(LOG_ERR, ex.backtrace());
    exit(1);
  } catch(std::exception &se) {
    params.push_back(log::Param("message", se.what()));
    m_log(LOG_ERR,
      "Aborting mount session: Caught an unexpected standard exception",
      params);
    exit(1);
  } catch(...) {
    m_log(LOG_ERR,
      "Aborting mount session: Caught an unexpected and unknown exception",
      params);
    exit(1);
  }
}

//------------------------------------------------------------------------------
// forkLabelSessions
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkLabelSessions() throw() {
  const std::list<std::string> unitNames = m_driveCatalogue.getUnitNames(
    DriveCatalogueEntry::DRIVE_STATE_WAITFORKLABEL);

  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor != unitNames.end(); itor++) {
    const std::string &unitName = *itor;
    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(unitName);
    forkLabelSession(drive);
  }
}

//------------------------------------------------------------------------------
// forkLabelSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkLabelSession(
  DriveCatalogueEntry &drive) throw() {
  const utils::DriveConfig &driveConfig = drive.getConfig();
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", driveConfig.unitName));

  // Release the connection with the label command from the drive catalogue
  castor::utils::SmartFd labelCmdConnection;
  try {
    labelCmdConnection.reset(drive.releaseLabelCmdConnection());
  } catch(castor::exception::Exception &ne) {
    params.push_back(log::Param("message", ne.getMessage().str()));
    m_log(LOG_ERR, "Failed to fork label session", params);
    return;
  }

  m_log.prepareForFork();
  const pid_t sessionPid = fork();

  // If fork failed
  if(0 > sessionPid) {
    // Log an error message and return
    char errBuf[100];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    params.push_back(log::Param("message", errBuf));
    m_log(LOG_ERR, "Failed to fork label session", params);

  // Else if this is the parent process
  } else if(0 < sessionPid) {
    // Only the child process should have the connection with the label-command
    // open
    close(labelCmdConnection.release());

    drive.forkedLabelSession(sessionPid);

  // Else this is the child process
  } else {
    // Clear the reactor which in turn will close all of the open
    // file-descriptors owned by the event handlers
    m_reactor.clear();

    runLabelSession(drive, labelCmdConnection.release());
  }
}

//------------------------------------------------------------------------------
// runLabelSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::runLabelSession(
  const DriveCatalogueEntry &drive, const int labelCmdConnection) throw() {
  const utils::DriveConfig &driveConfig = drive.getConfig();
  const pid_t sessionPid = getpid();
  std::list<log::Param> params;
  params.push_back(log::Param("sessionPid", sessionPid));
  params.push_back(log::Param("unitName", driveConfig.unitName));

  m_log(LOG_INFO, "Label-session child-process started", params);

  try {
    std::auto_ptr<legacymsg::RmcProxy> rmc(m_rmcFactory.create());
    std::auto_ptr<legacymsg::NsProxy> ns(m_nsFactory.create());
    castor::tape::System::realWrapper sWrapper;
    bool force = drive.getLabelJob().force==1 ? true : false;
    castor::tape::tapeserver::daemon::LabelSession labelsession(
      labelCmdConnection,
      *(rmc.get()),
      *(ns.get()),
      drive.getLabelJob(),
      m_log,
      sWrapper,
      driveConfig,
      force);
    labelsession.execute();
    exit(0);
  } catch(std::exception &se) {
    params.push_back(log::Param("message", se.what()));
    m_log(LOG_ERR, "Aborting label session: Caught an unexpected exception",
      params);
    exit(1);
  } catch(...) {
    m_log(LOG_ERR,
      "Aborting label session: Caught an unexpected and unknown exception",
      params);
    exit(1);
  }
}
