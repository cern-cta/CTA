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
#include "castor/tape/tapeserver/daemon/AdminAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/MountSessionAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/MountSession.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"
#include "castor/tape/tapeserver/daemon/VdqmAcceptHandler.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/tape/tapeserver/daemon/LabelSession.hpp"
#include "castor/utils/utils.hpp"
#include "castor/common/CastorConfiguration.hpp"
#include "h/Ctape.h"
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
  const utils::TpconfigLines &tpconfigLines,
  legacymsg::VdqmProxyFactory &vdqmFactory,
  legacymsg::VmgrProxyFactory &vmgrFactory,
  legacymsg::RmcProxyFactory &rmcFactory,
  legacymsg::TapeserverProxyFactory &tapeserverFactory,
  legacymsg::NsProxyFactory &nsFactory,
  io::PollReactor &reactor) throw(castor::exception::Exception):
  castor::server::Daemon(stdOut, stdErr, log),
  m_argc(argc),
  m_argv(argv),
  m_tpconfigLines(tpconfigLines),
  m_vdqmFactory(vdqmFactory),
  m_vmgrFactory(vmgrFactory),
  m_rmcFactory(rmcFactory),
  m_tapeserverFactory(tapeserverFactory),
  m_nsFactory(nsFactory),
  m_reactor(reactor),
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
  m_driveCatalogue.populateCatalogue(m_tpconfigLines);
  daemonizeIfNotRunInForeground();
  blockSignals();
  setUpReactor();
  registerTapeDrivesWithVdqm();
  mainEventLoop();
}
//------------------------------------------------------------------------------
// getConfigString
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::TapeDaemon::getConfigString(
  const std::string &category, const std::string &name)  {
  using namespace castor;

  std::ostringstream task;
  task << "get " << category << ":" << name << " from castor.conf";

  common::CastorConfiguration config;
  std::string value;

  try {
    config = common::CastorConfiguration::getConfig();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to get castor configuration: " << ne.getMessage().str();
    throw ex;
  }

  try {
    value = config.getConfEnt(category.c_str(), name.c_str());
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to get castor configuration entry: " << ne.getMessage().str();
    throw ex;
  }

  return value;
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
// logTpconfigLines
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::logTpconfigLines(
  const utils::TpconfigLines &lines) throw() {
  for(utils::TpconfigLines::const_iterator itor = lines.begin();
    itor != lines.end(); itor++) {
    logTpconfigLine(*itor);
  }
}

//------------------------------------------------------------------------------
// logTpconfigLine
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::logTpconfigLine(
  const utils::TpconfigLine &line) throw() {
  log::Param params[] = {
    log::Param("unitName", line.unitName),
    log::Param("dgn", line.dgn),
    log::Param("devFilename", line.devFilename),
    log::Param("density", line.density),
    log::Param("initialState", line.initialState),
    log::Param("librarySlot", line.librarySlot),
    log::Param("devType", line.devType)};
  m_log(LOG_INFO, "TPCONFIG line", params);
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
  const DriveCatalogue::DriveState driveState =
    m_driveCatalogue.getState(unitName);
  const std::string dgn = m_driveCatalogue.getDgn(unitName);

  std::list<log::Param> params;
  params.push_back(log::Param("server", m_hostName));
  params.push_back(log::Param("unitName", unitName));
  params.push_back(log::Param("dgn", dgn));

  std::auto_ptr<legacymsg::VdqmProxy> vdqm(m_vdqmFactory.create());

  switch(driveState) {
  case DriveCatalogue::DRIVE_STATE_DOWN:
    params.push_back(log::Param("state", "down"));
    m_log(LOG_INFO, "Registering tape drive in vdqm", params);
    vdqm->setDriveDown(m_hostName, unitName, dgn);
    break;
  case DriveCatalogue::DRIVE_STATE_UP:
    params.push_back(log::Param("state", "up"));
    m_log(LOG_INFO, "Registering tape drive in vdqm", params);
    vdqm->setDriveUp(m_hostName, unitName, dgn);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to register tape drive in vdqm"
        ": server=" << m_hostName << " unitName=" << unitName << " dgn=" << dgn
        << ": Invalid drive state: state=" <<
        DriveCatalogue::drvState2Str(driveState);
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
  createAndRegisterMountSessionAcceptHandler();
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
      m_log, m_vdqmFactory, m_driveCatalogue, m_hostName));
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
void castor::tape::tapeserver::daemon::TapeDaemon::createAndRegisterMountSessionAcceptHandler()  {
  castor::utils::SmartFd mountSessionListenSock;
  try {
    mountSessionListenSock.reset(
      io::createLocalhostListenerSock(TAPE_SERVER_MOUNTSESSION_LISTENING_PORT));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex(ne.code());
    ex.getMessage() <<
      "Failed to create socket to listen for mount session connections"
      ": " << ne.getMessage().str();
    throw ex;
  }
  {
    log::Param params[] = {
      log::Param("listeningPort", TAPE_SERVER_MOUNTSESSION_LISTENING_PORT)};
    m_log(LOG_INFO,
      "Listening for connections from the mount sessions", params);
  }

  std::auto_ptr<MountSessionAcceptHandler> mountSessionAcceptHandler;
  try {
    mountSessionAcceptHandler.reset(new MountSessionAcceptHandler(
      mountSessionListenSock.get(), m_reactor, m_log, m_driveCatalogue,
      m_hostName));
    mountSessionListenSock.release();
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() <<
      "Failed to create event handler for accepting mount-session connections"
      ": " << ba.what();
    throw ex;
  }
  m_reactor.registerHandler(mountSessionAcceptHandler.get());
  mountSessionAcceptHandler.release();
}

//------------------------------------------------------------------------------
// mainEventLoop
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::mainEventLoop()
   {
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
    reapZombie(sessionPid, waitpidStat);
  }
}

//------------------------------------------------------------------------------
// reapZombie
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::reapZombie(
  const pid_t sessionPid, const int waitpidStat) throw() {
  logSessionProcessTerminated(sessionPid, waitpidStat);

  std::list<log::Param> params;
  params.push_back(log::Param("sessionPid", sessionPid));

  DriveCatalogue::SessionType sessionType;
  try {
    sessionType = m_driveCatalogue.getSessionType(sessionPid);
    params.push_back(log::Param("sessionType", DriveCatalogue::sessionType2Str(sessionType)));
  } catch(castor::exception::Exception &ex) {
    params.push_back(log::Param("message", ex.getMessage().str()));
    m_log(LOG_ERR, "Failed to get the type of the reaped session", params);
    return;
  }

  switch(sessionType) {
  case DriveCatalogue::SESSION_TYPE_DATATRANSFER:
    return postProcessReapedDataTransferSession(sessionPid, waitpidStat);
  case DriveCatalogue::SESSION_TYPE_LABEL:
    return postProcessReapedLabelSession(sessionPid, waitpidStat);
  default:
    m_log(LOG_ERR, "Failed to perform post processing of reaped process"
      ": Unexpected session type", params);
    return;
  }
}

//------------------------------------------------------------------------------
// logSessionProcessTerminated
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::logSessionProcessTerminated(const pid_t sessionPid, const int waitpidStat) throw() {
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
// postProcessReapedDataTransferSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::postProcessReapedDataTransferSession(
  const pid_t sessionPid, const int waitpidStat) throw() {
  log::Param params[] = {log::Param("sessionPid", sessionPid)};

  if(WIFEXITED(waitpidStat) && 0 == WEXITSTATUS(waitpidStat)) {
    m_driveCatalogue.sessionSucceeded(sessionPid);
    m_log(LOG_INFO, "Mount session succeeded", params);
    notifyVdqmTapeUnmounted(sessionPid);
  } else {
    m_driveCatalogue.sessionFailed(sessionPid);
    m_log(LOG_INFO, "Mount session failed", params);
    setDriveDownInVdqm(sessionPid);
  }
}

//------------------------------------------------------------------------------
// notifyVdqmTapeUnmounted
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::notifyVdqmTapeUnmounted(const pid_t sessionPid) throw() {
  std::list<log::Param> params;
  try {
    params.push_back(log::Param("sessionPid", sessionPid));

    const std::string unitName = m_driveCatalogue.getUnitName(sessionPid);
    params.push_back(log::Param("unitName", unitName));

    const std::string vid = m_driveCatalogue.getVid(unitName);
    params.push_back(log::Param("vid", unitName));

    const std::string dgn = m_driveCatalogue.getDgn(unitName);
    params.push_back(log::Param("dgn", unitName));

    std::auto_ptr<legacymsg::VdqmProxy> vdqm(m_vdqmFactory.create());
    vdqm->tapeUnmounted(m_hostName, unitName, dgn, vid);
    m_log(LOG_INFO, "Notified vdqm that a tape was unmounted", params);
  } catch(castor::exception::Exception &ex) {
    params.push_back(log::Param("message", ex.getMessage().str()));
    m_log(LOG_ERR, "Failed to notify vdqm that a tape was unmounted", params);
    return;
  }
}

//------------------------------------------------------------------------------
// setDriveDownInVdqm
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setDriveDownInVdqm(const pid_t sessionPid) throw() {
  std::list<log::Param> params;
  try {
    params.push_back(log::Param("sessionPid", sessionPid));

    const std::string unitName = m_driveCatalogue.getUnitName(sessionPid);
    params.push_back(log::Param("unitName", unitName));

    const std::string dgn = m_driveCatalogue.getDgn(unitName);
    params.push_back(log::Param("dgn", unitName));

    std::auto_ptr<legacymsg::VdqmProxy> vdqm(m_vdqmFactory.create());
    vdqm->setDriveDown(m_hostName, unitName, dgn);
    m_log(LOG_INFO, "Set tape-drive down in vdqm", params);
  } catch(castor::exception::Exception &ex) {
    params.push_back(log::Param("message", ex.getMessage().str()));
    m_log(LOG_ERR, "Failed to set tape-drive down in vdqm", params);
    return;
  }
}

//------------------------------------------------------------------------------
// postProcessReapedLabelSession 
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::postProcessReapedLabelSession(
  const pid_t sessionPid, const int waitpidStat) throw() {
  std::list<log::Param> params;
  params.push_back(log::Param("sessionPid", sessionPid));

  if(WIFEXITED(waitpidStat) && 0 == WEXITSTATUS(waitpidStat)) {
    m_driveCatalogue.sessionSucceeded(sessionPid);
    m_log(LOG_INFO, "Label session succeeded", params);
  } else {
    m_driveCatalogue.sessionFailed(sessionPid);
    m_log(LOG_INFO, "Label session failed", params);
    setDriveDownInVdqm(sessionPid);
  }
}

//------------------------------------------------------------------------------
// forkMountSessions
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkMountSessions() throw() {
  const std::list<std::string> unitNames =
    m_driveCatalogue.getUnitNames(DriveCatalogue::DRIVE_STATE_WAITFORK);

  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor != unitNames.end(); itor++) {
    forkMountSession(*itor);
  }
}

//------------------------------------------------------------------------------
// forkMountSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkMountSession(const std::string &unitName) throw() {
  m_log.prepareForFork();
  const pid_t sessionPid = fork();

  // If fork failed
  if(0 > sessionPid) {
    // Log an error message and return
    char errBuf[100];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    log::Param params[] = {log::Param("message", errBuf)};
    m_log(LOG_ERR, "Failed to fork mount session for tape drive", params);

  // Else if this is the parent process
  } else if(0 < sessionPid) {
    m_driveCatalogue.forkedMountSession(unitName, sessionPid);

  // Else this is the child process
  } else {
    // Clear the reactor which in turn will close all of the open
    // file-descriptors owned by the event handlers
    m_reactor.clear();

    runMountSession(unitName);

    // The runMountSession() should call exit() and should therefore never
    // return
    m_log(LOG_ERR, "runMountSession() returned unexpectedly");
  }
}

//------------------------------------------------------------------------------
// runMountSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::runMountSession(const std::string &unitName) throw() {
  const pid_t sessionPid = getpid();
  {
    log::Param params[] = {
      log::Param("sessionPid", sessionPid),
      log::Param("unitName", unitName)};
    m_log(LOG_INFO, "Mount-session child-process started", params);
  }

  try {  
    MountSession::CastorConf castorConf;
    castorConf.rfioConnRetry =    getConfig<uint32_t>("RFIO", "CONRETRY");
    castorConf.rfioConnRetryInt = getConfig<uint32_t>("RFIO", "CONRETRY");
    castorConf.rfioIOBufSize =    getConfig<uint32_t>("RFIO", "IOBUFSIZE");
    castorConf.rtcopydBufsz =     getConfig<uint32_t>("RTCOPYD", "BUFSZ");
    castorConf.rtcopydNbBufs =    getConfig<uint32_t>("RTCOPYD", "NB_BUFS");
    castorConf.tapeBadMIRHandlingRepair = 
                     getConfigString("TAPE", "BADMIR_HANDLING");
    castorConf.tapeConfirmDriveFree = 
                     getConfigString("TAPE", "CONFIRM_DRIVE_FREE");
    castorConf.tapeConfirmDriveFreeInterval = 
                     getConfig<uint32_t>("TAPE", "CONFIRM_DRIVE_FREE_INTVL");
    castorConf.tapebridgeBulkRequestMigrationMaxBytes = 
                     getConfig<uint64_t>("TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXBYTES");
    castorConf.tapebridgeBulkRequestMigrationMaxFiles = 
                     getConfig<uint64_t>("TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXFILES");
    castorConf.tapebridgeBulkRequestRecallMaxBytes = 
                     getConfig<uint64_t>("TAPEBRIDGE", "BULKREQUESTRECALLMAXBYTES");
    castorConf.tapebridgeBulkRequestRecallMaxFiles = 
                     getConfig<uint64_t>("TAPEBRIDGE", "BULKREQUESTRECALLMAXFILES");
    castorConf.tapebridgeMaxBytesBeforeFlush = 
                     getConfig<uint64_t>("TAPEBRIDGE", "MAXBYTESBEFOREFLUSH");
    castorConf.tapebridgeMaxFilesBeforeFlush = 
                     getConfig<uint64_t>("TAPEBRIDGE", "MAXFILESBEFOREFLUSH");
    castorConf.tapeserverdDiskThreads = 
                     getConfig<uint32_t>("TAPEBRIDGE", "THREAD_POOL");
    castor::tape::System::realWrapper sysWrapper;
    const legacymsg::RtcpJobRqstMsgBody job = m_driveCatalogue.getVdqmJob(unitName);
    std::auto_ptr<legacymsg::VdqmProxy> vdqm(m_vdqmFactory.create());
    std::auto_ptr<legacymsg::VmgrProxy> vmgr(m_vmgrFactory.create());
    std::auto_ptr<legacymsg::RmcProxy> rmc(m_rmcFactory.create());
    std::auto_ptr<legacymsg::TapeserverProxy> tapeserver(m_tapeserverFactory.create());
    MountSession mountSession(
      m_argc,
      m_argv,
      m_hostName,
      job,
      m_log,
      sysWrapper,
      m_tpconfigLines,
      *(vdqm.get()),
      *(vmgr.get()),
      *(rmc.get()),
      *(tapeserver.get()),
      castorConf
    );

    mountSession.execute();

    exit(0);
  } catch(std::exception &se) {
    log::Param params[] = {
      log::Param("sessionPid", sessionPid),
      log::Param("unitName", unitName),
      log::Param("message", se.what())};
    m_log(LOG_ERR, "Aborting mount session: Caught an unexpected exception", params);
    exit(1);
  } catch(...) {
    log::Param params[] = {
      log::Param("sessionPid", sessionPid),
      log::Param("unitName", unitName)};
    m_log(LOG_ERR, "Aborting mount session: Caught an unexpected and unknown exception", params);
    exit(1);
  }
}

//------------------------------------------------------------------------------
// forkLabelSessions
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkLabelSessions() throw() {
  const std::list<std::string> unitNames =
    m_driveCatalogue.getUnitNames(DriveCatalogue::DRIVE_STATE_WAITLABEL);

  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor != unitNames.end(); itor++) {
    forkLabelSession(*itor);
  }
}

//------------------------------------------------------------------------------
// forkLabelSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkLabelSession(
  const std::string &unitName) throw() {
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", unitName));

  // Release the connection with the label command from the drive catalogue
  castor::utils::SmartFd labelCmdConnection;
  try {
    labelCmdConnection.reset(m_driveCatalogue.releaseLabelCmdConnection(unitName));
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

    m_driveCatalogue.forkedLabelSession(unitName, sessionPid);

  // Else this is the child process
  } else {
    // Clear the reactor which in turn will close all of the open
    // file-descriptors owned by the event handlers
    m_reactor.clear();

    runLabelSession(unitName, labelCmdConnection.release());
  }
}

//------------------------------------------------------------------------------
// runLabelSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::runLabelSession(
  const std::string &unitName, const int labelCmdConnection) throw() {
  const pid_t sessionPid = getpid();
  std::list<log::Param> params;
  params.push_back(log::Param("sessionPid", sessionPid));
  params.push_back(log::Param("unitName", unitName));

  m_log(LOG_INFO, "Label-session child-process started", params);

  try {
    const legacymsg::TapeLabelRqstMsgBody job =
      m_driveCatalogue.getLabelJob(unitName);
    std::auto_ptr<legacymsg::RmcProxy> rmc(m_rmcFactory.create());
    std::auto_ptr<legacymsg::NsProxy> ns(m_nsFactory.create());
    castor::tape::System::realWrapper sWrapper;
    bool force = job.force==1 ? true : false;
    castor::tape::tapeserver::daemon::LabelSession labelsession(
      labelCmdConnection,
      *(rmc.get()),
      *(ns.get()),
      job,
      m_log,
      sWrapper,
      m_tpconfigLines,
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
