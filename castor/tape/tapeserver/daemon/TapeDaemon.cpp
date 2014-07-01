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
 
#include "castor/common/CastorConfiguration.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/exception/BadAlloc.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapeserver/daemon/AdminAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/LabelCmdAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/LabelSession.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"
#include "castor/tape/tapeserver/daemon/TapeMessageHandler.hpp"
#include "castor/tape/tapeserver/daemon/VdqmAcceptHandler.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape.h"
#include "h/rtcp_constants.h"
#include "h/rtcpd_constants.h"

#include <algorithm>
#include <errno.h>
#include <limits.h>
#include <memory>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
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
  castor::server::ProcessCap &capUtils):
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
  m_hostName(getHostName()),
  m_processForkerCmdSenderSocket(-1),
  m_processForkerPid(0),
  m_zmqContext(NULL) {
}

//------------------------------------------------------------------------------
// getHostName
//------------------------------------------------------------------------------
std::string
  castor::tape::tapeserver::daemon::TapeDaemon::getHostName()
  const  {
  char nameBuf[81];
  if(gethostname(nameBuf, sizeof(nameBuf))) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get host name: " << message;
    throw ex;
  }

  return nameBuf;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeDaemon::~TapeDaemon() throw() {
  if(-1 != m_processForkerCmdSenderSocket) {
    std::list<log::Param> params;
    params.push_back(
      log::Param("cmdSenderSocket", m_processForkerCmdSenderSocket));
    if(close(m_processForkerCmdSenderSocket)) {
      char message[100];
      strerror_r(errno, message, sizeof(message));
      params.push_back(log::Param("message", message));
      m_log(LOG_ERR, "Failed to close the socket used for sending copmmands to"
        " the ProcessForker", params);
    } else {
      m_log(LOG_INFO, "Succesffuly closed the socket used for sending commands"
        " to the ProcessForker", params);
    }
  }

  if(NULL != m_zmqContext) {
    if(zmq_term(m_zmqContext)) {
      char message[100];
      sstrerror_r(errno, message, sizeof(message));
      castor::log::Param params[] = {castor::log::Param("message", message)};
      m_log(LOG_ERR, "Failed to destroy ZMQ context", params);
    }
  }
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
  setProcessCapabilities("cap_setgid,cap_setuid+ep cap_sys_rawio+p");

  const bool runAsStagerSuperuser = true;
  daemonizeIfNotRunInForeground(runAsStagerSuperuser);
  setDumpable();
  forkProcessForker();

  // There is no longer any need for the process to be able to change user,
  // however the process should still be permitted to perform raw IO in the
  // future
  setProcessCapabilities("cap_sys_rawio+p");

  blockSignals();
  initZmqContext();
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
// setDumpable
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setDumpable() {
  castor::utils::setDumpableProcessAttribute(true);
  const bool dumpable = castor::utils::getDumpableProcessAttribute();
  log::Param params[] = {
    log::Param("dumpable", dumpable ? "true" : "false")};
  m_log(LOG_INFO, "Got dumpable attribute of process", params);
  if(!dumpable) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set dumpable attribute of process to true";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setProcessCapabilities(
  const std::string &text) {
  try {
    m_capUtils.setProcText(text);
    log::Param params[] =
      {log::Param("capabilities", m_capUtils.getProcText())};
    m_log(LOG_INFO, "Set process capabilities", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set process capabilities to '" << text <<
      "': " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// forkProcessForker
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkProcessForker() {
  m_log.prepareForFork();
  // Create a socket pair for controlling the ProcessForker
  int sv[2] = {-1 , -1};
  if(socketpair(AF_LOCAL, SOCK_STREAM, 0, sv)) {
    char message[100];
    strerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to fork process forker: Failed to create socket"
      " pair for controlling the ProcessForker: " << message;
    throw ex;
  }

  // TapeDaemon should store the socket responsible for sending commands to the
  // ProcessForker
  //
  // The destructor of the TapeDaemon will close the socket responsible for
  // sending commands to the ProcessForker
  m_processForkerCmdSenderSocket = sv[0];

  const int processForkerCmdReceiverSocket = sv[1];

  {
    log::Param params[] = {
      log::Param("cmdSenderSocket", m_processForkerCmdSenderSocket),
      log::Param("cmdReceiverSocket", processForkerCmdReceiverSocket)};
    m_log(LOG_INFO, "TapeDaemon parent process succesfully created socket"
      " pair for controlling the ProcessForker", params);
  }

  const pid_t forkRc = fork();

  // If fork failed
  if(0 > forkRc) {
    close(processForkerCmdReceiverSocket);

    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to fork ProcessForker: " << message;
    throw ex;

  // Else if this is the parent process
  } else if(0 < forkRc) {
    m_processForkerPid = forkRc;

    {
      log::Param params[] = {
        log::Param("processForkerPid", m_processForkerPid)};
      m_log(LOG_INFO, "Successfully forked the ProcessForker", params);
    }

    if(close(processForkerCmdReceiverSocket)) {
      char message[100];
      sstrerror_r(errno, message, sizeof(message));
      castor::exception::Exception ex;
      ex.getMessage() << "TapeDaemon parent process failed to close the socket"
        " used to receive ProcessForker commands: " << message;
      throw ex;
    }

    {
      log::Param params[] =
        {log::Param("cmdReceiverSocket", processForkerCmdReceiverSocket)};
      m_log(LOG_INFO, "TapeDaemon parent process successfully closed the socket"
        " used to receive ProcessForker commands", params);
    }

    return;

  // Else this is the child process
  } else {
    // Clear the reactor which in turn will close all of the open
    // file-descriptors owned by the event handlers
    m_reactor.clear();

    if(close(m_processForkerCmdSenderSocket)) {
      char message[100];
      sstrerror_r(errno, message, sizeof(message));
      castor::exception::Exception ex;
      ex.getMessage() << "ProcessForker process failed to close the socket"
        " used to send ProcessForker commands: " << message;
      throw ex;
    }

    {
      m_processForkerCmdSenderSocket = -1;
      log::Param params[] = 
        {log::Param("cmdSenderSocket", m_processForkerCmdSenderSocket)};
      m_log(LOG_INFO, "ProcessForker process successfully closed the socket" 
        " used to send ProcessForker commands", params);
    }

    runProcessForker(processForkerCmdReceiverSocket);

    exit(0);
  }
}

//------------------------------------------------------------------------------
// runProcessForker
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::runProcessForker(
  const int cmdReceiverSocket) {

  if(close(cmdReceiverSocket)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "ProcessForker process failed to close the socket"
      " used to receive ProcessForker commands: " << message;
    throw ex;
  }

  {
    log::Param params[] =
      {log::Param("cmdReceiverSocket", cmdReceiverSocket)};
    m_log(LOG_INFO, "ProcessForker process successfully closed the socket"
      " used to receive ProcessForker commands", params);
  }
}

//------------------------------------------------------------------------------
// blockSignals
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::blockSignals() const {
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
  const DriveCatalogueEntry *drive = m_driveCatalogue.findDrive(unitName);
  const utils::DriveConfig &driveConfig = drive->getConfig();

  std::list<log::Param> params;
  params.push_back(log::Param("server", m_hostName));
  params.push_back(log::Param("unitName", unitName));
  params.push_back(log::Param("dgn", driveConfig.dgn));

  switch(drive->getState()) {
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
        DriveCatalogueEntry::drvState2Str(drive->getState());
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// initZmqContext
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::initZmqContext() {
  const int sizeOfIOThreadPoolForZMQ = 1;
  m_zmqContext = zmq_init(sizeOfIOThreadPoolForZMQ);
  if(NULL == m_zmqContext) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate ZMQ context: " << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setUpReactor
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setUpReactor() {
  createAndRegisterVdqmAcceptHandler();
  createAndRegisterAdminAcceptHandler();
  createAndRegisterLabelCmdAcceptHandler();
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
// createAndRegisterLabelCmdAcceptHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::createAndRegisterLabelCmdAcceptHandler()  {
  castor::utils::SmartFd listenSock;
  try {
    listenSock.reset(io::createListenerSock(TAPE_SERVER_LABELCMD_LISTENING_PORT));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex(ne.code());
    ex.getMessage() <<
      "Failed to create socket to listen for admin command connections"
      ": " << ne.getMessage().str();
    throw ex;
  }
  {
    log::Param params[] = {
      log::Param("listeningPort", TAPE_SERVER_LABELCMD_LISTENING_PORT)};
    m_log(LOG_INFO, "Listening for connections from label command",
      params);
  }

  std::auto_ptr<LabelCmdAcceptHandler> labelCmdAcceptHandler;
  try {
    labelCmdAcceptHandler.reset(new LabelCmdAcceptHandler(listenSock.get(), m_reactor,
      m_log, m_driveCatalogue, m_hostName, m_vdqm, m_vmgr));
    listenSock.release();
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() <<
      "Failed to create event handler for accepting label-command connections"
      ": " << ba.what();
    throw ex;
  }
  m_reactor.registerHandler(labelCmdAcceptHandler.get());
  labelCmdAcceptHandler.release();
}

//------------------------------------------------------------------------------
// createAndRegisterTapeMessageHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  createAndRegisterTapeMessageHandler()  {
  std::auto_ptr<TapeMessageHandler> tapeMessageHandler;
  try {
    tapeMessageHandler.reset(new TapeMessageHandler(m_reactor, m_log,
      m_driveCatalogue, m_hostName, m_vdqm, m_vmgr, m_zmqContext));
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
    forkDataTransferSessions();
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
  pid_t pid = 0;
  int waitpidStat = 0;
  while (0 < (pid = waitpid(-1, &waitpidStat, WNOHANG))) {
    handleReapedProcess(pid, waitpidStat);
  }
}

//------------------------------------------------------------------------------
// handleReapedProcess
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::handleReapedProcess(
  const pid_t pid, const int waitpidStat) throw() {
  logChildProcessTerminated(pid, waitpidStat);

  if(pid == m_processForkerPid) {
    handleReapedProcessForker(pid, waitpidStat);
  } else {
    handleReapedSession(pid, waitpidStat);
  }
}

//------------------------------------------------------------------------------
// handleReapedProcessForker
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::handleReapedProcessForker(
  const pid_t pid, const int waitpidStat) throw() {
  log::Param params[] = {
    log::Param("processForkerPid", pid)};
  m_log(LOG_INFO, "Handling reaped ProcessForker", params);
  m_processForkerPid = 0;
}

//------------------------------------------------------------------------------
// handleReapedSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::handleReapedSession(
  const pid_t pid, const int waitpidStat) throw() {
  try {
    const DriveCatalogueEntry *const drive =
      m_driveCatalogue.findConstDrive(pid);
    dispatchReapedSessionHandler(drive->getSessionType(), pid,
      waitpidStat);
  } catch(castor::exception::Exception &ne) {
    log::Param params[] = {log::Param("message", ne.getMessage().str())};
    m_log(LOG_ERR, "Failed to handle reaped session",
      params);
  }
}

//------------------------------------------------------------------------------
// logChildProcessTerminated
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::logChildProcessTerminated(
  const pid_t pid, const int waitpidStat) throw() {
  std::list<log::Param> params;
  params.push_back(log::Param("terminatedPid", pid));

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

  m_log(LOG_INFO, "Child process terminated", params);
}

//------------------------------------------------------------------------------
// dispatchReapedSessionHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  dispatchReapedSessionHandler(
  const DriveCatalogueEntry::SessionType sessionType,
  const pid_t pid,
  const int waitpidStat) {

  switch(sessionType) {
  case DriveCatalogueEntry::SESSION_TYPE_DATATRANSFER:
    return handleReapedDataTransferSession(pid, waitpidStat);
  case DriveCatalogueEntry::SESSION_TYPE_LABEL:
    return handleReapedLabelSession(pid, waitpidStat);
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to dispatch handler for reaped session"
        ": Unexpected session type: sessionType=" << sessionType;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleReapedDataTransferSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::handleReapedDataTransferSession(
  const pid_t pid, const int waitpidStat) {
  try {
    std::list<log::Param> params;
    params.push_back(log::Param("dataTransferPid", pid));
    DriveCatalogueEntry *const drive = m_driveCatalogue.findDrive(pid);
    const utils::DriveConfig &driveConfig = drive->getConfig();

    if(WIFEXITED(waitpidStat) && 0 == WEXITSTATUS(waitpidStat)) {
      const std::string vid = drive->getVid();
      drive->sessionSucceeded();
      m_log(LOG_INFO, "Data-transfer session succeeded", params);
      requestVdqmToReleaseDrive(driveConfig, pid);
      notifyVdqmTapeUnmounted(driveConfig, vid, pid);
    } else {
      drive->sessionFailed();
      m_log(LOG_INFO, "Data-transfer session failed", params);
      setDriveDownInVdqm(pid, drive->getConfig());
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle reaped data transfer session: " << 
    ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// requestVdqmToReleaseDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::requestVdqmToReleaseDrive(
  const utils::DriveConfig &driveConfig, const pid_t pid) {
  std::list<log::Param> params;
  try {
    const bool forceUnmount = true;

    params.push_back(log::Param("pid", pid));
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("dgn", driveConfig.dgn));
    params.push_back(log::Param("forceUnmount", forceUnmount));

    m_vdqm.releaseDrive(m_hostName, driveConfig.unitName, driveConfig.dgn,
      forceUnmount, pid);
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
  const pid_t pid) {
  try {
    std::list<log::Param> params;
    params.push_back(log::Param("pid", pid));
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
  const pid_t pid, const utils::DriveConfig &driveConfig) {
  std::list<log::Param> params;
  params.push_back(log::Param("pid", pid));

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
// handleReapedLabelSession 
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::handleReapedLabelSession(
  const pid_t pid, const int waitpidStat) {
  try {
    std::list<log::Param> params;
    params.push_back(log::Param("labelPid", pid));

    DriveCatalogueEntry *const drive = m_driveCatalogue.findDrive(pid);

    if(WIFEXITED(waitpidStat) && 0 == WEXITSTATUS(waitpidStat)) {
      drive->sessionSucceeded();
      m_log(LOG_INFO, "Label session succeeded", params);
    } else {
      drive->sessionFailed();
      m_log(LOG_INFO, "Label session failed", params);
      setDriveDownInVdqm(pid, drive->getConfig());
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle reaped label session: " <<
    ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// forkDataTransferSessions
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkDataTransferSessions() throw() {
  const std::list<std::string> unitNames = m_driveCatalogue.getUnitNamesWaitingForTransferFork();

  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor != unitNames.end(); itor++) {
    const std::string unitName = *itor;
    DriveCatalogueEntry *drive = m_driveCatalogue.findDrive(unitName);
    forkDataTransferSession(drive);
  }
}

//------------------------------------------------------------------------------
// forkDataTransferSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkDataTransferSession(
  DriveCatalogueEntry *drive) throw() {
  const utils::DriveConfig &driveConfig = drive->getConfig();

  std::list<log::Param> params;
  params.push_back(log::Param("unitName", driveConfig.unitName));

  m_log.prepareForFork();

  const pid_t forkRc = fork();

  // If fork failed
  if(0 > forkRc) {
    // Log an error message and return
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    params.push_back(log::Param("message", message));
    m_log(LOG_ERR, "Failed to fork mount session for tape drive", params);
    return;

  // Else if this is the parent process
  } else if(0 < forkRc) {
    drive->forkedDataTransferSession(forkRc);
    return;

  // Else this is the child process
  } else {
    // Clear the reactor which in turn will close all of the open
    // file-descriptors owned by the event handlers
    m_reactor.clear();

    try {
      m_vdqm.assignDrive(m_hostName, driveConfig.unitName,
        drive->getVdqmJob().dgn, drive->getVdqmJob().volReqId, getpid());
      m_log(LOG_INFO, "Assigned the drive in the vdqm");
    } catch(castor::exception::Exception &ex) {
      log::Param params[] = {log::Param("message", ex.getMessage().str())};
      m_log(LOG_ERR,
        "Data-transfer session could not be started: Failed to assign drive in vdqm",
        params);
    }

    runDataTransferSession(drive);
  }
}

//------------------------------------------------------------------------------
// runDataTransferSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::runDataTransferSession(
  const DriveCatalogueEntry *drive) throw() {
  const utils::DriveConfig &driveConfig = drive->getConfig();
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", driveConfig.unitName));

  m_log(LOG_INFO, "Mount-session child-process started", params);
  
  try {
    DataTransferSession::CastorConf castorConf;
    // This try bloc will allow us to send a failure notification to the client
    // if we fail before the DataTransferSession has an opportunity to do so.
    std::auto_ptr<DataTransferSession> dataTransferSession;
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
        tapeserver.reset(
          m_tapeserverFactory.create(DataTransferSession::getZmqContext()));
      }
      catch(const std::exception& e){
        m_log(LOG_ERR, "Failed to connect ZMQ/REQ socket in DataTransferSession");
      }
      dataTransferSession.reset(new DataTransferSession (
        m_argc,
        m_argv,
        m_hostName,
        drive->getVdqmJob(),
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
        client::ClientProxy cl(drive->getVdqmJob());
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
        m_log(LOG_ERR, "got non castor exception error while constructing mount session", params);
        client::ClientProxy cl(drive->getVdqmJob());
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
    int result = dataTransferSession->execute();
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
  const std::list<std::string> unitNames = m_driveCatalogue.getUnitNamesWaitingForLabelFork();

  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor != unitNames.end(); itor++) {
    const std::string &unitName = *itor;
    DriveCatalogueEntry *drive = m_driveCatalogue.findDrive(unitName);
    forkLabelSession(drive);
  }
}

//------------------------------------------------------------------------------
// forkLabelSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::forkLabelSession(
  DriveCatalogueEntry *drive) throw() {
  const utils::DriveConfig &driveConfig = drive->getConfig();
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", driveConfig.unitName));

  // Release the connection with the label command from the drive catalogue
  castor::utils::SmartFd labelCmdConnection;
  try {
    labelCmdConnection.reset(drive->releaseLabelCmdConnection());
  } catch(castor::exception::Exception &ne) {
    params.push_back(log::Param("message", ne.getMessage().str()));
    m_log(LOG_ERR, "Failed to fork label session", params);
    return;
  }

  m_log.prepareForFork();
  const pid_t forkRc = fork();

  // If fork failed
  if(0 > forkRc) {
    // Log an error message and return
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    params.push_back(log::Param("message", message));
    m_log(LOG_ERR, "Failed to fork label session", params);

  // Else if this is the parent process
  } else if(0 < forkRc) {
    // Only the child process should have the connection with the label-command
    // open
    close(labelCmdConnection.release());

    drive->forkedLabelSession(forkRc);

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
  const DriveCatalogueEntry *drive, const int labelCmdConnection) throw() {
  const utils::DriveConfig &driveConfig = drive->getConfig();
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", driveConfig.unitName));

  m_log(LOG_INFO, "Label-session child-process started", params);

  try {
    std::auto_ptr<legacymsg::RmcProxy> rmc(m_rmcFactory.create());
    std::auto_ptr<legacymsg::NsProxy> ns(m_nsFactory.create());
    castor::tape::System::realWrapper sWrapper;
    bool force = drive->getLabelJob().force==1 ? true : false;
    castor::tape::tapeserver::daemon::LabelSession labelsession(
      labelCmdConnection,
      *(rmc.get()),
      *(ns.get()),
      drive->getLabelJob(),
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
