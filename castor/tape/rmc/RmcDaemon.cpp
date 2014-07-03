/******************************************************************************
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
#include "castor/tape/rmc/AcceptHandler.hpp"
#include "castor/tape/rmc/RmcDaemon.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/rmc_constants.h"

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
castor::tape::rmc::RmcDaemon::RmcDaemon::RmcDaemon(
  std::ostream &stdOut,
  std::ostream &stdErr,
  log::Logger &log,
  reactor::ZMQReactor &reactor,
  legacymsg::CupvProxy &cupv) :
  castor::server::Daemon(stdOut, stdErr, log),
  m_reactor(reactor),
  m_cupv(cupv),
  m_programName("rmcd"),
  m_hostName(getHostName()),
  m_rmcPort(getRmcPort()) {
}

//------------------------------------------------------------------------------
// getHostName
//------------------------------------------------------------------------------
std::string castor::tape::rmc::RmcDaemon::RmcDaemon::getHostName()
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
// getRmcPort
//------------------------------------------------------------------------------
unsigned short castor::tape::rmc::RmcDaemon::getRmcPort()
   {
  std::string configParamValue;

  // If RMC PORT is not in /etc/castor.conf then use the compile time default
  try {
    const std::string category = "RMC";
    const std::string name = "PORT";
    configParamValue = getConfigParam(category, name);
  } catch(castor::exception::Exception &ex) {
    return RMC_PORT;
  }

  // If RMC PORT is in /etc/castor.conf then it must be a valid unsigned integer
  if(!castor::utils::isValidUInt(configParamValue.c_str())) {
    castor::exception::Exception ex;
    ex.getMessage() << "RMC PORT is not a valid unsigned integer";
    throw ex;
  }

  return atoi(configParamValue.c_str());
}

//------------------------------------------------------------------------------
// getConfigParam
//------------------------------------------------------------------------------
std::string castor::tape::rmc::RmcDaemon::getConfigParam(
  const std::string &category,
  const std::string &name)
   {
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
    value = config.getConfEntString(category.c_str(), name.c_str());
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to get castor configuration entry: " << ne.getMessage().str();
    throw ex;
  }

  return value;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::rmc::RmcDaemon::~RmcDaemon() throw() {
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::rmc::RmcDaemon::main(const int argc, char **const argv) throw() {
  try {

    exceptionThrowingMain(argc, argv);

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
void  castor::tape::rmc::RmcDaemon::exceptionThrowingMain(
  const int argc, char **const argv)  {
  logStartOfDaemon(argc, argv);
  parseCommandLine(argc, argv);
  const bool runAsStagerSuperuser = false;
  daemonizeIfNotRunInForeground(runAsStagerSuperuser);
  blockSignals();
  setUpReactor();
  mainEventLoop();
}

//------------------------------------------------------------------------------
// logStartOfDaemon
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::logStartOfDaemon(
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
std::string castor::tape::rmc::RmcDaemon::argvToString(
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
void castor::tape::rmc::RmcDaemon::blockSignals() const
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
// setUpReactor
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::setUpReactor()
   {
  createAndRegisterAcceptHandler();
}

//------------------------------------------------------------------------------
// createAndRegisterAcceptHandler
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::createAndRegisterAcceptHandler()  {
  castor::utils::SmartFd listenSock;
  try {
    listenSock.reset(io::createListenerSock(m_rmcPort));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex(ne.code());
    ex.getMessage() << "Failed to create socket to listen for client connections"
      ": " << ne.getMessage().str();
    throw ex;
  }
  {
    log::Param params[] = {
      log::Param("listeningPort", m_rmcPort)};
    m_log(LOG_INFO, "Listening for client connections", params);
  }

  std::auto_ptr<AcceptHandler> acceptHandler;
  try {
    acceptHandler.reset(new AcceptHandler(listenSock.get(), m_reactor, m_log));
    listenSock.release();
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() <<
      "Failed to create the event handler for accepting client connections"
      ": " << ba.what();
    throw ex;
  }
  m_reactor.registerHandler(acceptHandler.get());
  acceptHandler.release();
}

//------------------------------------------------------------------------------
// mainEventLoop
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::mainEventLoop()
   {
  while(handleEvents()) {
    forkChildProcesses();
  }
}

//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------
bool castor::tape::rmc::RmcDaemon::handleEvents()
   {
  const int timeout = 100; // 100 milliseconds
  m_reactor.handleEvents(timeout);
  return handlePendingSignals();
}

//------------------------------------------------------------------------------
// handlePendingSignals
//------------------------------------------------------------------------------
bool castor::tape::rmc::RmcDaemon::handlePendingSignals()
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
void castor::tape::rmc::RmcDaemon::reapZombies() throw() {
  pid_t childPid = 0;
  int waitpidStat = 0;

  while (0 < (childPid = waitpid(-1, &waitpidStat, WNOHANG))) {
    reapZombie(childPid, waitpidStat);
  }
}

//------------------------------------------------------------------------------
// reapZombie
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::reapZombie(const pid_t childPid, const int waitpidStat) throw() {
  logChildProcessTerminated(childPid, waitpidStat);
}

//------------------------------------------------------------------------------
// logChildProcessTerminated
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::logChildProcessTerminated(const pid_t childPid, const int waitpidStat) throw() {
  std::list<log::Param> params;
  params.push_back(log::Param("childPid", childPid));

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

  m_log(LOG_INFO, "Child-process terminated", params);
}

//------------------------------------------------------------------------------
// forkChildProcesses
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::forkChildProcesses() throw() {
/*
  const std::list<std::string> unitNames =
    m_driveCatalogue.getUnitNames(DriveCatalogue::DRIVE_STATE_WAITFORK);

  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor != unitNames.end(); itor++) {
    forkChildProcess(*itor);
  }
*/
}

//------------------------------------------------------------------------------
// forkChildProcess
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::forkChildProcess() throw() {
/*
  m_log.prepareForFork();
  const pid_t childPid = fork();

  // If fork failed
  if(0 > childPid) {
    // Log an error message and return
    char errBuf[100];
    sstrerror_r(errno, errBuf, sizeof(errBuf));
    log::Param params[] = {log::Param("message", errBuf)};
    m_log(LOG_ERR, "Failed to fork mount session for tape drive", params);

  // Else if this is the parent process
  } else if(0 < childPid) {
    m_driveCatalogue.forkedDataTransferSession(unitName, childPid);

  // Else this is the child process
  } else {
    // Clear the reactor which in turn will close all of the open
    // file-descriptors owned by the event handlers
    m_reactor.clear();

    runDataTransferSession(unitName);

    // The runDataTransferSession() should call exit() and should therefore never
    // return
    m_log(LOG_ERR, "runDataTransferSession() returned unexpectedly");
  }
*/
}
