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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
 
#include "common/Constants.hpp"
#include "AcsDaemon.hpp"
#include "AcsdConfiguration.hpp"
#include "mediachanger/acs/daemon/AcsMessageHandler.hpp"
//#include "AcsPendingRequests.hpp"
#include "mediachanger/acs/daemon/AcsdCmdLine.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/BadAlloc.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/log.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/threading/Daemon.hpp"
#include <memory>
#include <signal.h>
#include <unistd.h>

#include <getopt.h>
#include <iostream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
namespace cta { namespace mediachanger { namespace acs { namespace daemon {

AcsDaemon::AcsDaemon(
  const int argc,
  char **const argv,
  cta::log::Logger& log,  
  cta::mediachanger::reactor::ZMQReactor &reactor,
  const AcsdConfiguration &config):
  cta::server::Daemon(log),
  m_argc(argc),
  m_argv(argv),
  m_log(log),  
  m_reactor(reactor),
  m_programName("acsd"),
  m_hostName(getHostName()),  
  m_zmqContext(NULL),
  m_config(config),
  m_acsPendingRequests(config) {
}

//------------------------------------------------------------------------------
// getHostName
//------------------------------------------------------------------------------

std::string cta::mediachanger::acs::daemon::AcsDaemon::getHostName() const  {
  char nameBuf[81];
  if(gethostname(nameBuf, sizeof(nameBuf))) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to get host name: "<<ex.getMessage().str();
    m_log(LOG_ERR, ex.getMessage().str());  
    throw ex;
  }

  return nameBuf;
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
  AcsDaemon::~AcsDaemon() throw() {  
  m_reactor.clear();  
  destroyZmqContext();
  google::protobuf::ShutdownProtobufLibrary();
}

//------------------------------------------------------------------------------
// destroyZmqContext
//------------------------------------------------------------------------------

  void AcsDaemon::destroyZmqContext() throw() {
  if(NULL != m_zmqContext) {
    if(zmq_term(m_zmqContext)) {
      cta::exception::Exception ex;
      std::list<log::Param> params = {log::Param("message", ex.getMessage().str())};
      m_log(LOG_ERR, "Failed to destroy ZMQ context", params);
    } else {
      m_zmqContext = NULL;
      m_log(LOG_INFO, "Successfully destroyed ZMQ context");
    }
  }
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int AcsDaemon::main() throw() {
    
  try {

    exceptionThrowingMain(m_argc, m_argv);

  } catch (cta::exception::Exception &ex) {
    // Write the error to standard error
    std::cerr << std::endl << "Aborting: " << ex.getMessage().str() << std::endl
      << std::endl;
    // Log the error
    std::list<log::Param> params = {
    log::Param("Message", ex.getMessage().str())};
    m_log(LOG_ERR, "Aborting", params);

    return 1;
  }

  return 0;
}
//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------

void AcsDaemon::exceptionThrowingMain(
  const int argc, char **const argv)  {
  logStartOfDaemon(argc, argv);
  AcsdCmdLine(argc,argv); //parse command line
 
 const std::string runAsStagerSuperuser = "ACSD";
 const std::string runAsStagerSupergroup = "CTA";
 daemonizeIfNotRunInForegroundAndSetUserAndGroup(runAsStagerSuperuser, runAsStagerSupergroup);
 setDumpable();

 blockSignals();
 initZmqContext();
  //setUpReactor();  
 // mainEventLoop();

}

//------------------------------------------------------------------------------
// logStartOfDaemon
//------------------------------------------------------------------------------

  void AcsDaemon::logStartOfDaemon(
  const int argc, const char *const *const argv) throw() {
  const std::string concatenatedArgs = argvToString(argc, argv);
  std::ostringstream msg;
  msg << m_programName << " started";

  std::list<log::Param> params = {
    log::Param("argv", concatenatedArgs)};
  m_log(LOG_INFO, msg.str(), params);
}

//------------------------------------------------------------------------------
// argvToString
//------------------------------------------------------------------------------

  std::string AcsDaemon::argvToString(
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
void AcsDaemon::setDumpable() {
  cta::utils::setDumpableProcessAttribute(true);
  const bool dumpable = cta::utils::getDumpableProcessAttribute();
  std::list<log::Param> params = {
    log::Param("dumpable", dumpable ? "true" : "false")};
  m_log(LOG_INFO, "Got dumpable attribute of process", params);
  if(!dumpable) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to set dumpable attribute of process to true";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// blockSignals
//------------------------------------------------------------------------------
void AcsDaemon::blockSignals() const {
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
  cta::exception::Errnum::throwOnNonZero(
    sigprocmask(SIG_BLOCK, &sigs, NULL),
    "Failed to block signals: sigprocmask() failed");
}

//------------------------------------------------------------------------------
// initZmqContext
//------------------------------------------------------------------------------

  void AcsDaemon::initZmqContext() {
  const int sizeOfIOThreadPoolForZMQ = 1;
  m_zmqContext = zmq_init(sizeOfIOThreadPoolForZMQ);
  if(NULL == m_zmqContext) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate ZMQ context: " << ex.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setUpReactor
//------------------------------------------------------------------------------
/*void AcsDaemon::setUpReactor() {
  createAndRegisterAcsMessageHandler();
}
*/
//------------------------------------------------------------------------------
// createAndRegisterAcsMessageHandler
//------------------------------------------------------------------------------
/*void AcsDaemon::createAndRegisterAcsMessageHandler()  {
  try {
    std::unique_ptr<AcsMessageHandler> handler;
    try {
      handler.reset(new AcsMessageHandler(m_reactor, m_hostName, m_log, m_zmqContext,
        m_config, m_acsPendingRequests));
    } catch(std::bad_alloc &ba) {
      cta::exception::BadAlloc ex;
      ex.getMessage() <<
        "Failed to create event handler for communicating with "
        "the CASTOR ACS daemon: " << ba.what();
      throw ex;
    }
    m_reactor.registerHandler(handler.get());
    handler.release();
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create and register AcsMessageHandler: " <<
      ne.getMessage().str();
    throw ex;
  }
}
*/
//------------------------------------------------------------------------------
// mainEventLoop
//------------------------------------------------------------------------------
/*void AcsDaemon::mainEventLoop() {
  while(handleEvents()) {
  }
}*/

//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------
/*bool AcsDaemon::handleEvents() { 
  try {
    const int timeout = 100; // 100 milliseconds
    m_reactor.handleEvents(timeout);
  } catch(cta::exception::Exception &ex) {
    // Log exception and continue
    std::list<log::Param> params = {
      log::Param("message", ex.getMessage().str()),
      log::Param("backtrace", ex.backtrace())
    };
    m_log(LOG_ERR,
      "Unexpected cta exception thrown when handling an I/O event", params);
  } catch(std::exception &se) {
    // Log exception and continue
    std::list<log::Param> params = {log::Param("message", se.what())};
    m_log(LOG_ERR, "Unexpected exception thrown when handling an I/O event",
      params);
  } catch(...) {
    // Log exception and continue
    m_log(LOG_ERR,
      "Unexpected and unknown exception thrown when handling an I/O event");
  }
  
  try {
    handlePendingRequests();
  } catch(cta::exception::Exception &ex) {
    // Log exception and continue
    std::list<log::Param> params = {
      log::Param("message", ex.getMessage().str()),
      log::Param("backtrace", ex.backtrace())
    };
    m_log(LOG_ERR,
      "Unexpected cta exception thrown when handling pending requests", 
      params);
  } catch(std::exception &se) {
    // Log exception and continue
    std::list<log::Param> params = {log::Param("message", se.what())};
    m_log(LOG_ERR,
      "Unexpected exception thrown when handling pending requests", params);
  } catch(...) {
    // Log exception and continue
    m_log(LOG_ERR,
      "Unexpected and unknown exception thrown when handling pending requests");
  }
  
  return handlePendingSignals();
}
*/
//------------------------------------------------------------------------------
// handlePendingRequests
//------------------------------------------------------------------------------
/*void AcsDaemon::handlePendingRequests() {
  m_acsPendingRequests.tick(); 
  m_acsPendingRequests.handleCompletedRequests();
  m_acsPendingRequests.handleFailedRequests();
  m_acsPendingRequests.handleToDeleteRequests();
}
*/
//------------------------------------------------------------------------------
// handlePendingSignals
//------------------------------------------------------------------------------
/**
bool AcsDaemon::handlePendingSignals() throw() {
  bool continueMainEventLoop = true;
  int sig = 0;
  sigset_t allSignals;
  siginfo_t sigInfo;
  sigfillset(&allSignals);
  struct timespec immediateTimeout = {0, 0};

  // While there is a pending signal to be handled
  while (0 < (sig = sigtimedwait(&allSignals, &sigInfo, &immediateTimeout))) {
    switch(sig) {
    case SIGINT: // Signal number 2
      m_log(LOG_INFO, "Stopping gracefully because SIGINT was received");
      continueMainEventLoop = false;
      break;
    case SIGTERM: // Signal number 15
      m_log(LOG_INFO, "Stopping gracefully because SIGTERM was received");
      continueMainEventLoop = false;
      break;
    default:
      {
        std::list<log::Param> params = {log::Param("signal", sig)};
        m_log(LOG_INFO, "Ignoring signal", params);
      }
      break;
    }
  }

  return continueMainEventLoop;
}
**/

}}}}
