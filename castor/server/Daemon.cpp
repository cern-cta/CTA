/*******************************************************************************
 *                      castor/server/Daemon.hpp
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
 * @author castor dev team
 ******************************************************************************/

#include "castor/dlf/Dlf.hpp"
#include "castor/io/UDPSocket.hpp"
#include "castor/server/Daemon.hpp"
#include "castor/server/ThreadNotification.hpp"
#include "castor/System.hpp"

#include <signal.h>
#include <stdio.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::Daemon::Daemon(log::Logger &logger):
  m_foreground(false),
  m_runAsStagerSuperuser(false),
  m_logger(logger) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::Daemon::~Daemon() throw() {
}

//------------------------------------------------------------------------------
// getServerName
//------------------------------------------------------------------------------
const std::string &castor::server::Daemon::getServerName() const throw() {
  return m_logger.getProgramName();
}

//------------------------------------------------------------------------------
// runAsStagerSuperuser
//------------------------------------------------------------------------------
void castor::server::Daemon::runAsStagerSuperuser() throw() {
  m_runAsStagerSuperuser = true;
}

//-----------------------------------------------------------------------------
// dlfInit
//-----------------------------------------------------------------------------
void castor::server::Daemon::dlfInit(castor::dlf::Message messages[])
  throw (castor::exception::Exception) {
  castor::dlf::dlf_init((char*)m_logger.getProgramName().c_str(), messages);
  // Add framework specific messages
  castor::dlf::Message frameworkMessages[] =
    {{  1, "Error while reading datagrams" },
     {  2, "Error while accepting connections" },
     {  3, "Thread pool started" },
     {  4, "Exception caught in the user thread" },
     {  5, "Thread run error" },
     {  6, "NotifierThread exception" },
     {  8, "Exception caught while initializing the child process" },
     {  9, "Error while processing an object from the pipe" },
     { 10, "Uncaught exception in a thread from pool" },
     { 11, "Uncaught GENERAL exception in a thread from pool" },
     { 12, "Caught signal - GRACEFUL STOP" },
     { 14, "Caught signal - CHILD STOPPED" },
     { 15, "Signal caught but not handled - IMMEDIATE STOP" },
     { 16, "Exception during wait for signal loop" },
     { 18, "No idle thread in pool to process request" },
     { 19, "Error while dispatching to a thread" },
     { 20, "Spawning a new thread in pool" },
     { 21, "Terminating a thread in pool" },
     { 22, "Task processed" },
     { -1, "" }};
  castor::dlf::dlf_addMessages(DLF_BASE_FRAMEWORK, frameworkMessages);
}

//------------------------------------------------------------------------------
// daemonize
//------------------------------------------------------------------------------
void castor::server::Daemon::daemonize() throw (castor::exception::Exception) {
  // If the daemon is to be run in the background
  if (!m_foreground) {
    m_logger.prepareForFork();

    // We could set our working directory to '/' here with a call to chdir(2).
    // For the time being we don't and leave it to the initd script to change
    // to a suitable directory for us!
    int pid = fork();
    if (pid < 0) {
      castor::exception::Internal ex;
      ex.getMessage() << "Background daemon initialization failed with result "
                      << pid << std::endl;
      throw ex;
    }
    else if (pid > 0) {
      // The parent exits normally
      exit(EXIT_SUCCESS);
    }

    // Run the daemon in a new session
    setsid();

    // Redirect the standard file descriptors to /dev/null
    if ((freopen("/dev/null", "r", stdin)  == NULL) ||
        (freopen("/dev/null", "w", stdout) == NULL) ||
        (freopen("/dev/null", "w", stderr) == NULL)) {
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to redirect standard file descriptors to "
                      << "/dev/null" << std::endl;
      throw ex;
    }
  }

  // Change the user of the daemon process to the Castor superuser if requested
  if (m_runAsStagerSuperuser) {
    castor::System::switchToCastorSuperuser();
  }

  // Ignore SIGPIPE (connection lost with client)
  // and SIGXFSZ (a file is too big)
  signal(SIGPIPE, SIG_IGN);
  signal(SIGXFSZ, SIG_IGN);
}


//------------------------------------------------------------------------------
// sendNotification
//------------------------------------------------------------------------------
void castor::server::Daemon::sendNotification(const std::string &host,
  const int port, const char tpName, const int nbThreads) throw() {
  try {
    // Create notification message
    castor::server::ThreadNotification notif;
    notif.setTpName(tpName);
    notif.setNbThreads(nbThreads);

    // Create UDP socket and send packet
    castor::io::UDPSocket sock(port, host);
    sock.sendObject(notif);
    sock.close();
  } catch (castor::exception::Exception& ignored) {
    // This is a best effort service, ignore any failure
  } catch(...) {
    // This is a best effort service, ignore any failure
  }
}


//-----------------------------------------------------------------------------
// logMsg
//-----------------------------------------------------------------------------
void castor::server::Daemon::logMsg(
  const int priority,
  const std::string &msg,
  const int numParams,
  const log::Param params[],
  const struct timeval &timeStamp) throw() {
  m_logger.logMsg(priority, msg, numParams, params, timeStamp);
}

//-----------------------------------------------------------------------------
// logMsg
//-----------------------------------------------------------------------------
void castor::server::Daemon::logMsg(
  const int priority,
  const std::string &msg,
  const int numParams,
  const log::Param params[]) throw() {
  m_logger.logMsg(priority, msg, numParams, params);
}

//-----------------------------------------------------------------------------
// logMsg
//-----------------------------------------------------------------------------
void castor::server::Daemon::logMsg(
  const int priority,
  const std::string &msg) throw() {
  m_logger.logMsg(priority, msg);
}
