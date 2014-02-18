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
#include "castor/exception/Errnum.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/UDPSocket.hpp"
#include "castor/server/Daemon.hpp"
#include "castor/server/ThreadNotification.hpp"
#include "castor/System.hpp"
#include "h/Cgetopt.h"

#include <signal.h>
#include <stdio.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::Daemon::Daemon(std::ostream &stdOut, std::ostream &stdErr,
  log::Logger &logger) throw():
  m_stdOut(stdOut),
  m_stdErr(stdErr),
  m_foreground(false),
  m_commandLineHasBeenParsed(false),
  m_runAsStagerSuperuser(false),
  m_logger(logger) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::Daemon::~Daemon() throw() {
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::server::Daemon::parseCommandLine(int argc,
  char *argv[]) throw(castor::exception::Exception) {
  Coptions_t longopts[4];

  longopts[0].name = "foreground";
  longopts[0].has_arg = NO_ARGUMENT;
  longopts[0].flag = NULL;
  longopts[0].val = 'f';

  longopts[1].name = "config";
  longopts[1].has_arg = REQUIRED_ARGUMENT;
  longopts[1].flag = NULL;
  longopts[1].val = 'c';

  longopts[2].name = "help";
  longopts[2].has_arg = NO_ARGUMENT;
  longopts[2].flag = NULL;
  longopts[2].val = 'h';

  longopts[3].name = 0;

  Coptind = 1;
  Copterr = 0;
  Coptreset = 1;

  char c;
  while ((c = Cgetopt_long(argc, argv, "fc:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'c':
      setenv("PATH_CONFIG", Coptarg, 1);
      m_stdOut << "Using configuration file " << Coptarg << std::endl;
      break;
    case 'h':
      help(argv[0]);
      exit(0);
      break;
    default:
      break;
    }
  }

  m_commandLineHasBeenParsed = true;
}

//------------------------------------------------------------------------------
// help
//------------------------------------------------------------------------------
void castor::server::Daemon::help(const std::string &programName)
  throw() {
  m_stdOut << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--foreground            or -f         \tRemain in the Foreground\n"
    "\t--config <config-file>  or -c         \tConfiguration file\n"
    "\t--metrics               or -m         \tEnable metrics collection\n"
    "\t--help                  or -h         \tPrint this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch\n";
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

//------------------------------------------------------------------------------
// getForeground
//------------------------------------------------------------------------------
bool castor::server::Daemon::getForeground() const
  throw(castor::exception::CommandLineNotParsed) {
  if(!m_commandLineHasBeenParsed) {
    castor::exception::CommandLineNotParsed ex;
    ex.getMessage() <<
      "Failed to determine whether or not the daemon should run in the"
      " foreground because the command-line has not yet been parsed";
    throw ex;
  }

  return m_foreground;
}

//-----------------------------------------------------------------------------
// setCommandLineParsed
//-----------------------------------------------------------------------------
void castor::server::Daemon::setCommandLineHasBeenParsed(const bool foreground)
  throw() {
  m_foreground = foreground;
  m_commandLineHasBeenParsed = true;
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
  // Do nothing if already a daemon
  if (1 == getppid())  {
    return;
  }

  // If the daemon is to be run in the background
  if (!m_foreground) {
    m_logger.prepareForFork();

    {
      pid_t pid = 0;
      castor::exception::Errnum::throwOnNegative(pid = fork(),
        "Failed to daemonize: Failed to fork");
      // If we got a good PID, then we can exit the parent process
      if (0 < pid) {
        exit(EXIT_SUCCESS);
      }
    }

    // We could set our working directory to '/' here with a call to chdir(2).
    // For the time being we don't and leave it to the initd script to change
    // to a suitable directory for us!

    // Change the file mode mask
    umask(0);

    // Run the daemon in a new session
    castor::exception::Errnum::throwOnNegative(setsid(),
      "Failed to daemonize: Failed to run daemon is a new session");

    // Redirect standard files to /dev/null
    castor::exception::Errnum::throwOnNull(
      freopen("/dev/null", "r", stdin),
      "Failed to daemonize: Falied to freopen stdin");
    castor::exception::Errnum::throwOnNull(
      freopen("/dev/null", "w", stdout),
      "Failed to daemonize: Failed to freopen stdout");
    castor::exception::Errnum::throwOnNull(
      freopen("/dev/null", "w", stderr),
      "Failed to daemonize: Failed to freopen stderr");
  } // if (!m_foreground)

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
