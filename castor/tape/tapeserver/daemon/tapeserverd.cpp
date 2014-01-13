/******************************************************************************
 *                      tapeserverd.cpp
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

#include <iostream>
#include <exception>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sstream>
#include <signal.h>

#include "tapeserverd.hpp"
#include "castor/log/LoggerImplementation.hpp"
#include "log.h"
#include "castor/io/AbstractSocket.hpp"

int main(int argc, char ** argv) {
  return castor::tape::Server::Daemon::main(argc, argv);
}

int castor::tape::Server::Daemon::main(int argc, char ** argv) throw() {
  try {
    /* Before anything, we need a logger: */
    castor::log::LoggerImplementation logger("tapeserverd");
    try {
      /* Setup the object (parse command line, register logger) */
      castor::tape::Server::Daemon daemon(argc, argv, logger);
      /* ... and run. */
      daemon.run();
    /* Catch all block for any left over exception which will go to the logger */
    } catch (std::exception & e) {
      logger.logMsg(LOG_ALERT, std::string("Uncaught standard exception in tapeserverd:\n")+
      e.what());
      exit(EXIT_FAILURE);
    } catch (...) {
      logger.logMsg(LOG_ALERT, std::string("Uncaught non-standard exception in tapeserverd."));
      exit(EXIT_FAILURE);
    }
  /* Catch-all block for before logger creation. */
  } catch (std::exception & e) {
    std::cerr << "Uncaught standard exception in tapeserverd:" << std::endl
        << e.what() << std::endl;
    exit(EXIT_FAILURE);
  } catch (...) {
    std::cerr << "Uncaught non-standard exception in tapeserverd." << std::endl;
    exit(EXIT_FAILURE);
  }
  return (EXIT_SUCCESS);
}

castor::tape::Server::Daemon::Daemon(int argc, char** argv, 
    castor::log::Logger & logger) throw (castor::tape::Exception):
castor::server::Daemon(logger),
    m_option_run_directory ("/var/log/castor") {
  parseCommandLineOptions(argc, argv);
}

void castor::tape::Server::Daemon::run() {
  /* Block signals, we will handle them synchronously */
  blockSignals();
  /* Daemonize if requested */
  if (!m_foreground) daemonize();
  /* Setup the the mother forker, which will spawn and handle the tape sessions */
  /* TODO */
  /* Setup the listening socket for VDQM requests */
  /* TODO */
  /* Loop on both */
}

void castor::tape::Server::Daemon::parseCommandLineOptions(int argc, char** argv)
throw (castor::tape::Exception)
{
  /* Expect -f or --foreground */
  struct ::option opts[] = {
    { "foreground", no_argument, 0, 'f'},
    { 0, 0, 0, 0}
  };
  int c;
  while (-1 != (c = getopt_long(argc, argv, ":f", opts, NULL))) {
    switch (c) {
      case 'f':
        m_foreground = true;
        break;
      case ':':
      {
        castor::tape::exceptions::InvalidArgument ex(std::string("The -") + (char) optopt + " option requires a parameter");
        throw ex;
      }
      case '?':
      {
        std::stringstream err("Unknown command-line option");
        if (optopt) err << std::string(": -") << optopt;
        castor::tape::exceptions::InvalidArgument ex(err.str().c_str());
        throw ex;
      }
      default:
      {
        std::stringstream err;
        err << "getopt_long returned the following unknown value: 0x" <<
            std::hex << (int) c;
        castor::tape::exceptions::InvalidArgument ex(err.str().c_str());
        throw ex;
      }
    }
  }
}

void castor::tape::Server::Daemon::daemonize()
{
  pid_t pid, sid;

  /* already a daemon */
  if (getppid() == 1) return;

  /* Fork off the parent process */
  castor::tape::exceptions::throwOnNegativeWithErrno(pid = fork(),
    "Failed to fork in castor::tape::Server::Daemon::daemonize");
  /* If we got a good PID, then we can exit the parent process. */
  if (pid > 0) {
    exit(EXIT_SUCCESS);
  }
  /* Change the file mode mask */
  umask(0);
  /* Create a new session for the child process */
  castor::tape::exceptions::throwOnNegativeWithErrno(sid = setsid(),
    "Failed to create new session in castor::tape::Server::Daemon::daemonize");
  /* At this point we are executing as the child process, and parent process should be init */
  if (getppid() != 1) { 
    castor::tape::Exception e("Failed to detach from parent process in castor::tape::Server::Daemon::daemonize");
    throw e;
  }
  /* Change the current working directory.  This prevents the current
     directory from being locked; hence not being able to remove it. */
  castor::tape::exceptions::throwOnNegativeWithErrno(
    chdir(m_option_run_directory.c_str()),
    std::string("Failed to chdir in castor::tape::Server::Daemon::daemonize"
      " ( destination directory: ") + m_option_run_directory + ")");
  /* Redirect standard files to /dev/null */
  castor::tape::exceptions::throwOnNullWithErrno(
    freopen("/dev/null", "r", stdin),
    "Failed to freopen stdin in castor::tape::Server::Daemon::daemonize");
  castor::tape::exceptions::throwOnNullWithErrno(
    freopen("/dev/null", "r", stdout),
    "Failed to freopen stdout in castor::tape::Server::Daemon::daemonize");
  castor::tape::exceptions::throwOnNullWithErrno(
    freopen("/dev/null", "r", stderr),
    "Failed to freopen stderr in castor::tape::Server::Daemon::daemonize");
}

void castor::tape::Server::Daemon::blockSignals()
{
  sigset_t sigs;
  sigemptyset(&sigs);
  /* The list of signal that should not disturb our daemon. Some of them
   * will be dealt with synchronously in the main loop.
   * See signal(7) for full list.
   */
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
  castor::tape::exceptions::throwOnNonZeroWithErrno(
    sigprocmask(SIG_BLOCK, &sigs, NULL), 
    "Failed to sigprocmask in castor::tape::Server::Daemon::blockSignals");
}
