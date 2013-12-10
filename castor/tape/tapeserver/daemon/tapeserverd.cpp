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

#include "tapeserverd.hpp"

int main(int argc, char ** argv) {
  try {
    castor::tape::Server::Daemon daemon(argc, argv);
  } catch (std::exception & e) {
    std::cerr << "Uncaught standard exception in tapeserverd:" << std::endl
        << e.what() << std::endl;
    exit(EXIT_FAILURE);
  } catch (...) {
    std::cerr << "Uncaught non-standard exception in tapeserverd:" << std::endl;
    exit(EXIT_FAILURE);
  }
}

castor::tape::Server::Daemon::Daemon(int argc, char** argv) throw (castor::tape::Exception) {
  m_options = getCommandLineOptions(argc, argv);
  if (m_options.daemonize) daemonize();
}

castor::tape::Server::Daemon::options castor::tape::Server::Daemon::getCommandLineOptions(int argc, char** argv)
throw (castor::tape::Exception)
{
  options ret;
  /* Expect -f or --foreground */
  struct ::option opts[] = {
    { "foreground", no_argument, 0, 'f'},
    { 0, 0, 0, 0}
  };
  int c;
  while (-1 != (c = getopt_long(argc, argv, ":f", opts, NULL))) {
    switch (c) {
      case 'f':
        ret.daemonize = false;
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
  return ret;
}

void castor::tape::Server::Daemon::daemonize()
{
  pid_t pid, sid;

  /* already a daemon */
  if (getppid() == 1) return;

  /* Fork off the parent process */
  pid = fork();
  if (pid < 0) {
    castor::tape::exceptions::Errnum e("Failed to fork in castor::tape::Server::Daemon::daemonize");
    throw e;
  }
  /* If we got a good PID, then we can exit the parent process. */
  if (pid > 0) {
    exit(EXIT_SUCCESS);
  }

  /* Change the file mode mask */
  umask(0);

  /* Create a new session for the child process */
  sid = setsid();
  if (sid < 0) {
    castor::tape::exceptions::Errnum e("Failed to create new session in castor::tape::Server::Daemon::daemonize");
    throw e;
  }

  /* At this point we are executing as the child process, and parent process should be init */
  if (getppid() != 1) { 
    castor::tape::Exception e("Failed to detach from parent process in castor::tape::Server::Daemon::daemonize");
    throw e;
  }

  /* Change the current working directory.  This prevents the current
     directory from being locked; hence not being able to remove it. */
  if ((chdir(m_options.runDirectory.c_str())) < 0) {
    std::stringstream err("Failed to chdir in castor::tape::Server::Daemon::daemonize");
    err << " ( destination directory: " << m_options.runDirectory << ")";
    castor::tape::exceptions::Errnum e(err.str());
    throw e;
  }

  /* Redirect standard files to /dev/null */
  freopen("/dev/null", "r", stdin);
  freopen("/dev/null", "w", stdout);
  freopen("/dev/null", "w", stderr);
}
