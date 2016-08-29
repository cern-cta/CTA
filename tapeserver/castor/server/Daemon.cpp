/*******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 ******************************************************************************/

#include "castor/exception/Errnum.hpp"
#include "castor/server/Daemon.hpp"
#include "castor/server/ThreadNotification.hpp"
#include "castor/System.hpp"
#include <getopt.h>

#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::Daemon::Daemon(std::ostream &stdOut, std::ostream &stdErr,
  log::Logger &log) throw():
  m_stdOut(stdOut),
  m_stdErr(stdErr),
  m_log(log),
  m_foreground(false),
  m_commandLineHasBeenParsed(false) {
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
  char *argv[])  {
  struct ::option longopts[4];

  longopts[0].name = "foreground";
  longopts[0].has_arg = no_argument;
  longopts[0].flag = NULL;
  longopts[0].val = 'f';

  longopts[1].name = "config";
  longopts[1].has_arg = required_argument;
  longopts[1].flag = NULL;
  longopts[1].val = 'c';

  longopts[2].name = "help";
  longopts[2].has_arg = no_argument;
  longopts[2].flag = NULL;
  longopts[2].val = 'h';

  memset(&longopts[3], 0, sizeof(struct ::option));

  char c;
  // Reset getopt's global variables to make sure we start fresh
  optind=0;
  // Prevent getopt from printing out errors on stdout
  opterr=0;
  // We ask getopt to not reshuffle argv ('+')
  while ((c = getopt_long(argc, argv, "+fc:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'c':
      setenv("PATH_CONFIG", optarg, 1);
      m_stdOut << "Using configuration file " << optarg << std::endl;
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
    "\t--help                  or -h         \tPrint this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch\n";
}

//------------------------------------------------------------------------------
// getServerName
//------------------------------------------------------------------------------
const std::string &castor::server::Daemon::getServerName() const throw() {
  return m_log.getProgramName();
}

//------------------------------------------------------------------------------
// getForeground
//------------------------------------------------------------------------------
bool castor::server::Daemon::getForeground() const
   {
  if(!m_commandLineHasBeenParsed) {
    cta::exception::CommandLineNotParsed ex;
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

//------------------------------------------------------------------------------
// daemonizeIfNotRunInForeground
//------------------------------------------------------------------------------
void castor::server::Daemon::daemonizeIfNotRunInForeground(
  const bool runAsStagerSuperuser) {
  // Do nothing if already a daemon
  if (1 == getppid())  {
    return;
  }

  // If the daemon is to be run in the background
  if (!m_foreground) {
    m_log.prepareForFork();

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
  if (runAsStagerSuperuser) {
    castor::System::switchToCastorSuperuser();
  }

  // Ignore SIGPIPE (connection lost with client)
  // and SIGXFSZ (a file is too big)
  signal(SIGPIPE, SIG_IGN);
  signal(SIGXFSZ, SIG_IGN);
}

