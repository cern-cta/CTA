/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Errnum.hpp"
#include "common/threading/Daemon.hpp"
#include "common/threading/System.hpp"
#include <getopt.h>

#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::server::Daemon::Daemon(cta::log::Logger &log) throw():
  m_log(log),
  m_foreground(false),
  m_commandLineHasBeenParsed(false) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::server::Daemon::~Daemon() {
}

//------------------------------------------------------------------------------
// getServerName
//------------------------------------------------------------------------------
const std::string &cta::server::Daemon::getServerName() const throw() {
  return m_log.getProgramName();
}

//------------------------------------------------------------------------------
// getForeground
//------------------------------------------------------------------------------
bool cta::server::Daemon::getForeground() const
   {
  if(!m_commandLineHasBeenParsed) {
    CommandLineNotParsed ex;
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
void cta::server::Daemon::setCommandLineHasBeenParsed(const bool foreground)
  throw() {
  m_foreground = foreground;
  m_commandLineHasBeenParsed = true;
}

//------------------------------------------------------------------------------
// daemonizeIfNotRunInForegroundAndSetUserAndGroup
//------------------------------------------------------------------------------
void cta::server::Daemon::daemonizeIfNotRunInForegroundAndSetUserAndGroup(const std::string &userName,
  const std::string &groupName) {
  // Do nothing if already a daemon
  if (1 == getppid())  {
    return;
  }

  // If the daemon is to be run in the background
  if (!m_foreground) {
    m_log.prepareForFork();

    {
      pid_t pid = 0;
      cta::exception::Errnum::throwOnNegative(pid = fork(),
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
    cta::exception::Errnum::throwOnNegative(setsid(),
      "Failed to daemonize: Failed to run daemon is a new session");

    // Redirect standard files to /dev/null
    cta::exception::Errnum::throwOnNull(
      freopen("/dev/null", "r", stdin),
      "Failed to daemonize: Falied to freopen stdin");
    cta::exception::Errnum::throwOnNull(
      freopen("/dev/null", "w", stdout),
      "Failed to daemonize: Failed to freopen stdout");
    cta::exception::Errnum::throwOnNull(
      freopen("/dev/null", "w", stderr),
      "Failed to daemonize: Failed to freopen stderr");
  } // if (!m_foreground)

  // Change the user and group of the daemon process
  std::list<log::Param> params = {
    log::Param("userName", userName),
    log::Param("groupName", groupName)};
  m_log(log::INFO, "Setting user name and group name of current process", params);
  cta::System::setUserAndGroup(userName, groupName);

  // Ignore SIGPIPE (connection lost with client)
  // and SIGXFSZ (a file is too big)
  signal(SIGPIPE, SIG_IGN);
  signal(SIGXFSZ, SIG_IGN);
}

