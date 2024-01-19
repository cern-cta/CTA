/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
cta::server::Daemon::Daemon(cta::log::Logger &log) noexcept :
  m_log(log),
  m_foreground(false),
  m_commandLineHasBeenParsed(false) {
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
void cta::server::Daemon::setCommandLineHasBeenParsed(const bool foreground) noexcept {
  m_foreground = foreground;
  m_commandLineHasBeenParsed = true;
}

//------------------------------------------------------------------------------
// daemonizeIfNotRunInForegroundAndSetUserAndGroup
//------------------------------------------------------------------------------
void cta::server::Daemon::daemonizeIfNotRunInForegroundAndSetUserAndGroup(const std::string &userName,
  const std::string &groupName) {
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
    // to a suitable directory for us.

    // Change the file mode mask: block EXECUTE permission for USER and all permissions for GROUP and OTHER
    umask(0177);

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

