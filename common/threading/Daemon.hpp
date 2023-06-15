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

#pragma once

#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"

#include <ostream>

namespace cta {
namespace server {

/**
 * This class contains the code common to all daemon classes.
 *
 * The code common to all daemon classes includes daemonization and logging.
 */
class Daemon {
public:
  CTA_GENERATE_EXCEPTION_CLASS(CommandLineNotParsed);

  /**
   * Constructor
   *
   * @param stdOut Stream representing standard out.
   * @param stdErr Stream representing standard error.
   * @param log Object representing the API of the CASTOR logging system.
   */
  Daemon(cta::log::Logger& log) throw();

  /**
   * Destructor.
   */
  virtual ~Daemon();

  /**
   * Returns this server's name as used by the CASTOR logging system.
   */
  const std::string& getServerName() const throw();

  /**
   * Returns true if the daemon is configured to run in the foreground.
   */
  bool getForeground() const;

protected:
  /**
   * Tells the daemon object that the command-line has been parsed.  This
   * method allows subclasses to implement their own command-line parsing logic,
   * whilst enforcing the fact that they must provide values for the options and
   * arguments this class requires.
   *
   * @param foreground Set to true if the daemon should run in the foreground.
   */
  void setCommandLineHasBeenParsed(const bool foreground) throw();

  /**
   * Daemonizes the daemon if it has not been configured to run in the
   * foreground.
   *
   * This method also sets the user and group of the process to the specified
   * values.
   *
   * Please make sure that the setForeground() method has been called as
   * appropriate before this method is called.
   *
   * This method takes into account whether or not the daemon should run in
   * foregreound or background mode (m_foreground).
   *
   * @param userName The name of the user.
   * @param groupName The name of the group.
   */
  void daemonizeIfNotRunInForegroundAndSetUserAndGroup(const std::string& userName, const std::string& groupName);

  /**
   * Object representing the API of the CASTOR logging system.
   */
  cta::log::Logger& m_log;

private:
  /**
   * Flag indicating whether the server should run in foreground or background
   * mode.
   */
  bool m_foreground;

  /**
   * True if the command-line has been parsed.
   */
  bool m_commandLineHasBeenParsed;

};  // class Daemon

}  // namespace server
}  // namespace cta
