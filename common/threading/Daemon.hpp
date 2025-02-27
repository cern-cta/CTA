/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

namespace cta::server {

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
   * @param stdOut Stream representing standard out
   * @param stdErr Stream representing standard error
   * @param log Object representing the API of the logging system
   */
  explicit Daemon(cta::log::Logger& log) noexcept;

  /**
   * Destructor
   */
  virtual ~Daemon() = default;

protected:

  /**
   * Tells the daemon object that the command-line has been parsed.  This
   * method allows subclasses to implement their own command-line parsing logic,
   * whilst enforcing the fact that they must provide values for the options and
   * arguments this class requires.
   *
   * @param foreground Set to true if the daemon should run in the foreground.
   */
  void setCommandLineHasBeenParsed(const bool foreground) noexcept;

  /**
   * Object representing the API of the CTA logging system.
   */
  cta::log::Logger &m_log;

private:


}; // class Daemon

} // namespace cta::server
