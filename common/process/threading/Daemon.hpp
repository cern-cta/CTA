/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  void setCommandLineHasBeenParsed(const bool foreground) noexcept;

  /**
   * Daemonizes the daemon if it has not been configured to run in the
   * foreground.
   *
   * Please make sure that the setForeground() method has been called as
   * appropriate before this method is called.
   *
   * This method takes into account whether or not the daemon should run in
   * foregreound or background mode (m_foreground).
   */
  void daemonizeIfNotRunInForeground();

  /**
   * Object representing the API of the CASTOR logging system.
   */
  cta::log::Logger& m_log;

private:
  /**
   * Flag indicating whether the server should run in foreground or background
   * mode.
   */
  bool m_foreground = false;

  /**
   * True if the command-line has been parsed.
   */
  bool m_commandLineHasBeenParsed = false;

};  // class Daemon

}  // namespace cta::server
