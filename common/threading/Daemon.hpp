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

#pragma once

#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"

#include <ostream>

namespace cta { namespace server {

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
  Daemon(log::Logger &log)
    throw();

  /**
   * Destructor.
   */
  virtual ~Daemon();

  /**
   * Returns this server's name as used by the CASTOR logging system.
   */
  const std::string &getServerName() const throw();

  /**
   * Returns true if the daemon is configured to run in the foreground.
   */
  bool getForeground() const ;

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
   * Please make sure that the setForeground() method has been called as
   * appropriate before this method is called.
   *
   * This method takes into account whether or not the dameon should run in
   * foregreound or background mode (m_foreground).
   *
   * @param runAsStagerSuperuser Set to true if the user ID and group ID of the
   * daemon should be set to those of the stager superuser.
   */
  void daemonizeIfNotRunInForeground(const bool runAsStagerSuperuser);

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

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

}; // class Daemon

} // namespace server
} // namespace castor

