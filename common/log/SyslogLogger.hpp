/******************************************************************************
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
 * Interface to the CASTOR logging system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "common/log/Logger.hpp"
#include "common/threading/Mutex.hpp"

#include <map>
#include <syslog.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

namespace cta {
namespace log {

/**
 * Class implementaing the API of the CASTOR logging system.
 */
class SyslogLogger: public Logger {
public:

  /**
   * Constructor
   *
   * @param socketName The socket to which the logging system should write.
   * @param programName The name of the program to be prepended to every log
   * message.
   * @param logMask The log mask.
   */
  SyslogLogger(const std::string &socketName, const std::string &programName,
    const int logMask);

  /**
   * Destructor.
   */
  ~SyslogLogger();

  /**
   * Prepares the logger object for a call to fork().
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  void prepareForFork() ;

protected:

  /**
   * The socket to which the logging system should write.
   */
  std::string m_socketName;

  /**
   * Mutex used to protect the critical section of the SyslogLogger
   * object.
   */
  threading::Mutex m_mutex;

  /**
   * The file descriptor of the socket used to send messages to syslog.
   */
  int m_logFile;

  /**
   * Connects to syslog.
   *
   * Please note that this method must be called from within the critical
   * section of the SyslogLogger object.
   *
   * If the connection with syslog is already open then this method does
   * nothing.
   *
   * This method does not throw an exception if the connection cannot be made
   * to syslog.  In this case the internal state of the SyslogLogger
   * object reflects the fact that no connection was made.
   */
  void openLog();

  /**
   * A reduced version of syslog.  This method is able to set the message
   * timestamp.  This is necessary when logging messages asynchronously of there
   * creation, such as when retrieving logs from the DB.
   *
   * @param msg The message to be logged.
   */
  void reducedSyslog(std::string msg);

  /**
   * Closes the connection to syslog.
   *
   * Please note that this method must be called from within the critical
   * section of the SyslogLogger object.
   *
   * If the connection to syslog is already closed then this method does
   * nothing.
   */
  void closeLog();

}; // class SyslogLogger

} // namespace log
} // namespace cta

