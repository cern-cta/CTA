/******************************************************************************
 *                      castor/log/DummyLogger.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/log/Logger.hpp"

#include <map>
#include <pthread.h>
#include <syslog.h>
#include <sys/time.h>

namespace castor {
namespace log {

/**
 * A dummy logger class whose implementation of the API of the CASTOR logging
 * system does nothing.
 *
 * The primary purpose of this class is to facilitate the unit testing of
 * classes that require a logger object.  Using an instance of this class
 * during unit testing means that no logs will actually be written to a log
 * file.
 */
class DummyLogger: public Logger {
public:

  /**
   * Constructor
   *
   * @param programName The name of the program to be prepended to every log
   * message.
   */
  DummyLogger(const std::string &programName)
    throw(castor::exception::Exception, castor::exception::InvalidArgument);

  /**
   * Destructor.
   */
  ~DummyLogger() throw();

  /**
   * Prepares the logger object for a call to fork().
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  void prepareForFork() ;

  /**
   * Dummy operator() method that does nothing.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const std::vector<Param> &params,
    const struct timeval &timeStamp) throw();

  /**
   * Dummy operator() method that does nothing.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const std::list<Param> &params,
    const struct timeval &timeStamp) throw();

  /**
   * Dummy operator() method that does nothing.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const int numParams,
    const Param params[],
    const struct timeval &timeStamp) throw();

  /**
   * Dummy operator() method that does nothing.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const std::vector<Param> &params) throw();

  /**
   * Dummy operator() method that does nothing.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const std::list<Param> &params) throw();

  /**
   * Dummy operator() method that does nothing.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   */
  void operator() (
    const int priority,
    const std::string &msg,
    const int numParams,
    const Param params[]) throw();

  /**
   * Dummy operator() method that does nothing.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   */
  void operator() (
    const int priority,
    const std::string &msg) throw();

}; // class DummyLogger

} // namespace log
} // namespace castor

