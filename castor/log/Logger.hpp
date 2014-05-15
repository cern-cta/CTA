/******************************************************************************
 *                      castor/log/Logger.hpp
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

// Include Files
#include "castor/exception/InvalidArgument.hpp"
#include "castor/log/Param.hpp"

#include <list>
#include <map>
#include <pthread.h>
#include <syslog.h>
#include <sys/time.h>
#include <vector>

/**
 * It is a convention of CASTOR to use syslog level of LOG_NOTICE to label
 * user errors.  This macro helps enforce that convention and document it in
 * the code.
 */
#define LOG_USERERR LOG_NOTICE

namespace castor {
namespace log {

/**
 * Abstract class representing the API of the CASTOR logging system.
 *
 * The intended way to use the CASTOR logging API is as follows:
 *
 * 1. Keep a reference to a Logger object, for example:
 * \code{.cpp}
 *
 * class MyClassThatWillLog {
 * protected:
 *   Logger & m_log;
 *
 * public:
 *   MyClassThatWillLog(Logger &log): m_log(log) {
 *     ....
 *   }
 * }
 *
 * \endcode
 *
 * 2. To log a message, use the reference to the Logger object like a function.
 *    In other words the Logger object implements operator() and therefore
 *    behaves like a functor:
 * \code{.cpp}
 *
 * void MyClassThatWillLog::aMethodThatWillLog() {
 *   ....
 *   m_log(LOG_INFO, "My log message");
 *   ....
 * }
 *
 * \endcode
 *
 * The Logger object implements operator() in order to avoid the following long
 * winded syntax (which does not work by the way, so please do NOT copy and
 * paste the following example):
 * \code{.cpp}
 *
 * m_log.logMsg(LOG_INFO, "My log message");
 *
 * \endcode
 */
class Logger {
public:

  /**
   * Constructor
   *
   * @param programName The name of the program to be prepended to every log
   * message.
   */
  Logger(const std::string &programName)
    throw(castor::exception::Exception, castor::exception::InvalidArgument);

  /**
   * Destructor.
   */
  virtual ~Logger() throw() = 0;

  /**
   * Prepares the logger object for a call to fork().
   *
   * No further calls to operator() should be made after calling this
   * method until the call to fork() has completed.
   */
  virtual void prepareForFork()  = 0;

  /**
   * Returns the name of the program that is to  be prepended to every log
   * message.
   */
  const std::string &getProgramName() const throw();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  virtual void operator() (
    const int priority,
    const std::string &msg,
    const std::vector<Param> &params,
    const struct timeval &timeStamp) throw() = 0;

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  virtual void operator() (
    const int priority,
    const std::string &msg,
    const std::list<Param> &params,
    const struct timeval &timeStamp) throw() = 0;

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  virtual void operator() (
    const int priority,
    const std::string &msg,
    const int numParams,
    const Param params[],
    const struct timeval &timeStamp) throw() = 0;

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   */
  virtual void operator() (
    const int priority,
    const std::string &msg,
    const std::vector<Param> &params) throw() = 0;

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param params the parameters of the message.
   */
  virtual void operator() (
    const int priority,
    const std::string &msg,
    const std::list<Param> &params) throw() = 0;

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   * @param numParams the number of parameters in the message.
   * @param params the parameters of the message.
   */
  virtual void operator() (
    const int priority,
    const std::string &msg,
    const int numParams,
    const Param params[]) throw() = 0;

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   */
  virtual void operator() (
    const int priority,
    const std::string &msg) throw() = 0;

  /**
   * A template function that wraps operator() in order to get the compiler
   * to automatically determine the size of the params parameter, therefore
   *
   * Note that this version of operator() allows the caller to specify the
   * time stamp of the log message.
   *
   * @param priority the priority of the message as defined by the syslog
   * API.
   * @param msg the message.
   * @param params the parameters of the message.
   * @param timeStamp the time stamp of the log message.
   */
  template<int numParams> void operator() (
    const int priority,
    const std::string &msg,
    castor::log::Param(&params)[numParams],
    const struct timeval &timeStamp) throw() {
    operator() (priority, msg, numParams, params, timeStamp);
  }

  /**
   * A template function that wraps operator() in order to get the compiler
   * to automatically determine the size of the params parameter, therefore
   * removing the need for the devloper to provide it explicity.
   *
   * Note that this version of operator() implicitly uses the current time as
   * the time stamp of the message.
   *
   * @param priority the priority of the message as defined by the syslog
   * API.
   * @param msg the message.
   * @param params the parameters of the message.
   */
  template<int numParams> void operator() (
    const int priority,
    const std::string &msg,
    castor::log::Param(&params)[numParams]) throw() {
    operator() (priority, msg, numParams, params);
  }

protected:

  /**
   * The name of the program to be prepended to every log message.
   */
  const std::string m_programName;

}; // class Logger

} // namespace log
} // namespace castor

