/******************************************************************************
 *                      castor/log/LogContext.hpp
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
 * Interface to the CASTOR logging system
 *
 * @author Eric.Cano@cern.ch
 *****************************************************************************/

#pragma once

#include <ostream>
#include "castor/log/Logger.hpp"

namespace castor {
namespace log {

/**
 * Container for a set of parameters to be used repetitively in logs. The
 * container is ordered , by order of inclusion. There can be only one
 * parameter value per parameter name.
 */
class LogContext {
  friend std::ostream & operator << (std::ostream & os , const LogContext & lc);
public:

  /**
   * Constructor
   *
   * @param programName The name of the program to be prepended to every log
   * message.
   */
  LogContext(castor::log::Logger &logger)
    throw();

  /**
   * Destructor.
   */
  virtual ~LogContext() throw() {};
  
  /**
   * Access to the logger object.
   * @return  reference to this context's logger
   */
  castor::log::Logger & logger() throw() { return m_log; }

  /**
   * Add a parameter to the container. Replaces any parameter going by the same
   * name. Does not throw exceptions (fails silently).
   * @param param
   */
  void pushOrReplace(const Param & param) throw ();

  /**
   * Removes a parameter from the list.
   * @param paramName value of param.getName();
   */
  void erase(const std::string & paramName) throw ();

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   *
   * Note that this version of logMsg() implicitly uses the current time as
   * the time stamp of the message.
   * 
   * All the parameters present in the context will be added to the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   */
  virtual void log(
    const int priority,
    const std::string &msg) throw();
  
  /**
   * Logs a multiline backtrace as multiple entries in the logs, without
   * the context
   * @param priority the logging priority
   * @param backtrace the multi-line (\n separated) stack trace
   */
  virtual void logBacktrace(
    const int priority,
    const std::string &backtrace) throw();
  
  /**
   * Small introspection function to help in tests
   * @return size
   */
  size_t size() const { return m_params.size(); }

  /**
   * Writes a message into the CASTOR logging system. Note that no exception
   * will ever be thrown in case of failure. Failures will actually be
   * silently ignored in order to not impact the processing.
   * 
   * All the parameters present in the context will be added to the log message.
   *
   * @param priority the priority of the message as defined by the syslog API.
   * @param msg the message.
   */
  virtual void log(
    const int priority,
    const std::string &msg,
    const struct timeval &timeStamp) throw();
  /**
   * Helper class to find parameters by name.
   */
  class ParamNameMatcher {
  public:
    ParamNameMatcher(const std::string & name) throw (): m_name(name) {}
    bool operator() (const Param & p) throw () { return m_name == p.getName(); }
  private:
    std::string m_name;
  };
  
  /**
   * Scoped parameter addition to the context. Constructor adds the parameter,
   * destructor erases it.
   */
  class ScopedParam {
  public:
    ScopedParam(LogContext & context, const Param &param) throw();
    ~ScopedParam() throw();
  private:
    LogContext & m_context;
    std::string m_name;
  };
private:
  Logger & m_log;
  std::list<Param> m_params;
}; // class LogContext

std::ostream & operator << (std::ostream & os , const LogContext & lc);

} // namespace log
} // namespace castor
