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

#include "common/log/Logger.hpp"

#include <ostream>

namespace cta::log {

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
  LogContext(Logger&logger)
    throw();

  /**
   * Destructor.
   */
  virtual ~LogContext() throw() {};
  
  /**
   * Access to the logger object.
   * @return  reference to this context's logger
   */
  Logger & logger() const throw() { return m_log; }

  /**
   * Add a parameter to the container. Replaces any parameter going by the same
   * name. Does not throw exceptions (fails silently).
   * @param param
   */
  void pushOrReplace(const Param & param) throw();
  
  /**
   * Move a parameter with a given name to the end of the container it it 
   * present.
   * 
   * @param paramName  The name of the parameter to check and move.
   */
  void moveToTheEndIfPresent(const std::string &paramName) throw();

  /**
   * Removes a parameter from the list.
   * @param paramName value of param.getName();
   */
  void erase(const std::string & paramName) throw();

  /**
   * Clears the context content.
   */
  
  void clear();
  
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
   * Helper class to find parameters by name.
   */
  class ParamNameMatcher {
  public:
    ParamNameMatcher(const std::string & name) throw(): m_name(name) {}
    bool operator() (const Param & p) throw() { return m_name == p.getName(); }
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

class ScopedParamContainer{
  public:
    ScopedParamContainer(LogContext & context):m_context(context) {}
    ~ScopedParamContainer() {
      for(auto it = m_names.cbegin(); it != m_names.cend(); ++it) {
        m_context.erase(*it);
      }
    }

    template <class T> ScopedParamContainer& add(const std::string& s,const T& t){
      m_context.pushOrReplace(Param(s,t));
      m_names.push_back(s);
      return *this;
    }
  private:
        
    LogContext & m_context;
    std::list<std::string> m_names;
};

std::ostream & operator << (std::ostream & os , const LogContext & lc);

} // namespace cta::log
