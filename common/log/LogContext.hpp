/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Logger.hpp"

#include <ostream>
#include <set>
#include <vector>

namespace cta::log {

/**
 * Container for a set of parameters to be used repetitively in logs. The
 * container is ordered , by order of inclusion. There can be only one
 * parameter value per parameter name.
 */
class LogContext {
  friend std::ostream& operator<<(std::ostream& os, const LogContext& lc);

public:
  /**
   * Constructor
   *
   * @param programName The name of the program to be prepended to every log message
   */
  explicit LogContext(Logger& logger) noexcept;

  /**
   * Destructor
   */
  virtual ~LogContext() = default;

  /**
   * Access to the logger object.
   * @return  reference to this context's logger
   */
  Logger& logger() const noexcept { return m_log; }

  /**
   * Add a parameter to the container. Replaces any parameter going by the same
   * name. Does not throw exceptions (fails silently).
   * @param param
   */
  void push(const Param& param) noexcept;
  void push(Param&& param) noexcept;

  /**
   * Removes a parameter from the list.
   * @param paramNamesSet set of values of param.getName();
   */
  void erase(const std::set<std::string>& paramNamesSet) noexcept;

  /**
   * Clears the context content.
   */

  void clear();

  /**
   * Writes a message into the CTA logging system. Note that no exception
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
  virtual void log(int priority, std::string_view msg) noexcept;

  /**
   * Logs a multiline backtrace as multiple entries in the logs, without
   * the context
   * @param priority the logging priority
   * @param backtrace the multi-line (\n separated) stack trace
   */
  virtual void logBacktrace(int priority, std::string_view backtrace) noexcept;

  /**
   * Small introspection function to help in tests
   * @return size
   */
  size_t size() const { return m_params.size(); }

  /**
   * Scoped parameter addition to the context. Constructor adds the parameter,
   * destructor erases it.
   */
  class ScopedParam {
  public:
    ScopedParam(LogContext& context, const Param& param) noexcept;
    ScopedParam(LogContext& context, Param&& param) noexcept;
    ~ScopedParam() noexcept;

  private:
    LogContext& m_context;
    std::string m_name;
  };

private:
  Logger& m_log;
  std::vector<Param> m_params;
};  // class LogContext

class ScopedParamContainer {
public:
  explicit ScopedParamContainer(LogContext& context) : m_context(context) {}

  ~ScopedParamContainer() { m_context.erase(m_names); }

  template<typename S, typename T>
    requires std::constructible_from<std::string, S&&>
  ScopedParamContainer& add(S&& s, T&& t) {
    m_names.emplace(s);
    m_context.push(Param(std::forward<S>(s), std::forward<T>(t)));
    return *this;
  }

  void log(int iPriority, std::string_view strvMsg) noexcept { m_context.log(iPriority, strvMsg); }

private:
  LogContext& m_context;
  std::set<std::string> m_names;
};

std::ostream& operator<<(std::ostream& os, const LogContext& lc);

}  // namespace cta::log
