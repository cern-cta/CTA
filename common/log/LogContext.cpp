/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/LogContext.hpp"
#include "common/log/Param.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <algorithm>
#include <bfd.h>

namespace cta::log {

LogContext::LogContext(Logger& logger) noexcept:
m_log(logger) {}

void LogContext::pushOrReplace(const Param& param) noexcept {
  ParamNameMatcher match(param.getName());
  std::list<Param>::iterator i =
      std::find_if(m_params.begin(), m_params.end(), match);
  if (i != m_params.end()) {
    i->setValue(param.getValueVariant());
  } else {
    m_params.push_back(param);
  }
}

void LogContext::moveToTheEndIfPresent(const std::string& paramName) noexcept {
  ParamNameMatcher match(paramName);
  std::list<Param>::iterator i =
      std::find_if(m_params.begin(), m_params.end(), match);
  if (i != m_params.end()) {
    const Param param(paramName, i->getValueVariant());
    m_params.erase(i);
    m_params.push_back(param);
  }
}

void LogContext::erase(const std::set<std::string>& paramNamesSet) noexcept {
  ParamNameMatcher match(paramNamesSet);
  m_params.erase(std::remove_if(m_params.begin(), m_params.end(), match), m_params.end());
}

void LogContext::clear() {
  m_params.clear();
}

void LogContext::log(int priority, std::string_view msg) noexcept {
  m_log(priority, msg, m_params);
}

void LogContext::logBacktrace(const int priority, std::string_view backtrace) noexcept {
  // Sanity check to prevent substr from throwing exceptions
  if (!backtrace.size())
    return;
  size_t position = 0;
  int lineNumber = 0;
  bool stillGoing = true;
  while(stillGoing) {
    size_t next = backtrace.find_first_of("\n", position);
    std::string line;
    if(next != std::string::npos) {
      line = backtrace.substr(position, next - position);
      // If our position is out of range, substr would throw an exception
      // so we check here if we would get out of range.
      position = next + 1;
      if (position >= backtrace.size())
        stillGoing = false;
    } else {
      stillGoing=false;
      line = backtrace.substr(position);
    }
    if (line.size()) {
      ScopedParam sp1 (*this, Param("traceFrameNumber", lineNumber++));
      ScopedParam sp2 (*this, Param("traceFrame", line));
      log(priority, "Stack trace");
    }
  }
}

LogContext::ScopedParam::ScopedParam(
    LogContext& context,
    const Param& param) noexcept:
    m_context(context), m_name(param.getName()) {
  m_context.pushOrReplace(param);
}

LogContext::ScopedParam::~ScopedParam() noexcept {
   m_context.erase({m_name});
}

std::ostream & operator << (std::ostream & os,
    const LogContext & lc) {
  bool first=true;
  for (std::list<Param>::const_iterator p = lc.m_params.begin();
      p != lc.m_params.end(); ++p) {
    if (!first) {
      os << " ";
    } else {
      first = false;
    }
    os << p->getName() << "=" << p->getValueStr();
  }
  return os;
}

} // namespace cta::log
