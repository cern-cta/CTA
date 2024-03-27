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
    i->setValue(param.getValue());
  } else {
    m_params.push_back(param);
  }
}

void LogContext::moveToTheEndIfPresent(std::string_view paramName) noexcept {
  ParamNameMatcher match(paramName);
  std::list<Param>::iterator i = 
      std::find_if(m_params.begin(), m_params.end(), match);
  if (i != m_params.end()) {    
    const Param param(paramName,i->getValue());
    m_params.erase(i);
    m_params.push_back(param);
  }
}

void LogContext::erase(std::string_view paramName) noexcept {
  ParamNameMatcher match(paramName);
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
   m_context.erase(m_name);
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
