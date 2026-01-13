/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/LogContext.hpp"

#include "common/log/Logger.hpp"
#include "common/log/Param.hpp"

#include <algorithm>
#include <bfd.h>
#include <ranges>

namespace cta::log {

LogContext::LogContext(Logger& logger) noexcept : m_log(logger) {}

void LogContext::push(const Param& param) noexcept {
  auto i = std::ranges::find_if(m_params, [&param](const Param& p) { return p.getName() == param.getName(); });
  if (i != m_params.end()) {
    i->setValue(param.getValueVariant());
  } else {
    m_params.push_back(param);
  }
}

void LogContext::push(Param&& param) noexcept {
  auto i = std::ranges::find_if(m_params, [&param](const Param& p) { return p.getName() == param.getName(); });
  if (i != m_params.end()) {
    i->setValue(param.getValueVariant());
  } else {
    m_params.push_back(std::move(param));
  }
}

void LogContext::erase(const std::set<std::string>& paramNamesSet) noexcept {
  auto toEraseRange =
    std::ranges::remove_if(m_params, [&paramNamesSet](const Param& p) { return paramNamesSet.contains(p.getName()); });
  m_params.erase(toEraseRange.begin(), toEraseRange.end());
}

void LogContext::clear() {
  m_params.clear();
}

void LogContext::log(int priority, std::string_view msg) noexcept {
  m_log(priority, msg, m_params);
}

void LogContext::logBacktrace(const int priority, std::string_view backtrace) noexcept {
  // Sanity check to prevent substr from throwing exceptions
  if (!backtrace.size()) {
    return;
  }
  size_t position = 0;
  int lineNumber = 0;
  bool stillGoing = true;
  while (stillGoing) {
    size_t next = backtrace.find_first_of("\n", position);
    std::string line;
    if (next != std::string::npos) {
      line = backtrace.substr(position, next - position);
      // If our position is out of range, substr would throw an exception
      // so we check here if we would get out of range.
      position = next + 1;
      if (position >= backtrace.size()) {
        stillGoing = false;
      }
    } else {
      stillGoing = false;
      line = backtrace.substr(position);
    }
    if (line.size()) {
      ScopedParam sp1(*this, Param("traceFrameNumber", lineNumber++));
      ScopedParam sp2(*this, Param("traceFrame", line));
      log(priority, "Stack trace");
    }
  }
}

LogContext::ScopedParam::ScopedParam(LogContext& context, const Param& param) noexcept
    : m_context(context),
      m_name(param.getName()) {
  m_context.push(param);
}

LogContext::ScopedParam::ScopedParam(LogContext& context, Param&& param) noexcept
    : m_context(context),
      m_name(param.getName()) {
  m_context.push(std::move(param));
}

LogContext::ScopedParam::~ScopedParam() noexcept {
  m_context.erase({m_name});
}

std::ostream& operator<<(std::ostream& os, const LogContext& lc) {
  bool first = true;
  for (auto& p : lc.m_params) {
    if (!first) {
      os << " ";
    } else {
      first = false;
    }
    os << p.getName() << "=" << p.getValueStr();
  }
  return os;
}

}  // namespace cta::log
