/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ParseResult.hpp"

#include <algorithm>

namespace cta::runtime::parsing {

ParseResult::ParseResult() = default;

ParseResult::ParseResult(std::string_view error) : m_error(error) {}

ParseResult::ParseResult(std::string_view fieldName, const ParseResult& child)
    : m_fieldName(fieldName),
      m_childErrors {child} {}

ParseResult::ParseResult(std::string_view fieldName, const std::vector<ParseResult>& children)
    : m_fieldName(fieldName),
      m_childErrors {children} {}

std::string ParseResult::what(int indent) const {
  const int indentIncrement = 4;
  if (ok()) {
    return "";
  }

  if (m_childErrors.empty()) {
    return m_error + '\n';
  }
  std::string message;
  if (!m_fieldName.empty()) {
    message += "Failed to parse field '" + m_fieldName + "':\n";
  }
  // Ensure messages are consistently sorted and grouped
  auto sortedChildren = m_childErrors;
  std::ranges::sort(sortedChildren, [](const auto& a, const auto& b) {
    if (!a.m_error.empty() && !b.m_error.empty()) {
      return a.m_error < b.m_error;
    }
    return a.m_fieldName < b.m_fieldName;
  });

  std::string indentation(indent, ' ');
  for (size_t idx = 1; const auto& childErr : sortedChildren) {
    message += indentation + std::to_string(idx) + ") " + childErr.what(indent + indentIncrement);
    idx++;
  }
  return message;
}

bool ParseResult::ok() const {
  return m_error.empty() && m_childErrors.empty();
}

}  // namespace cta::runtime::parsing
