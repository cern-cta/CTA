/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <vector>

namespace cta::runtime::parsing {

class ParseResult {
public:
  static ParseResult success() { return ParseResult(); }

  static ParseResult error(std::string_view error) { return ParseResult(error); }

  static ParseResult error(std::string_view fieldName, const ParseResult& child) {
    return ParseResult(fieldName, child);
  }

  static ParseResult error(std::string_view fieldName, const std::vector<ParseResult>& children) {
    return ParseResult(fieldName, children);
  }

  // TODO: add test to verify error message
  std::string what(int indent = 0) const {
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
    std::sort(sortedChildren.begin(), sortedChildren.end(), [](const auto& a, const auto& b) {
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

  bool ok() const { return m_error.empty() && m_childErrors.empty(); }

  void addError(const ParseResult& child) { m_childErrors.push_back(child); }

private:
  ParseResult() {}

  ParseResult(std::string_view error) : m_error(error) {}

  ParseResult(std::string_view fieldName, const ParseResult& child) : m_fieldName(fieldName), m_childErrors {child} {}

  ParseResult(std::string_view fieldName, const std::vector<ParseResult>& children)
      : m_fieldName(fieldName),
        m_childErrors {children} {}

  std::string m_fieldName;
  std::string m_error;
  std::vector<ParseResult> m_childErrors;
};

}  // namespace cta::runtime::parsing
