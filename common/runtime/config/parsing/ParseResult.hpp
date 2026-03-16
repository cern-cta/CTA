/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <vector>

namespace cta::runtime::parsing {

/**
   * @brief Encapsulates the result of a parsing step.
   *
   * In this case there are a few scenarios:
   * - Parsing was a success. In this case, the class is essentially empty.
   * - Parsing was a failure and we were at a leave node. In this case, only an error message is passed in.
   * - Parsing was a failure and we were not at a leave node. In this case, the error is determined by the child parse results.
   *   Here all child parse results should be errors. They may be errors at leave nodes or other intermediate errors.
   *
   * From these cases, the what() function can constructed a comprehensive error message which follows the tree-like structure of the parser.
   *
   */
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

  std::string what(int indent = 0) const;

  bool ok() const;

private:
  ParseResult();
  ParseResult(std::string_view error);
  ParseResult(std::string_view fieldName, const ParseResult& child);
  ParseResult(std::string_view fieldName, const std::vector<ParseResult>& children);

  std::string m_fieldName;
  std::string m_error;
  std::vector<ParseResult> m_childErrors;
};

}  // namespace cta::runtime::parsing
