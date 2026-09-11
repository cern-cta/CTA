/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <vector>

namespace cta::runtime::parsing {

/**
 * @brief Encapsulates the result of parsing a TOML node into a config field.
 *
 * A result is either successful, a leaf error, or a field containing one or more child errors. Nested results preserve
 * the TOML structure so parsing failures can be reported together with readable field context.
 */
class ParseResult {
public:
  /**
   * @return A successful parse result.
   */
  static ParseResult success() { return ParseResult(); }

  /**
   * @brief Create a leaf parse error.
   *
   * @param error Human-readable error message.
   */
  static ParseResult error(std::string_view error) { return ParseResult(error); }

  /**
   * @brief Wrap a child error with its containing field name.
   *
   * @param fieldName Name of the containing field.
   * @param child Error returned while parsing that field.
   */
  static ParseResult error(std::string_view fieldName, const ParseResult& child) {
    return ParseResult(fieldName, child);
  }

  /**
   * @brief Wrap multiple child errors with their containing field name.
   *
   * @param fieldName Name of the containing field.
   * @param children Errors returned while parsing that field.
   */
  static ParseResult error(std::string_view fieldName, const std::vector<ParseResult>& children) {
    return ParseResult(fieldName, children);
  }

  /**
   * @brief Format the error tree as a deterministic, numbered list.
   *
   * @param indent Initial indentation used when formatting nested errors.
   * @return An empty string for a successful result, otherwise the formatted errors.
   */
  std::string what(int indent = 0) const;

  /**
   * @return true if no parsing errors have been recorded.
   */
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
