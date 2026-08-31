/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace cta::runtime {

/**
 * @brief Collects semantic errors found while validating an initialized config struct.
 *
 * A default-constructed result represents successful validation. Errors can be added locally or merged from child
 * configs with a field-path prefix. ConfigLoader converts a non-successful root result into a UserError.
 */
class ValidationResult {
public:
  /**
   * @brief Add an error for a field relative to the config currently being validated.
   *
   * @param field Field name or relative field path.
   * @param message Description of the violated constraint, without a trailing full stop.
   */
  void addError(std::string_view field, std::string message);

  /**
   * @brief Merge errors from a child config and prefix their field paths.
   *
   * @param prefix Name of the child field in the parent config.
   * @param child Validation result returned by the child config.
   */
  void merge(std::string_view prefix, ValidationResult child);

  /**
   * @return true if no validation errors have been recorded.
   */
  bool ok() const { return m_errors.empty(); }

  /**
   * @brief Format all errors as a deterministic, sorted, numbered list.
   *
   * @return An empty string for a successful result, otherwise the formatted errors.
   */
  std::string what() const;

private:
  struct Error {
    std::string field;
    std::string message;
  };

  std::vector<Error> m_errors;
};

}  // namespace cta::runtime
