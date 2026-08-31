/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace cta::runtime {

class ValidationResult {
public:
  void addError(std::string_view field, std::string message);
  void merge(std::string_view prefix, ValidationResult child);

  bool ok() const { return m_errors.empty(); }

  std::string what() const;

private:
  struct Error {
    std::string field;
    std::string message;
  };

  std::vector<Error> m_errors;
};

}  // namespace cta::runtime
