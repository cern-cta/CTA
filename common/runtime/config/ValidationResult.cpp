/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ValidationResult.hpp"

#include <algorithm>
#include <utility>

namespace cta::runtime {

void ValidationResult::addError(std::string_view field, std::string message) {
  m_errors.push_back({std::string(field), std::move(message)});
}

void ValidationResult::merge(std::string_view prefix, ValidationResult child) {
  for (auto& error : child.m_errors) {
    error.field = error.field.empty() ? std::string(prefix) : std::string(prefix) + "." + error.field;
    m_errors.push_back(std::move(error));
  }
}

std::string ValidationResult::what() const {
  auto errors = m_errors;
  std::ranges::sort(errors, [](const auto& lhs, const auto& rhs) {
    return lhs.field == rhs.field ? lhs.message < rhs.message : lhs.field < rhs.field;
  });

  std::string message;
  for (std::size_t index = 0; index < errors.size(); ++index) {
    message += std::to_string(index + 1) + ") Field '" + errors[index].field + "' " + errors[index].message + ".\n";
  }
  return message;
}

}  // namespace cta::runtime
