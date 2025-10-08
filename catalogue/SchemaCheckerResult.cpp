/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <iostream>

#include "SchemaCheckerResult.hpp"

namespace cta::catalogue {

SchemaCheckerResult SchemaCheckerResult::operator+=(const SchemaCheckerResult& other) {
  m_errors.insert(m_errors.end(),other.m_errors.begin(),other.m_errors.end());
  m_warnings.insert(m_warnings.end(),other.m_warnings.begin(), other.m_warnings.end());
  if(m_status == Status::SUCCESS) {
    // The status should not change if it is failed
    m_status = other.m_status;
  }
  return *this;
}

void SchemaCheckerResult::addError(const std::string& error) {
  m_errors.emplace_back(error);
  m_status = Status::FAILED;
}

void SchemaCheckerResult::addWarning(const std::string& warning) {
  m_warnings.emplace_back(warning);
}

void SchemaCheckerResult::displayErrors(std::ostream & os) const {
  for(auto &error: m_errors) {
    os << "  ERROR: " << error << std::endl;
  }
}

void SchemaCheckerResult::displayWarnings(std::ostream& os) const {
  for(auto &warning: m_warnings) {
    os << "  WARNING: " << warning << std::endl;
  }
}

std::string SchemaCheckerResult::statusToString(const Status& status) {
  switch(status) {
    case Status::SUCCESS:
      return "SUCCESS";
    case Status::FAILED:
      return "FAILED";
    default:
      return "  UnknownStatus";
  }
}

} // namespace cta::catalogue
