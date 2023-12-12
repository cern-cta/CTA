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
