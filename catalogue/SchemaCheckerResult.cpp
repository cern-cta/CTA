/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <iostream>

#include "SchemaCheckerResult.hpp"

namespace cta {
namespace catalogue {

SchemaCheckerResult::SchemaCheckerResult():m_status(Status::SUCCESS) {
}

SchemaCheckerResult::SchemaCheckerResult(const SchemaCheckerResult& orig): m_errors(orig.m_errors), m_warnings(orig.m_warnings) {
  m_status = orig.m_status;
}

SchemaCheckerResult SchemaCheckerResult::operator=(const SchemaCheckerResult &other){
  if(this != &other){
    m_errors = other.m_errors;
    m_warnings = other.m_warnings;
    m_status = other.m_status;
  }
  return *this;
}

SchemaCheckerResult SchemaCheckerResult::operator +=(const SchemaCheckerResult& other){
  m_errors.insert(m_errors.end(),other.m_errors.begin(),other.m_errors.end());
  m_warnings.insert(m_warnings.end(),other.m_warnings.begin(), other.m_warnings.end());
  if(m_status == Status::SUCCESS){
    // The status should not change if it is failed
    m_status = other.m_status;
  }
  return *this;
}

SchemaCheckerResult::~SchemaCheckerResult() {
}

void SchemaCheckerResult::addError(const std::string& error){
  m_errors.emplace_back(error);
  m_status = Status::FAILED;
}

void SchemaCheckerResult::addWarning(const std::string& warning){
  m_warnings.emplace_back(warning);
}

SchemaCheckerResult::Status SchemaCheckerResult::getStatus() const {
  return m_status;
}

void SchemaCheckerResult::displayErrors(std::ostream & os) const {
  for(auto &error: m_errors){
    os << "  ERROR: " << error << std::endl;
  }
}

void SchemaCheckerResult::displayWarnings(std::ostream& os) const{
  for(auto &warning: m_warnings){
    os << "  WARNING: " << warning << std::endl;
  }
}


std::string SchemaCheckerResult::statusToString(const Status& status){
  switch(status){
    case Status::SUCCESS:
      return "SUCCESS";
    case Status::FAILED:
      return "FAILED";
    default:
      return "  UnknownStatus";
  }
}

}}