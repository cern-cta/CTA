/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <iostream>

#include "SchemaComparerResult.hpp"

namespace cta {
namespace catalogue {

SchemaComparerResult::SchemaComparerResult():m_status(Status::SUCCESS) {
}

SchemaComparerResult::SchemaComparerResult(const SchemaComparerResult& orig) {
  m_diffs = orig.m_diffs;
  m_status = orig.m_status;
}

SchemaComparerResult SchemaComparerResult::operator=(const SchemaComparerResult &other){
  if(this != &other){
    m_diffs = other.m_diffs;
    m_status = other.m_status;
  }
  return *this;
}

SchemaComparerResult SchemaComparerResult::operator +=(const SchemaComparerResult& other){
  m_diffs.insert(m_diffs.end(),other.m_diffs.begin(),other.m_diffs.end());
  if(m_status == Status::SUCCESS){
    // The status should not change if it is failed
    m_status = other.m_status;
  }
  return *this;
}

SchemaComparerResult::~SchemaComparerResult() {
}

void SchemaComparerResult::addDiff(const std::string& diff){
  m_diffs.emplace_back(diff);
  m_status = Status::FAILED;
}

SchemaComparerResult::Status SchemaComparerResult::getStatus() const {
  return m_status;
}

void SchemaComparerResult::printDiffs() const {
  for(auto &diff: m_diffs){
    std::cout << diff << std::endl;
  }
}

std::string SchemaComparerResult::StatusToString(const Status& status){
  switch(status){
    case Status::SUCCESS:
      return "SUCCESS";
    case Status::FAILED:
      return "FAILED";
    default:
      return "UnknownStatus";
  }
}

}}