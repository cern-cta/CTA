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

#pragma once

#include <list>
#include <string>

namespace cta {
namespace catalogue {
class SchemaComparerResult {
public:
  enum Status {
    SUCCESS,
    FAILED
  };
  static std::string StatusToString(const Status& status);
  
  SchemaComparerResult();
  SchemaComparerResult(const SchemaComparerResult& orig);
  SchemaComparerResult operator=(const SchemaComparerResult &other);
  SchemaComparerResult operator+=(const SchemaComparerResult &other);
  void printDiffs() const;
  Status getStatus() const;
  void addDiff(const std::string &diff);
  virtual ~SchemaComparerResult();
private:
  std::list<std::string> m_diffs;
  Status m_status;
};

}}
