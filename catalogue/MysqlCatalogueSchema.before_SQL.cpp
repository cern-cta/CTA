/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#include "catalogue/MysqlCatalogueSchema.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MysqlCatalogueSchema::MysqlCatalogueSchema(): CatalogueSchema(
  // CTA_SQL_SCHEMA - The contents of sqlite_catalogue_schema.cpp go here
  , 
  // CTA_SQL_TRIGGER - The contents of mysql_catalogue_schema_trigger.cpp go here
  ) {
}

std::vector<std::string>
MysqlCatalogueSchema::triggers() {
  std::vector<std::string> results;
  // assume each trigger command starts with "CREATE TRIGGER"
  const std::string trigger_prefix = "CREATE TRIGGER";

  const std::string& sql_str = sql_trigger;

  size_t idx = sql_str.find(trigger_prefix);

  while (idx != std::string::npos) {
    size_t idx_next = sql_str.find(trigger_prefix, idx+1);

    size_t len = 0;
    if (idx_next != std::string::npos) {
      len = idx_next - idx;
    } else {
      len = idx_next;
    }
    std::string s = sql_str.substr(idx, len);
    results.push_back(s);

    idx = idx_next;
  }
  return results;
}

} // namespace catalogue
} // namespace cta
