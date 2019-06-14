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

#pragma once

#include "CatalogueSchema.hpp"

#include <string>
#include <vector>

namespace cta {
namespace catalogue {

/**
 * Structure containing the SQL to create the schema of the CTA catalogue
 * database in an SQLite database.
 *
 * The CMakeLists.txt file of this directory instructs cmake to generate
 * MysqlCatalogueSchema.cpp from:
 *   - MysqlCatalogueSchema.before_SQL.cpp
 *   - sqlite_catalogue_schema.sql
 *
 * The MysqlCatalogueSchema.before_SQL.cpp file is not compilable and is therefore
 * difficult for Integrated Developent Environments (IDEs) to handle.
 *
 * The purpose of this class is to help IDEs by isolating the "non-compilable"
 * issues into a small cpp file.
 */
struct MysqlCatalogueSchema: public CatalogueSchema {
  /**
   * Constructor.
   */
  MysqlCatalogueSchema();

  /**
   * Return a list of trigger commands
   */
  std::vector<std::string> triggers();
};

} // namespace catalogue
} // namespace cta
