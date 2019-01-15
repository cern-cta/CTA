/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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

#include <string>

namespace cta {
namespace catalogue {

/**
 * Structure containing the SQL to create the schema of the in memory CTA
 * database.
 *
 * The CMakeLists.txt file of this directory instructs cmake to generate
 * PostgresCatalogueSchema.cpp from:
 *   - PostgresCatalogueSchema.before_SQL.cpp
 *   - postgres_catalogue_schema.sql
 *
 * The PostgresSchema.before_SQL.cpp file is not compilable and is therefore
 * difficult for Integrated Developent Environments (IDEs) to handle.
 *
 * The purpose of this class is to help IDEs by isolating the "non-compilable"
 * issues into a small cpp file.
 */
struct PostgresCatalogueSchema {
  /**
   * Constructor.
   */
  PostgresCatalogueSchema();

  /**
   * The schema.
   */
  const std::string sql;
};

} // namespace catalogue
} // namespace cta
