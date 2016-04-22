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

#include <string>

namespace cta {
namespace catalogue {

/**
 * Structure containing the SQL to generate the database schema of the CTA
 * catalogue.
 *
 * The CMakeLists.txt file of this directory instructs cmake to generate the
 * implementation file of this class, SqliteCatalogueSchema.cpp, by combining
 * the contents of the SqliteCatalogueSchema.before_SQL.cpp with the
 * contents of the master schema file, catalogue_schema.sql.  This means the
 * SqliteCatalogueSchema.before_SQL.cpp file is not compilable.
 *
 * The purpose of this class is to isolate the "non-compilable" issues into a
 * a small and encapsulated compilation unit, namely SqliteCatalogueSchema.o, so
 * that the remaining business logic can be implemented in the non-generated
 * and compilable SqliteCatalogue.cpp file.  This means that IDEs can work as
 * normal with the bulk of the code in SqliteCatalogue.cpp, whereas they will
 * struggle with SqliteCatalogueSchema.before_SQL.cpp which is therefore
 * intentionally small.
 */
struct SqliteCatalogueSchema {
  /**
   * Constructor.
   */
  SqliteCatalogueSchema();

  std::string sql;
};

} // namespace catalogue
} // namespace cta
