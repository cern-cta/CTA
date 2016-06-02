/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
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

#include "catalogue/RdbmsCatalogue.hpp"

namespace cta {
namespace catalogue {

/**
 * CTA catalogue class to be used for unit testing.
 */
class InMemoryCatalogue: public RdbmsCatalogue {
public:

  /**
   * Constructor.
   *
   * @param conn The connection to the underlying relational database.  Please
   * note that the InMemoryCatalogue will own and therefore delete the
   * specified database connection.
   */
  InMemoryCatalogue();

  /**
   * Destructor.
   */
  virtual ~InMemoryCatalogue();

protected:

  /**
   * This is an InMemoryCatalogue specific method that creates the catalogue
   * database schema.
   */
  void createCatalogueSchema();

  /**
   * This is an InMemoryCatalogue specific method that executes the specified
   * non-query multi-line SQL statement.
   *
   * Please note that each statement must end with a semicolon.  If the last
   * statement is missing a semicolon then it will not be executed.
   *
   * @param multiStmt Non-query multi-line SQL statement.
   */
  void executeNonQueryMultiStmt(const std::string &multiStmt);

}; // class InMemoryCatalogue

} // namespace catalogue
} // namespace cta
