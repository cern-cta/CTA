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

class CatalogueFactory;

/**
 * CTA catalogue class to be used for unit testing.
 */
class InMemoryCatalogue: public RdbmsCatalogue {

  /**
   * The CatalogueFactory is a friend so that it can call the private
   * constructor of this class.
   */
  friend CatalogueFactory;

private:

  /**
   * Private constructor only to be called by the CatalogueFactory class (a
   * friend).
   *
   * @param conn The connection to the underlying relational database.  Please
   * note that the InMemoryCatalogue will own and therefore delete the
   * specified database connection.
   */
  InMemoryCatalogue();

public:

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

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   */
  virtual uint64_t getNextArchiveFileId();

private:

  /**
   * Creates the ARCHIVE_FILE_ID table that will be used to generate ever
   * incrementing identifiers for archive files.
   */
  void createArchiveFileIdTable();

}; // class InMemoryCatalogue

} // namespace catalogue
} // namespace cta
