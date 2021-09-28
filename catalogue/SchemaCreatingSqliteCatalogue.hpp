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

#pragma once

#include "catalogue/SqliteCatalogue.hpp"

namespace cta {
namespace catalogue {

class CatalogueFactory;

/**
 * CTA catalogue class to be used for unit testing.
 */
class SchemaCreatingSqliteCatalogue: public SqliteCatalogue {
public:

  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param filename The filename to be passed to the sqlite3_open() function.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   */
  SchemaCreatingSqliteCatalogue(
    log::Logger &log,
    const std::string &filename,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns);

  /**
   * Destructor.
   */
  virtual ~SchemaCreatingSqliteCatalogue() override;

private:

  /**
   * Creates the database schema.
   */
  void createCatalogueSchema();

  /**
   * Parses the specified string of multiple SQL statements separated by
   * semicolons and calls executeNonQuery() for each statement found.
   *  
   * Please note that this method does not support statements that themselves
   * contain one more semicolons.
   *
   * @param conn The database connection.
   * @param sqlStmts Multiple SQL statements separated by semicolons.
   * Statements that themselves contain one more semicolons are not supported.
   */
  void executeNonQueries(rdbms::Conn &conn, const std::string &sqlStmts);

}; // class SchemaCreatingSqliteCatalogue

} // namespace catalogue
} // namespace cta
