/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "catalogue/rdbms/sqlite/SqliteCatalogue.hpp"

namespace cta::catalogue {

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

} // namespace cta::catalogue
