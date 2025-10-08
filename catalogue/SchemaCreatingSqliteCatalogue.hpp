/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

private:
  /**
   * Creates the database schema.
   */
  void createCatalogueSchema() const;

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
  void executeNonQueries(rdbms::Conn &conn, const std::string &sqlStmts) const;

}; // class SchemaCreatingSqliteCatalogue

} // namespace cta::catalogue
