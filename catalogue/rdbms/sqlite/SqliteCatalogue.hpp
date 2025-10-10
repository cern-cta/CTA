/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/rdbms/RdbmsCatalogue.hpp"

namespace cta::catalogue {

/**
 * An SQLite implementation of the CTA catalogue.
 */
class SqliteCatalogue: public RdbmsCatalogue {
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
  SqliteCatalogue(
    log::Logger &log,
    const std::string &filename,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns);

public:
  /**
   * Destructor.
   */
  ~SqliteCatalogue() override = default;

protected:
  /**
   * Creates a temporary table from the list of disk file IDs provided in the search criteria.
   *
   * @param conn The database connection.
   * @param diskFileIds List of disk file IDs (fxid).
   * @return Name of the temporary table
   */
  std::string createAndPopulateTempTableFxid(rdbms::Conn &conn,
    const std::optional<std::vector<std::string>> &diskFileIds) const override;
}; // class SqliteCatalogue

} // namespace cta::catalogue
