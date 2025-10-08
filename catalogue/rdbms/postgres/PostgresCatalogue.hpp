/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "rdbms/Conn.hpp"

namespace cta::catalogue {

/**
 * An Postgres based implementation of the CTA catalogue.
 */
class PostgresCatalogue: public RdbmsCatalogue {
public:
  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param login The database login details.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   */
  PostgresCatalogue(
    log::Logger &log,
    const rdbms::Login &login,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns);

  /**
   * Destructor.
   */
  ~PostgresCatalogue() override = default;

  /**
   * Creates a temporary table from the list of disk file IDs provided in the search criteria.
   *
   * @param conn The database connection.
   * @param diskFileIds List of disk file IDs (fxid).
   * @return Name of the temporary table
   */
  std::string createAndPopulateTempTableFxid(rdbms::Conn &conn,
    const std::optional<std::vector<std::string>> &diskFileIds) const override;
}; // class PostgresCatalogue

} // namespace cta::catalogue
