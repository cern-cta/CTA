/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/CatalogueFactory.hpp"
#include "common/log/Logger.hpp"

namespace cta::catalogue {

/**
 * Factory of Catalogue objects.
 */
class InMemoryCatalogueFactory: public CatalogueFactory {
public:

  /**
   * Constructor
   *
   * @param log Object representing the API to the CTA logging system.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   * @param maxTriesToConnext The maximum number of times a single method should
   * try to connect to the database in the event of LostDatabaseConnection
   * exceptions being thrown.
   */
  InMemoryCatalogueFactory(
    log::Logger &log,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns,
    const uint32_t maxTriesToConnect);

  /**
   * Returns a newly created CTA catalogue object.
   */
  std::unique_ptr<Catalogue> create() override;

private:

  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger &m_log;

  /**
   * The maximum number of concurrent connections to the underlying relational
   * database for all operations accept listing archive files which can be
   * relatively long operations.
   */
  uint64_t m_nbConns;

  /**
   * The maximum number of concurrent connections to the underlying relational
   * database for the sole purpose of listing archive files.
   */
  uint64_t m_nbArchiveFileListingConns;

  /**
   * The maximum number of times a single method should try to connect to the
   * database in the event of LostDatabaseConnection
   * exceptions being thrown.
   */
  uint32_t m_maxTriesToConnect;

}; // class InMemoryCatalogueFactory

} // namespace cta::catalogue
