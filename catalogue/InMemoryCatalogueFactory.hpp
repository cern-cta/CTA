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

#include "catalogue/CatalogueFactory.hpp"
#include "common/log/Logger.hpp"

namespace cta {
namespace catalogue {

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

} // namespace catalogue
} // namespace cta
