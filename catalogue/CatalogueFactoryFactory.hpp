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

#include <memory>

namespace cta {

namespace log {
class Logger;
}

namespace rdbms {
class Login;
}

namespace catalogue {

class CatalogueFactory;

/**
 * Factory of catalogue factories.  This class is a singleton.
 */
class CatalogueFactoryFactory {
public:
  /**
   * Prevent objects of this class from being instantiated.
   */
  CatalogueFactoryFactory() = delete;

  /**
   * Creates a factory of CTA catalogue objects using the specified database
   * login details.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param login The database connection details.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   * @return The newly created CTA catalogue object.  Please note that it is the
   * responsibility of the caller to delete the returned CTA catalogue object.
   * @param maxTriesToConnext The maximum number of times a single method should
   * try to connect to the database in the event of LostDatabaseConnection
   * exceptions being thrown.
   */
  static std::unique_ptr<CatalogueFactory> create(
    log::Logger &log,
    const rdbms::Login &login,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns,
    const uint32_t maxTriesToConnect = 3);
};  // class CatalogueFactoryFactory

}  // namespace catalogue
}  // namespace cta
