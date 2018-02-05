/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
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

#include "catalogue/Catalogue.hpp"
#include "rdbms/Login.hpp"

#include <memory>
#include <mutex>

namespace cta {
namespace catalogue {

/**
 * Factory for creating CTA catalogue objects.  This class is a singleton.
 */
class CatalogueFactory {
public:

  /**
   * Prevent objects of this class from being instantiated.
   */
  CatalogueFactory() = delete;

  /**
   * Creates a CTA catalogue object using the specified database login details.
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
  static std::unique_ptr<Catalogue> create(
    log::Logger &log,
    const rdbms::Login &login,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns,
    const uint32_t maxTriesToConnect = 3);

}; // class CatalogueFactory

} // namespace catalogue
} // namespace cta
