/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "catalogue/SchemaCreatingSqliteCatalogue.hpp"

namespace cta {
namespace catalogue {

class CatalogueFactory;

/**
 * CTA catalogue class to be used for unit testing.
 */
class InMemoryCatalogue: public SchemaCreatingSqliteCatalogue {
public:

  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   */
  InMemoryCatalogue(
    log::Logger &log,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns);

  /**
   * Destructor.
   */
  virtual ~InMemoryCatalogue() override;

}; // class InMemoryCatalogue

} // namespace catalogue
} // namespace cta
