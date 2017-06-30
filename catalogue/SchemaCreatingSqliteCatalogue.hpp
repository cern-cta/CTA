/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
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

#include "catalogue/SqliteCatalogue.hpp"

namespace cta {
namespace catalogue {

class CatalogueFactory;

/**
 * Asbtract CTA catalogue class to be used for unit testing.
 */
class SchemaCreatingSqliteCatalogue: public SqliteCatalogue {
public:

  /**
   * Constructor.
   *
   * @param filename The filename to be passed to the sqlite3_open() function.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   */
  SchemaCreatingSqliteCatalogue(const std::string &filename, const uint64_t nbConns,
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

}; // class SchemaCreatingSqliteCatalogue

} // namespace catalogue
} // namespace cta
