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

#include "catalogue/RdbmsCatalogue.hpp"

namespace cta {
namespace catalogue {

class CatalogueFactory;

/**
 * CTA catalogue class to be used for unit testing.
 */
class OracleCatalogue: public RdbmsCatalogue {

  /**
   * The CatalogueFactory is a friend so that it can call the private
   * constructor of this class.
   */
  friend CatalogueFactory;

private:

  /**
   * Private constructor only to be called by the CatalogueFactory class (a
   * friend).
   *
   * @param username The database username.
   * @param password The database password.
   * @param database The database name.
   */
  OracleCatalogue(
    const std::string &username,
    const std::string &password,
    const std::string &database);

public:

  /**
   * Destructor.
   */
  virtual ~OracleCatalogue();

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   */
  virtual uint64_t getNextArchiveFileId();

}; // class OracleCatalogue

} // namespace catalogue
} // namespace cta
