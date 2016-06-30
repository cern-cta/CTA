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
 * An Oracle based implementation of the CTA catalogue.
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
   * Deletes the specified archive file and its associated tape copies from the
   * catalogue.
   *
   * @param archiveFileId The unique identifier of the archive file.
   * @return The metadata of the deleted archive file including the metadata of
   * the associated and also deleted tape copies.
   */
  virtual common::dataStructures::ArchiveFile deleteArchiveFile(const uint64_t archiveFileId);

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   */
  virtual uint64_t getNextArchiveFileId();

  /**
   * Selects the specified tape within the Tape table for update.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because some database technologies directly support SELECT FOR UPDATE
   * whilst others do not.
   *
   * @param vid The volume identifier of the tape.
   */
  virtual common::dataStructures::Tape selectTapeForUpdate(const std::string &vid);

}; // class OracleCatalogue

} // namespace catalogue
} // namespace cta
