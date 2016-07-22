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
 * An SQLite implementation of the CTA catalogue.
 */
class SqliteCatalogue: public RdbmsCatalogue {
public:

  /**
   * Constructor.
   *
   * @param filename The filename to be passed to the sqlite3_open() function.
   * @param nbConns The maximum number of concurrent connections to the underyling database.
   */
  SqliteCatalogue(const std::string &filename, const uint64_t nbConns);

protected:

  /**
   * Protected constructor only to be called by sub-classes.
   *
   * Please note that it is the responsibility of the sub-class to set
   * RdbmsCatalogue::m_conn.
   */
  SqliteCatalogue();

public:

  /**
   * Destructor.
   */
  virtual ~SqliteCatalogue() override;

  /**
   * Deletes the specified archive file and its associated tape copies from the
   * catalogue.
   *
   * @param instanceName The name of the instance from where the deletion request originated
   * @param archiveFileId The unique identifier of the archive file.
   * @return The metadata of the deleted archive file including the metadata of
   * the associated and also deleted tape copies.
   */
  virtual common::dataStructures::ArchiveFile deleteArchiveFile(const std::string &diskInstanceName,
    const uint64_t archiveFileId) override;

protected:

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   */
  virtual uint64_t getNextArchiveFileId() override;

  /**
   * Selects the specified tape within th eTape table for update.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because some database technologies directly support SELECT FOR UPDATE
   * whilst others do not.
   *
   * @param vid The volume identifier of the tape.
   */
  virtual common::dataStructures::Tape selectTapeForUpdate(const std::string &vid) override;

}; // class SqliteCatalogue

} // namespace catalogue
} // namespace cta
