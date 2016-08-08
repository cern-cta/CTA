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
public:

  /**
   * Constructor.
   *
   * @param username The database username.
   * @param password The database password.
   * @param database The database name.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database.
   */
  OracleCatalogue(
    const std::string &username,
    const std::string &password,
    const std::string &database,
    const uint64_t nbConns);

  /**
   * Destructor.
   */
  ~OracleCatalogue() override;

  /**
   * Deletes the specified archive file and its associated tape copies from the
   * catalogue.
   *
   * Please note that the name of the disk instance is specified in order to
   * prevent a disk instance deleting an archive file that belongs to another
   * disk instance.
   *
   * @param instanceName The name of the instance from where the deletion request originated
   * @param archiveFileId The unique identifier of the archive file.
   * @return The metadata of the deleted archive file including the metadata of
   * the associated and also deleted tape copies.
   */
  common::dataStructures::ArchiveFile deleteArchiveFile(const std::string &diskInstanceName,
    const uint64_t archiveFileId) override;

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return A unique archive ID that can be used by a new archive file within
   * the catalogue.
   */
  uint64_t getNextArchiveFileId(rdbms::Conn &conn) override;

  /**
   * Selects the specified tape within the Tape table for update.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because some database technologies directly support SELECT FOR UPDATE
   * whilst others do not.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   */
  common::dataStructures::Tape selectTapeForUpdate(rdbms::Conn &conn, const std::string &vid) override;

}; // class OracleCatalogue

} // namespace catalogue
} // namespace cta
