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
   * @param log Object representing the API to the CTA logging system.
   * @param filename The filename to be passed to the sqlite3_open() function.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   */
  SqliteCatalogue(
    log::Logger &log,
    const std::string &filename,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns);

public:

  /**
   * Destructor.
   */
  ~SqliteCatalogue() override;

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
   * @throw ArchiveFileDoesNotExistInCatalogue If the specified archive file
   * does not exist in the catalogue.
   */
  common::dataStructures::ArchiveFile deleteArchiveFile(const std::string &diskInstanceName,
    const uint64_t archiveFileId) override;

protected:

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * PLEASE NOTE the SQLite implemenation of getNextArchiveFileId() takes a lock
   * on m_mutex in order to serialize access to the SQLite database.  This has
   * been done in an attempt to avoid SQLite busy errors.
   *
   * @param conn The database connection.
   * @return A unique archive ID that can be used by a new archive file within
   * the catalogue.
   */
  uint64_t getNextArchiveFileId(rdbms::PooledConn &conn) override;

  /**
   * Notifies the catalogue that the specified files have been written to tape.
   *
   * @param events The tape file written events.
   */
  void filesWrittenToTape(const std::set<TapeFileWritten> &events) override;

private:

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param autocommitMode The autocommit mode of the statement.
   * @param conn The database connection.
   * @param event The tape file written event.
   */
  void fileWrittenToTape(const rdbms::Stmt::AutocommitMode autocommitMode, rdbms::PooledConn &conn,
    const TapeFileWritten &event);

  /**
   * Selects the specified tape within the Tape table.
   *
   * @param autocommitMode The autocommit mode of the statement.
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   */
  common::dataStructures::Tape selectTape(const rdbms::Stmt::AutocommitMode autocommitMode, rdbms::PooledConn &conn,
    const std::string &vid);

}; // class SqliteCatalogue

} // namespace catalogue
} // namespace cta
