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
#include "rdbms/Login.hpp"

namespace cta {
namespace catalogue {

class CatalogueFactory;

/**
 * A MySQL implementation of the CTA catalogue.
 */
class MysqlCatalogue: public RdbmsCatalogue {
public:

  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param login Login object to connect MySQL server.
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
  MysqlCatalogue(
    log::Logger &log,
    const rdbms::Login &login,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns,
    const uint32_t maxTriesToConnect);

  /**
   * Destructor.
   */
  ~MysqlCatalogue() override;

protected:

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solutions to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return A unique archive ID that can be used by a new archive file within
   * the catalogue.
   */
  uint64_t getNextArchiveFileId(rdbms::Conn &conn) override;

  /**
   * Returns a unique storage class ID that can be used by a new storage class
   * within the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique archive ID that can be used by a new archive file within
   * within the catalogue.
   */
  uint64_t getNextStorageClassId(rdbms::Conn &conn) override;

  /**
   * Notifies the catalogue that the specified files have been written to tape.
   *
   * @param events The tape file written events.
   */
  void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &events) override;

private:

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param autocommitMode The autocommit mode of the statement.
   * @param conn The database connection.
   * @param event The tape file written event.
   */
  void fileWrittenToTape(const rdbms::AutocommitMode autocommitMode, rdbms::Conn &conn,
    const TapeFileWritten &event);

  /**
   * Selects the specified tape within the Tape table for update.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   */
  common::dataStructures::Tape selectTapeForUpdate(rdbms::Conn &conn, const std::string &vid);
}; // class MysqlCatalogue

} // namespace catalogue
} // namespace cta
