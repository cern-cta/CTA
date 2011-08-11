/******************************************************************************
 *                castor/tape/tapebridge/PendingMigrationsStore.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSSTORE_HPP
#define CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSSTORE_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"

#include <list>
#include <map>
#include <stdint.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * The store of pending file-migrations.  A pending file-migration is one
 * that has either just been sent to the rtcpd daemon or has been written to
 * tape without a flush.
 */
class PendingMigrationsStore {
public:
  /**
   * Constructor.
   */
  PendingMigrationsStore();

  /**
   * Adds the specified pending file-migration to the store.
   *
   * This method should be called immediately after receiving the positive
   * acknowledgement from the rtcpd daemon that the specified pending
   * file-migration request has been successfully submitted to the rtcpd
   * daemon.
   *
   * This method throws an exception if a file with the same tape-file
   * sequence-number has already been added to the store.
   *
   * This method throws an exception if the tape-file sequence-number of the
   * file to be added is not 1 plus the tape-file sequence-number of the
   * previously added file.  This test is of course not applicable when the
   * pending migrations store is empty and the very first file is being added.
   *
   * @param fileToMigrate The pending file-migration to be added to the store.
   */
  void add(const tapegateway::FileToMigrate &fileToMigrate)
    throw(castor::exception::Exception);

  /**
   * Marks the pending file-migration identified by the specified file-migrated
   * notification as being successfully written but not yet flushed to tape.
   *
   * This method throws an exception if the specified pending file-migration
   * cannot be found in the pending file-migration store.
   *
   * his method throws an exception if at least one file has already been
   * marked as being written but not yet flushed to tape and the tape-file
   * sequence-number of the specified file to be marked is not 1 plus that of
   * the previously marked file.
   *
   * This method throws an exception if any of the fields common to both the
   * file to migrate request and the file-migrated notification do not match
   * exactly.
   *
   * @param fileMigratedNotification The file-migrated notification to be held
   *                                 back from the client (tapegatewayd daemon
   *                                 or writetp) until the rtcpd daemon has
   *                                 notified the tapebridged daemon that the
   *                                 file has been flushed to tape.
   */
  void markAsWrittenWithoutFlush(
    const tapegateway::FileMigratedNotification &fileMigratedNotification)
    throw(castor::exception::Exception);

  /**
   * Gets and removes the pending file-migrations that have a tape-file
   * sequence-number less than or equal to the specified maximum.
   *
   * The pending file-migrations are returned as a list of file-migrated
   * notification messages that require their aggregatorTransactionId members
   * to be set before being sent to the tapegatewayd daemon.  The list in
   * ascending order of tape-file sequence-number.
   *
   * This method will throw an exception if there is at least one pending
   * file-migration with a tape-file sequence-number less than or equal to
   * the specified maximum that has not been marked as being written without a
   * flush.
   */
  std::list<tapegateway::FileMigratedNotification>
    getAndRemoveFilesWrittenWithoutFlush(const uint32_t maxFseq)
    throw(castor::exception::Exception);

  /**
   * Retruns the total number of pending file-grations currently inside the
   * store.
   */
  uint32_t getNbPendingMigrations() const;

private:

  /**
   * Data structure used to store a pending migration and its status.
   */
  struct PendingMigration {
    /**
     * The possible states of a pending file-migration whilst it is within the
     * pending migration store.
     */
    enum PendingMigrationStatus {
      /**
       * The request to carry out the pending file-migration has been sent to
       * the rtcpd daemon but not data has yet been written to tape.
       */
      PENDINGMIGRATION_SENT_TO_RTCPD,

      /**
       * The contents of the file of the pending file-migration has been
       * written to tape but has not yet been flushed.
       */
      PENDINGMIGRATION_WRITTEN_WITHOUT_FLUSH
    };

    /**
     * Constructor.
     */
    PendingMigration();

    /**
     * Constructor.
     *
     * @param s The status of the pending file-migration.
     * @param f The pending file-migration.
     */
    PendingMigration(
      const PendingMigrationStatus s,
      const tapegateway::FileToMigrate &f);

    /**
     * The status of the file whilst within the pending migration store.
     */
    PendingMigrationStatus status;

    /**
     * The pending file to be migrated from disk to tape.
     *
     * This field is set when the pending file-migration has been successfully
     * sent to the rtcpd daemon.
     */
    tapegateway::FileToMigrate fileToMigrate;

    /**
     * The pending file-migrated notification.
     *
     * This field is set when the pending file-magration has been written but
     * not yet flushed to tape.  The contsnts of this filed will be sent to the
     * tapegatewayd daemon at the later stage when rtcpd has notified the
     * tapebridged daemon that the file has been flushed to tape.
     */
    tapegateway::FileMigratedNotification fileMigratedNotification;

  }; // struct PendingMigration

  /**
   * Map datatype used to store pending file-migrations indexed by tape-file
   * sequence-number.
   */
  typedef std::map<uint32_t, PendingMigration> PendingMigrationMap;

  /**
   * The pending migrations stored in a map indexed by the tape-file
   * sequence-number.
   */
  PendingMigrationMap m_pendingMigrations;

  /**
   * The tape-file sequence-number of the last pending file-migration added to
   * the store, or 0 if the store is currently empty.
   */
  uint32_t m_tapeFseqOfLastFileAdded;

  /**
   * The tape-file sequence-number of the last pending file-migration to be
   * marked as written without a flush, or 0 if no pending file-migration
   * has yet been marked as such.
   */
  uint32_t m_tapeFseqOfLastFileWrittenWithoutFlush;

  /**
   * Private copy-constructor to prevent copies being made.
   */
  PendingMigrationsStore(PendingMigrationsStore &);

  /**
   * Private assignment operator to prevent assignments.
   */
  PendingMigrationsStore &operator=(const PendingMigrationsStore&);

  /**
   * This method throws an exception if one or more the fields common to the
   * specified file to migrate request and file-migrated notification do not
   * match exactly.
   *
   * @param fileToMigrate            The request to migrate the file.
   * @param fileMigratedNotification The notification that the file has been
   *                                 migrated.
   */
  void checkForMismatches(const tapegateway::FileToMigrate &fileToMigrate,
    const tapegateway::FileMigratedNotification &fileMigratedNotification)
    const throw(castor::exception::Exception);

  /**
   * Clears all of the pending migrations from this store.
   */
  void clear();

}; // class PendingMigrationsStore

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSSTORE_HPP
