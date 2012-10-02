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
#include "castor/tape/tapebridge/FileWrittenNotificationList.hpp"
#include "castor/tape/tapebridge/RequestToMigrateFile.hpp"
#include "castor/tape/tapebridge/TapeFlushConfigParams.hpp"

#include <list>
#include <map>
#include <stdint.h>
#include <string>

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
   *
   * @param tapeFlushConfigParams The tape flush configuration parameters.
   */
  PendingMigrationsStore(const TapeFlushConfigParams &tapeFlushConfigParams);

  /**
   * Adds the specified file-migration request to the store.
   *
   * This method throws an exception if a file with the same tape-file
   * sequence-number has already been added to the store.
   *
   * This method throws an exception if the tape-file sequence-number of the
   * file to be added is not 1 plus the tape-file sequence-number of the
   * previously added file.  This test is of course not applicable when the
   * pending migrations store is empty and the very first file is being added.
   *
   * @param request The request to migrate a file to tape.
   */
  void receivedRequestToMigrateFile(const RequestToMigrateFile &request)
    throw(castor::exception::Exception);

  /**
   * Marks the pending file-migration identified by the specified file-migrated
   * notification as being successfully written but not yet flushed to tape.
   *
   * This method throws an exception if the specified pending file-migration
   * cannot be found in the pending file-migration store.
   *
   * This method throws an exception if at least one file has already been
   * marked as being written but not yet flushed to tape and the tape-file
   * sequence-number of the specified file to be marked is not 1 plus that of
   * the previously marked file.
   *
   * This method throws an exception if any of the fields common to both the
   * file to migrate request and the file-migrated notification do not match
   * exactly.
   *
   * @param notification The file-migrated notification to be held back from
   *                     the client (tapegatewayd daemon or writetp) until the
   *                     rtcpd daemon has notified the tapebridged daemon that
   *                     the file has been flushed to tape.
   */
  void fileWrittenWithoutFlush(const FileWrittenNotification &notification)
    throw(castor::exception::Exception);

  /**
   * To be called when there are no more files to be migrated.
   *
   * The pending-migrations store uses this notification to determine the
   * tape-file sequence-number of the very last file that is going to be
   * written to tape.  The pending-migrations store uses this knowledge to
   * check the validity of the tape-file sequence-number of flushes to tape.
   */
  void noMoreFilesToMigrate() throw(castor::exception::Exception);

  /**
   * Removes the pending file-migrations that have a tape-file sequence-number
   * less than or equal to the specified tape-file sequence-number of the
   * flush to tape.
   *
   * The pending file-migrations are returned as a list of "file written to
   * tape" notifications.  The list is in ascending order of tape-file
   * sequence-number.
   *
   * This method will throw an exception if there is at least one pending
   * file-migration with a tape-file sequence-number less than or equal to
   * the specified maximum that has not been marked as being written without a
   * flush.
   */
  FileWrittenNotificationList dataFlushedToTape(
    const int32_t tapeFSeqOfFlush) throw(castor::exception::Exception);

  /**
   * Returns the total number of pending file-migrations currently inside the
   * store.
   */
  uint32_t getNbPendingMigrations() const;

private:

  /**
   * The tape flush configuration parameters.
   */
  const TapeFlushConfigParams m_tapeFlushConfigParams;

  /**
   * The current number of bytes written to tape without a flush to tape.
   */
  uint64_t m_nbBytesWrittenWithoutFlush;

  /**
   * The current number of files written to tape without a flush to tape.
   */
  uint64_t m_nbFilesWrittenWithoutFlush;

  /**
   * The tape-file sequence-number of the very last file that will be written
   * to tape at the end of the current tape session.  If the file is not yet
   * known then the value of this member-variable will be 0.
   */
  int32_t m_tapeFSeqOfEndOfSessionFile;

  /**
   * The tape-file sequence-number of the last pending file-migration to be
   * marked as written without a flush, or 0 if no pending file-migration
   * has yet been marked as such.
   */
  int32_t m_tapeFSeqOfLastFileWrittenWithoutFlush;

  /**
   * The tape-file sequence-number of the last pending file-migration added to
   * the store, or 0 if the store is currently empty.
   */
  int32_t m_tapeFSeqOfLastFileAdded;

  /**
   * The sequence-number of the file written to tape that matched or
   * immediately exceeded the maximum number of bytes to be written to tape
   * before a flush.  If the file is not known then the value of this
   * member-variable is 0.
   */
  int32_t m_tapeFSeqOfMaxBytesFile;

  /**
   * The sequence-number of the file written to tape that matched the maximum
   * number of files to be written to tape before a flush.  If the file is not
   * known then the value of this member-variable is 0.
   */
  int32_t m_tapeFSeqOfMaxFilesFile;

  /**
   * Tracks the migration of a file to tape.
   */
  class Migration {
  public:
    /**
     * The possible states of a file-migration whilst it is within the pending
     * migration store.
     */
    enum Status {
      /**
       * Status is not yet known.
       */
      UNKNOWN,

      /**
       * The request to carry out the pending file-migration has been sent to
       * the rtcpd daemon but not data has yet been written to tape.
       */
      SENT_TO_RTCPD,

      /**
       * The contents of the file of the pending file-migration has been
       * written to tape but has not yet been flushed.
       */
      WRITTEN_WITHOUT_FLUSH
    };

    /**
     * Constructor.
     */
    Migration(): m_status(UNKNOWN) {
      // Do nothing
    }

    /**
     * Constructor.
     *
     * @param status  The status of the file-migration.
     * @param request The request to migrate the file to tape.
     */
    Migration(const Status status, const RequestToMigrateFile &request):
      m_status(status),
      m_request(request) {
      // Do nothing
    }

    /**
     * Sets the status of the file-migration.
     */
    void setStatus(const Status status) {
      m_status = status;
    }

    /**
     * Returns the status of the file-migration.
     */
    Status getStatus() const {
      return m_status;
    }

    /**
     * Returns the request to migrate the file to tape.
     */
    const RequestToMigrateFile &getRequest() const {
      return m_request;
    }

    /**
     * Sets the "file written to tape" notification.
     */
    void setFileWrittenNotification(const FileWrittenNotification
      &notification) {
      m_fileWrittenNotification = notification;
    }

    /**
     * Gets the "file written to tape" notification.
     */
    const FileWrittenNotification &getFileWrittenNotification() const {
      return m_fileWrittenNotification;
    }

  private:

    /**
     * The status of the file-migration whilst within the store.
     */
    Status m_status;

    /**
     * The request to migrate the file to tape.
     */
    RequestToMigrateFile m_request;

    /**
     * The "file written to tape" notification.
     *
     * This field is set when the pending file-migration has been written but
     * not yet flushed to tape.  The contents of this field will be sent to
     * the tapegatewayd daemon when the rtcpd daemon has notified the
     * tapebridged daemon that the file has been flushed to tape.
     */
    FileWrittenNotification m_fileWrittenNotification;

  }; // struct Migration

  /**
   * Map of file-migrations indexed by tape-file sequence-number.
   */
  typedef std::map<uint32_t, Migration> MigrationMap;

  /**
   * The file-migrations in the store indexed by tape-file sequence-number.
   */
  MigrationMap m_migrations;

  /**
   * Private copy-constructor to prevent copies being made.
   * Not implemented so that it cannot be called
   */
  PendingMigrationsStore(PendingMigrationsStore &);

  /**
   * Private assignment operator to prevent assignments.
   * Not implemented so that it cannot be called
   */
  PendingMigrationsStore &operator=(const PendingMigrationsStore&);

  /**
   * This method throws an exception if one or more the fields common to the
   * specified migrate-file request and file-migrated notification do not
   * match exactly.
   *
   * @param request      The request to migrate the file to tape.
   * @param notification The notification that the file has been migrated to
   *                     tape.
   */
  void checkForMismatches(const RequestToMigrateFile &request,
    const FileWrittenNotification &notification)
    const throw(castor::exception::Exception);

  /**
   * Clears all of the pending migrations from this store.
   */
  void clear();

  /**
   * Returns the maximum number of bytes before the tape drive buffer should
   * be flushed.
   */
  uint64_t getMaxBytesBeforeFlush() const throw(castor::exception::Exception);

  /**
   * Returns the maximum number of files before the tape drive buffer should
   * be flushed.
   */
  uint64_t getMaxFilesBeforeFlush() const throw(castor::exception::Exception);

}; // class PendingMigrationsStore

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSSTORE_HPP
