/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <memory>

#include "catalogue/interfaces/ArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsStorageClassCatalogue.hpp"
#include "catalogue/TimeBasedCache.hpp"
#include "common/dataStructures/TapeCopyToPoolMap.hpp"
#include "common/log/Logger.hpp"

namespace cta {

namespace common::dataStructures {
struct DeleteArchiveRequest;
struct MountPolicy;
struct RequesterIdentity;
}

namespace rdbms {
class Conn;
class ConnPool;
}

namespace catalogue {

struct ArchiveFileRowWithoutTimestamps;
struct ArchiveFileRow;

class RdbmsCatalogue;

template <typename Value>
class ValueAndTimeBasedCacheInfo;

struct User;
struct Group;

class RdbmsArchiveFileCatalogue : public ArchiveFileCatalogue {
public:
  ~RdbmsArchiveFileCatalogue() override = default;

  uint64_t checkAndGetNextArchiveFileId(const std::string &diskInstanceName, const std::string &storageClassName,
    const common::dataStructures::RequesterIdentity &user) override;

  common::dataStructures::ArchiveFileQueueCriteria getArchiveFileQueueCriteria(const std::string &diskInstanceName,
    const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override;

  ArchiveFileItor getArchiveFilesItor(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override;

  common::dataStructures::ArchiveFile getArchiveFileForDeletion(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override;

  std::list<common::dataStructures::ArchiveFile> getFilesForRepack(const std::string &vid, const uint64_t startFSeq,
    const uint64_t maxNbFiles) const override;

  ArchiveFileItor getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const override;

  common::dataStructures::ArchiveFileSummary getTapeFileSummary(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override;

  common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) const override;

  void modifyArchiveFileStorageClassId(const uint64_t archiveFileId,
    const std::string& newStorageClassName) const override;

  void modifyArchiveFileFxIdAndDiskInstance(const uint64_t archiveId, const std::string& fxId,
    const std::string &diskInstance) const override;

  void moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
    log::LogContext & lc) override;

  void updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance,
    const std::string &diskFileId) override;

protected:
  RdbmsArchiveFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue *rdbmsCatalogue);

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
  virtual uint64_t getNextArchiveFileId(rdbms::Conn &conn) = 0;

  friend class OracleFileRecycleLogCatalogue;
  friend class PostgresFileRecycleLogCatalogue;
  friend class SqliteFileRecycleLogCatalogue;
  /**
   * Returns the archive file with the specified unique identifier or nullptr if
   * it does not exist.
   *
   * Please note that an archive file with no associated tape files is
   * considered not to exist by this method.
   *
   * @param conn The database connection.
   * @param id The unique identifier of the archive file.
   * @return The archive file.
   */
  std::unique_ptr<common::dataStructures::ArchiveFile> getArchiveFileById(rdbms::Conn &conn,
    const uint64_t archiveFileId) const;

  /**
   * Copy the archiveFile and the associated tape files from the ARCHIVE_FILE and TAPE_FILE tables to the FILE_RECYCLE_LOG table
   * and deletes the ARCHIVE_FILE and TAPE_FILE entries.
   * @param conn the database connection
   * @param request the request that contains the necessary informations to identify the archiveFile to copy to the FILE_RECYCLE_LOG table
   * @param lc the log context
   */
  virtual void copyArchiveFileToFileRecyleLogAndDelete(rdbms::Conn & conn,
    const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) = 0;

  void deleteArchiveFile(rdbms::Conn& conn, const common::dataStructures::DeleteArchiveRequest& request);

protected:
  log::Logger &m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;

private:

  /**
   * Cached versions of tape copy to tape tape pool mappings for specific
   * storage classes.
   */
  mutable TimeBasedCache<catalogue::StorageClass, common::dataStructures::TapeCopyToPoolMap> m_tapeCopyToPoolCache;

  /**
   * Cached versions of the expected number of archive routes for specific
   * storage classes as specified by the call to the createStorageClass()
   * method as opposed to the actual number entered so far using the
   * createArchiveRoute() method.
   */
  mutable TimeBasedCache<catalogue::StorageClass, uint64_t> m_expectedNbArchiveRoutesCache;

  /**
   * Returns a cached version of the mapping from tape copy to tape pool for the
   * specified storage class.
   *
   * This method updates the cache when necessary.
   *
   * @param storageClass The fully qualified storage class, in other words the
   * name of the disk instance and the name of the storage class.
   * @return The mapping from tape copy to tape pool for the specified storage
   * class.
   */
  common::dataStructures::TapeCopyToPoolMap getCachedTapeCopyToPoolMap(
    const catalogue::StorageClass &storageClass) const;

  /**
   * Returns a cached version of the expected number of archive routes for the
   * specified storage class as specified by the call to the
   * createStorageClass() method as opposed to the actual number entered so far
   * using the createArchiveRoute() method.
   *
   * This method updates the cache when necessary.
   *
   * @param storageClass The fully qualified storage class, in other words the
   * name of the disk instance and the name of the storage class.
   * @return The expected number of archive routes.
   */
  uint64_t getCachedExpectedNbArchiveRoutes(const catalogue::StorageClass &storageClass) const;

  /**
   * Returns a cached version of the specified requester mount-policy or std::nullopt
   * if one does not exist.
   *
   * @param user The fully qualified user, in other words the name of the disk
   * instance and the name of the group.
   * @return The mount policy or std::nullopt if one does not exists.
   * @throw UserErrorWithTimeBasedCacheInfo if there was a user error.
   */
  ValueAndTimeBasedCacheInfo<std::optional<common::dataStructures::MountPolicy> > getCachedRequesterMountPolicy(
    const User &user) const;

  /**
   * Returns a cached version of the specified requester-group mount-policy or
   * nullptr if one does not exist.
   *
   * This method updates the cache when necessary.
   *
   * @param group The fully qualified group, in other words the name of the disk
   * instance and the name of the group.
   * @return The cached mount policy or std::nullopt if one does not exists.
   */
  ValueAndTimeBasedCacheInfo<std::optional<common::dataStructures::MountPolicy> > getCachedRequesterGroupMountPolicy(
    const Group &group) const;

  /**
   * Throws a UserError exception if the specified searchCriteria is not valid
   * due to a user error.
   *
   * @param searchCriteria The search criteria.
   */
  void checkTapeFileSearchCriteria(const TapeFileSearchCriteria &searchCriteria) const;

    /**
   * Throws a UserError exception if the specified searchCriteria is not valid
   * due to a user error.
   *
   * @param conn The database connection.
   * @param searchCriteria The search criteria.
   */
  void checkTapeFileSearchCriteria(rdbms::Conn &conn, const TapeFileSearchCriteria &searchCriteria) const;

  /**
   * Returns an iterator across the files on the specified tape ordered by
   * FSEQ.
   *
   * @param vid The volume identifier of the tape.
   * @return The iterator.
   */
  ArchiveFileItor getTapeContentsItor(const std::string &vid) const;

  /**
   * Returns the specified archive files.  Please note that the list of files
   * is ordered by archive file ID.
   *
   * @param conn The database connection.
   * @param searchCriteria The search criteria.
   * @return The archive files.
   */
  ArchiveFileItor getArchiveFilesItor(rdbms::Conn &conn, const TapeFileSearchCriteria &searchCriteria) const;

  /**
   * Returns the mapping from tape copy to tape pool for the specified storage
   * class.
   *
   * @param conn The database connection.
   * @param storageClass The fully qualified storage class, in other words the
   * name of the disk instance and the name of the storage class.
   * @return The mapping from tape copy to tape pool for the specified storage
   * class.
   */
  common::dataStructures::TapeCopyToPoolMap getTapeCopyToPoolMap(rdbms::Conn &conn,
    const catalogue::StorageClass &storageClass) const;

  /**
   * Returns the expected number of archive routes for the specified storage
   * class as specified by the call to the createStorageClass() method as
   * opposed to the actual number entered so far using the createArchiveRoute()
   * method.
   *
   * @param conn The database connection.
   * @param storageClass The fully qualified storage class, in other words the
   * name of the disk instance and the name of the storage class.
   * @return The expected number of archive routes.
   */
  uint64_t getExpectedNbArchiveRoutes(rdbms::Conn &conn, const catalogue::StorageClass &storageClass) const;

  /**
   * Throws an exception if the delete request passed in parameter is not consistent
   * to allow a deletion of the ArchiveFile from the Catalogue.
   * @param deleteRequest, the deleteRequest to check the consistency.
   * @param archiveFile the ArchiveFile to delete to check the deleteRequest consistency against.
   */
  void checkDeleteRequestConsistency(const cta::common::dataStructures::DeleteArchiveRequest deleteRequest,
    const cta::common::dataStructures::ArchiveFile & archiveFile) const;

  friend class RdbmsFileRecycleLogCatalogue;
  friend class SqliteTapeFileCatalogue;

  /**
   * An RdbmsCatalogue specific method that inserts the specified row into the
   * ArchiveFile table.
   *
   * @param conn The database connection.
   * @param row The row to be inserted.
   */
  void insertArchiveFile(rdbms::Conn &conn, const ArchiveFileRowWithoutTimestamps &row) const;

  /**
   * Returns the specified archive file row.   A nullptr pointer is returned if
   * there is no corresponding row in the ARCHIVE_FILE table.
   *
   * @param conn The database connection.
   * @param id The identifier of the archive file.
   * @return The archive file row or nullptr.
   */
  std::unique_ptr<ArchiveFileRow> getArchiveFileRowById(rdbms::Conn &conn, const uint64_t id) const;

  friend class RdbmsTapeFileCatalogue;
  /**
   * Returns the specified archive file.   A nullptr pointer is returned if
   * there are no corresponding rows in the TAPE_FILE table. Only looks at TAPE_FILE entries
   * on Tapes with state 'ACTIVE'
   *
   * @param conn The database connection.
   * @param archiveFileId The identifier of the archive file.
   * @return The archive file or nullptr.
   */
  std::unique_ptr<common::dataStructures::ArchiveFile> getArchiveFileToRetrieveByArchiveFileId(rdbms::Conn &conn,
    const uint64_t archiveFileId) const;

  /**
   * Returns the specified archive file.   A nullptr pointer is returned if
   * there are no corresponding rows in the TAPE_FILE table.
   *
   * @param conn The database connection.
   * @param archiveFileId The identifier of the archive file.
   * @return A list of tape file vid and corresponding tape state pairs
   */
  const std::list<std::pair<std::string, std::string>> getTapeFileStateListForArchiveFileId(rdbms::Conn &conn,
    const uint64_t archiveFileId) const;
};

}} // namespace cta::catalogue
