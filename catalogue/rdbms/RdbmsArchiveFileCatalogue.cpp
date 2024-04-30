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

#include <string>

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/ArchiveFileRowWithoutTimestamps.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/Group.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueGetArchiveFilesForRepackItor.hpp"
#include "catalogue/rdbms/RdbmsCatalogueGetArchiveFilesItor.hpp"
#include "catalogue/rdbms/RdbmsCatalogueTapeContentsItor.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsMountPolicyCatalogue.hpp"
#include "catalogue/rdbms/RdbmsStorageClassCatalogue.hpp"
#include "catalogue/User.hpp"
#include "catalogue/ValueAndTimeBasedCacheInfo.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/UserError.hpp"
#include "common/exception/UserErrorWithCacheInfo.hpp"
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsArchiveFileCatalogue::RdbmsArchiveFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log),
    m_connPool(connPool),
    m_rdbmsCatalogue(rdbmsCatalogue),
    m_tapeCopyToPoolCache(10),
    m_expectedNbArchiveRoutesCache(10) {}

uint64_t RdbmsArchiveFileCatalogue::checkAndGetNextArchiveFileId(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) {
  try {
    const auto storageClass = catalogue::StorageClass(storageClassName);
    const auto copyToPoolMap = getCachedTapeCopyToPoolMap(storageClass);
    const auto expectedNbRoutes = getCachedExpectedNbArchiveRoutes(storageClass);

    // Check that the number of archive routes is correct
    if(copyToPoolMap.empty()) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << storageClassName << " has no archive routes";
      throw ue;
    }
    if(copyToPoolMap.size() != expectedNbRoutes) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << storageClassName << " does not have the"
        " expected number of archive routes routes: expected=" << expectedNbRoutes << ", actual=" <<
        copyToPoolMap.size();
      throw ue;
    }

    
    // Only consider the requester's group if there is no user mount policy
    if(const auto userMountPolicyAndCacheInfo = getCachedRequesterMountPolicy(User(diskInstanceName, user.name));
      !userMountPolicyAndCacheInfo.value) {
      const auto groupMountPolicyAndCacheInfo = getCachedRequesterGroupMountPolicy(Group(diskInstanceName, user.group));
      const auto& groupMountPolicy = groupMountPolicyAndCacheInfo.value;

      if(!groupMountPolicy) {

        const auto defaultUserMountPolicyAndCacheInfo = getCachedRequesterMountPolicy(User(diskInstanceName, "default"));
        

        if(!defaultUserMountPolicyAndCacheInfo.value) {
          exception::UserErrorWithCacheInfo ue(userMountPolicyAndCacheInfo.cacheInfo);
          ue.getMessage() << "Failed to check and get next archive file ID: No mount rules: storageClass=" <<
            storageClassName << " requester=" << diskInstanceName << ":" << user.name << ":" << user.group;
          throw ue;
        }
      }
    }

    // Now that we have found both the archive routes and the mount policy it's
    // safe to consume an archive file identifier
    auto conn = m_connPool->getConn();
    return getNextArchiveFileId(conn);
  } catch(exception::UserErrorWithCacheInfo &ue) {
    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("cacheInfo", ue.cacheInfo)
       .add("userError", ue.getMessage().str());
    lc.log(log::INFO, "Catalogue::checkAndGetNextArchiveFileId caught a UserErrorWithCacheInfo");
    throw;
  }
}

common::dataStructures::ArchiveFileQueueCriteria RdbmsArchiveFileCatalogue::getArchiveFileQueueCriteria(
  const std::string &diskInstanceName, const std::string &storageClassName,
  const common::dataStructures::RequesterIdentity &user) {
  const auto storageClass = catalogue::StorageClass(storageClassName);
  const common::dataStructures::TapeCopyToPoolMap copyToPoolMap = getCachedTapeCopyToPoolMap(storageClass);
  const uint64_t expectedNbRoutes = getCachedExpectedNbArchiveRoutes(storageClass);

  // Check that the number of archive routes is correct
  if(copyToPoolMap.empty()) {
    exception::UserError ue;
    ue.getMessage() << "Storage class " << diskInstanceName << ": " << storageClassName << " has no archive routes";
    throw ue;
  }
  if(copyToPoolMap.size() != expectedNbRoutes) {
    exception::UserError ue;
    ue.getMessage() << "Storage class " << diskInstanceName << ": " << storageClassName << " does not have the"
      " expected number of archive routes routes: expected=" << expectedNbRoutes << ", actual=" <<
      copyToPoolMap.size();
    throw ue;
  }

  // Get the mount policy - user mount policies overrule group ones
  const auto userMountPolicyAndCacheInfo = getCachedRequesterMountPolicy(User(diskInstanceName, user.name));

  if(const auto& userMountPolicy = userMountPolicyAndCacheInfo.value; userMountPolicy) {
    return common::dataStructures::ArchiveFileQueueCriteria(copyToPoolMap, *userMountPolicy);
  } else {
    const auto groupMountPolicyAndCacheInfo = getCachedRequesterGroupMountPolicy(Group(diskInstanceName, user.group));

    if(const auto& groupMountPolicy = groupMountPolicyAndCacheInfo.value; groupMountPolicy) {
      return common::dataStructures::ArchiveFileQueueCriteria(copyToPoolMap, *groupMountPolicy);
    } else {
      const auto defaultUserMountPolicyAndCacheInfo = getCachedRequesterMountPolicy(User(diskInstanceName, "default"));

      if(const auto& defaultUserMountPolicy = defaultUserMountPolicyAndCacheInfo.value; defaultUserMountPolicy) {
        return common::dataStructures::ArchiveFileQueueCriteria(copyToPoolMap, *defaultUserMountPolicy);
      } else {
        exception::UserErrorWithCacheInfo ue(defaultUserMountPolicyAndCacheInfo.cacheInfo);
        ue.getMessage() << "Failed to get archive file queue criteria: No mount rules: storageClass=" <<
          storageClassName << " requester=" << diskInstanceName << ":" << user.name << ":" << user.group;
        throw ue;
      }
    }
  }
}

ArchiveFileItor RdbmsArchiveFileCatalogue::getArchiveFilesItor(const TapeFileSearchCriteria &searchCriteria) const {
  checkTapeFileSearchCriteria(searchCriteria);

  // If this is the listing of the contents of a tape
  if (!searchCriteria.archiveFileId && !searchCriteria.diskInstance && !searchCriteria.diskFileIds &&
    !searchCriteria.fSeq && searchCriteria.vid) {
    return getTapeContentsItor(searchCriteria.vid.value());
  }

  // Create a connection to populate the temporary table (specialised by database type)
  auto conn = m_rdbmsCatalogue->m_archiveFileListingConnPool->getConn();
  const auto tempDiskFxidsTableName = m_rdbmsCatalogue->createAndPopulateTempTableFxid(conn,
    searchCriteria.diskFileIds);
  // Pass ownership of the connection to the Iterator object
  auto impl = new RdbmsCatalogueGetArchiveFilesItor(m_log, std::move(conn), searchCriteria, tempDiskFxidsTableName);
  return ArchiveFileItor(impl);
}

common::dataStructures::ArchiveFile RdbmsArchiveFileCatalogue::getArchiveFileForDeletion(
  const TapeFileSearchCriteria &criteria) const {
  if (!criteria.diskFileIds && !criteria.archiveFileId) {
    throw exception::UserError("To delete a file copy either the diskFileId+diskInstanceName or archiveFileId must be specified");
  }
  if (criteria.diskFileIds && !criteria.diskInstance) {
    throw exception::UserError("DiskFileId makes no sense without disk instance");
  }
  if (!criteria.vid) {
    throw exception::UserError("Vid must be specified");
  }

  auto vid = criteria.vid.value();
  TapeFileSearchCriteria searchCriteria = criteria;
  searchCriteria.vid = std::nullopt; //unset vid, we want to get all copies of the archive file so we can check that it is not a one copy file
  auto itor = getArchiveFilesItor(searchCriteria);

  // itor should have at most one archive file since we always search on unique attributes
  if (!itor.hasMore()) {
    if (criteria.archiveFileId) {
      throw exception::UserError(std::string("Cannot delete a copy of the file with archiveFileId ") +
                                std::to_string(criteria.archiveFileId.value()) +
                                 " because the file does not exist");
    } else {
      throw exception::UserError(std::string("Cannot delete a copy of the file with eosFxid ") +
                                criteria.diskFileIds.value().front() + " and diskInstance " +
                                criteria.diskInstance.value() + " because the file does not exist");
    }
  }

  cta::common::dataStructures::ArchiveFile af = itor.next();

  if (af.tapeFiles.size() == 1) {
    if (criteria.archiveFileId) {
      throw exception::UserError(std::string("Cannot delete a copy of the file with archiveFileId ") +
                                std::to_string(criteria.archiveFileId.value()) +
                                " because it is the only copy");
    } else {
      throw exception::UserError(std::string("Cannot delete a copy of the file with eosFxid ") +
                                criteria.diskFileIds.value().front() + " and diskInstance " +
                                criteria.diskInstance.value() + " because it is the only copy");
    }
  }
  af.tapeFiles.removeAllVidsExcept(vid); // assume there is only one copy per vid, this should return a list with at most one item
  if (af.tapeFiles.empty()) {
    if (criteria.archiveFileId) {
      throw exception::UserError(std::string("No copy of the file with archiveFileId ") +
                                std::to_string(criteria.archiveFileId.value()) +
                                 " on vid " + vid);
    } else {
      throw exception::UserError(std::string("No copy of the file with eosFxid ") +
                                criteria.diskFileIds.value().front() + " and diskInstance " +
                                criteria.diskInstance.value() + " on vid " + vid);
    }
  }
  if (af.tapeFiles.size() > 1){
    if (criteria.archiveFileId) {
      throw exception::UserError(std::string("Error: More than one copy of the file with archiveFileId ") +
                                std::to_string(criteria.archiveFileId.value()) +
                                 " on vid " + vid);
    } else {
      throw exception::UserError(std::string("Error: More than one copy of the file with eosFxid ") +
                                criteria.diskFileIds.value().front() + " and diskInstance " +
                                criteria.diskInstance.value() + " on vid " + vid);
    }
  }
  return af;
}

std::list<common::dataStructures::ArchiveFile> RdbmsArchiveFileCatalogue::getFilesForRepack(const std::string &vid,
  const uint64_t startFSeq, const uint64_t maxNbFiles) const {
  std::string sql =
    "SELECT "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
      "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
      "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
      "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
      "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
      "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
      "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
      "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
      "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
      "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
      "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
      "TAPE_FILE.VID AS VID,"
      "TAPE_FILE.FSEQ AS FSEQ,"
      "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
      "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
      "TAPE_FILE.COPY_NB AS COPY_NB,"
      "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
      "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME "
    "FROM "
      "ARCHIVE_FILE "
    "INNER JOIN STORAGE_CLASS ON "
      "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
    "INNER JOIN TAPE_FILE ON "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
    "INNER JOIN TAPE ON "
      "TAPE_FILE.VID = TAPE.VID "
    "INNER JOIN TAPE_POOL ON "
      "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
    "WHERE "
      "TAPE_FILE.VID = :VID AND "
      "TAPE_FILE.FSEQ >= :START_FSEQ "
      "ORDER BY FSEQ";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VID", vid);
  stmt.bindUint64(":START_FSEQ", startFSeq);
  auto rset = stmt.executeQuery();

  std::list<common::dataStructures::ArchiveFile> archiveFiles;
  while(rset.next()) {
    common::dataStructures::ArchiveFile archiveFile;

    archiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
    archiveFile.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
    archiveFile.diskFileId = rset.columnString("DISK_FILE_ID");
    archiveFile.diskFileInfo.owner_uid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_UID"));
    archiveFile.diskFileInfo.gid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_GID"));
    archiveFile.fileSize = rset.columnUint64("SIZE_IN_BYTES");
    archiveFile.checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"),
      static_cast<uint32_t>(rset.columnUint64("CHECKSUM_ADLER32")));
    archiveFile.storageClass = rset.columnString("STORAGE_CLASS_NAME");
    archiveFile.creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
    archiveFile.reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");

    common::dataStructures::TapeFile tapeFile;
    tapeFile.vid = rset.columnString("VID");
    tapeFile.fSeq = rset.columnUint64("FSEQ");
    tapeFile.blockId = rset.columnUint64("BLOCK_ID");
    tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
    tapeFile.copyNb = static_cast<uint8_t>(rset.columnUint64("COPY_NB"));
    tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
    tapeFile.checksumBlob = archiveFile.checksumBlob; // Duplicated for convenience

    archiveFile.tapeFiles.push_back(tapeFile);

    archiveFiles.push_back(archiveFile);

    if(maxNbFiles == archiveFiles.size()) break;
  }
  return archiveFiles;
}

ArchiveFileItor RdbmsArchiveFileCatalogue::getArchiveFilesForRepackItor(const std::string &vid,
  const uint64_t startFSeq) const {
  auto impl = std::make_unique<RdbmsCatalogueGetArchiveFilesForRepackItor>(m_log, *(m_rdbmsCatalogue->m_archiveFileListingConnPool),
    vid, startFSeq);
  return ArchiveFileItor(impl.release());
}

common::dataStructures::ArchiveFileSummary RdbmsArchiveFileCatalogue::getTapeFileSummary(
  const TapeFileSearchCriteria &searchCriteria) const {
  auto conn = m_connPool->getConn();

  std::string sql =
    "SELECT "
      "COALESCE(SUM(ARCHIVE_FILE.SIZE_IN_BYTES), 0) AS TOTAL_BYTES,"
      "COUNT(ARCHIVE_FILE.ARCHIVE_FILE_ID) AS TOTAL_FILES "
    "FROM "
      "ARCHIVE_FILE "
    "INNER JOIN STORAGE_CLASS ON "
      "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
    "INNER JOIN TAPE_FILE ON "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
    "INNER JOIN TAPE ON "
      "TAPE_FILE.VID = TAPE.VID "
    "INNER JOIN TAPE_POOL ON "
      "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID";

  const bool thereIsAtLeastOneSearchCriteria =
    searchCriteria.archiveFileId  ||
    searchCriteria.diskInstance   ||
    searchCriteria.vid            ||
    searchCriteria.diskFileIds;

  if(thereIsAtLeastOneSearchCriteria) {
    sql += " WHERE ";
  }

  bool addedAWhereConstraint = false;

  if(searchCriteria.archiveFileId) {
    sql += " ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
    addedAWhereConstraint = true;
  }
  if(searchCriteria.diskInstance) {
    if(addedAWhereConstraint) sql += " AND ";
    sql += "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
    addedAWhereConstraint = true;
  }
  if(searchCriteria.vid) {
    if(addedAWhereConstraint) sql += " AND ";
    sql += "TAPE_FILE.VID = :VID";
    addedAWhereConstraint = true;
  }
  if(searchCriteria.diskFileIds) {
    const auto tempDiskFxidsTableName = m_rdbmsCatalogue->createAndPopulateTempTableFxid(conn,
      searchCriteria.diskFileIds);

    if(addedAWhereConstraint) sql += " AND ";
    sql += "ARCHIVE_FILE.DISK_FILE_ID IN (SELECT DISK_FILE_ID FROM " + tempDiskFxidsTableName + ")";
  }

  auto stmt = conn.createStmt(sql);
  if(searchCriteria.archiveFileId) {
    stmt.bindUint64(":ARCHIVE_FILE_ID", searchCriteria.archiveFileId.value());
  }
  if(searchCriteria.diskInstance) {
    stmt.bindString(":DISK_INSTANCE_NAME", searchCriteria.diskInstance.value());
  }
  if(searchCriteria.vid) {
    stmt.bindString(":VID", searchCriteria.vid.value());
  }
  auto rset = stmt.executeQuery();

  if(!rset.next()) {
    throw exception::Exception("SELECT COUNT statement did not return a row");
  }

  common::dataStructures::ArchiveFileSummary summary;
  summary.totalBytes = rset.columnUint64("TOTAL_BYTES");
  summary.totalFiles = rset.columnUint64("TOTAL_FILES");
  return summary;
}

common::dataStructures::ArchiveFile RdbmsArchiveFileCatalogue::getArchiveFileById(const uint64_t id) const {
  auto conn = m_connPool->getConn();
  const auto archiveFile = getArchiveFileById(conn, id);

  // Throw an exception if the archive file does not exist
  if(nullptr == archiveFile.get()) {
    exception::Exception ex;
    ex.getMessage() << "No such archive file with ID " << id;
    throw ex;
  }

  return *archiveFile;
}

std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsArchiveFileCatalogue::getArchiveFileById(rdbms::Conn &conn,
  const uint64_t id) const {
  const char *const sql =
    "SELECT "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
      "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
      "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
      "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
      "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
      "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
      "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
      "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
      "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
      "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
      "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
      "TAPE_FILE.VID AS VID,"
      "TAPE_FILE.FSEQ AS FSEQ,"
      "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
      "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
      "TAPE_FILE.COPY_NB AS COPY_NB,"
      "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
    "FROM "
      "ARCHIVE_FILE "
    "INNER JOIN STORAGE_CLASS ON "
      "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
    "INNER JOIN TAPE_FILE ON "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
    "WHERE "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID "
    "ORDER BY "
      "TAPE_FILE.CREATION_TIME ASC";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":ARCHIVE_FILE_ID", id);
  auto rset = stmt.executeQuery();
  std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
  while (rset.next()) {
    if(nullptr == archiveFile.get()) {
      archiveFile = std::make_unique<common::dataStructures::ArchiveFile>();

      archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
      archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
      archiveFile->diskFileInfo.owner_uid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_UID"));
      archiveFile->diskFileInfo.gid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_GID"));
      archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
      archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"),
        static_cast<uint32_t>(rset.columnUint64("CHECKSUM_ADLER32")));
      archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
      archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
      archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
    }

    // If there is a tape file
    if(!rset.columnIsNull("VID")) {
      // Add the tape file to the archive file's in-memory structure
      common::dataStructures::TapeFile tapeFile;
      tapeFile.vid = rset.columnString("VID");
      tapeFile.fSeq = rset.columnUint64("FSEQ");
      tapeFile.blockId = rset.columnUint64("BLOCK_ID");
      tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
      tapeFile.copyNb = static_cast<uint8_t>(rset.columnUint64("COPY_NB"));
      tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
      tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience

      archiveFile->tapeFiles.push_back(tapeFile);
    }
  }

  return archiveFile;
}

common::dataStructures::TapeCopyToPoolMap RdbmsArchiveFileCatalogue::getCachedTapeCopyToPoolMap(
  const catalogue::StorageClass &storageClass) const {
  auto l_getNonCachedValue = [this,&storageClass] {
    auto conn = m_connPool->getConn();
    return getTapeCopyToPoolMap(conn, storageClass);
  };
  return m_tapeCopyToPoolCache.getCachedValue(storageClass, l_getNonCachedValue).value;
}

uint64_t RdbmsArchiveFileCatalogue::getCachedExpectedNbArchiveRoutes(
  const catalogue::StorageClass &storageClass) const {
  auto l_getNonCachedValue = [this,&storageClass] {
    auto conn = m_connPool->getConn();
    return getExpectedNbArchiveRoutes(conn, storageClass);
  };
  return m_expectedNbArchiveRoutesCache.getCachedValue(storageClass, l_getNonCachedValue).value;
}

ValueAndTimeBasedCacheInfo<std::optional<common::dataStructures::MountPolicy>>
  RdbmsArchiveFileCatalogue::getCachedRequesterMountPolicy(const User &user) const {
  auto l_getNonCachedValue = [this,&user] {
    auto conn = m_connPool->getConn();
    const auto mountPolicy = static_cast<RdbmsMountPolicyCatalogue*>(m_rdbmsCatalogue->MountPolicy().get());
    return mountPolicy->getRequesterMountPolicy(conn, user);
  };
  return m_rdbmsCatalogue->m_userMountPolicyCache.getCachedValue(user, l_getNonCachedValue);
}

//------------------------------------------------------------------------------
// getCachedRequesterGroupMountPolicy
//------------------------------------------------------------------------------
ValueAndTimeBasedCacheInfo<std::optional<common::dataStructures::MountPolicy>>
  RdbmsArchiveFileCatalogue::getCachedRequesterGroupMountPolicy(const Group &group) const {
  auto l_getNonCachedValue = [this,&group] {
    auto conn = m_connPool->getConn();
    const auto mountPolicyCatalogue = static_cast<RdbmsMountPolicyCatalogue*>(m_rdbmsCatalogue->MountPolicy().get());
    return mountPolicyCatalogue->getRequesterGroupMountPolicy(conn, group);
  };
  return m_rdbmsCatalogue->m_groupMountPolicyCache.getCachedValue(group, l_getNonCachedValue);
}

ArchiveFileItor RdbmsArchiveFileCatalogue::getArchiveFilesItor(rdbms::Conn &conn,
  const TapeFileSearchCriteria &searchCriteria) const {

  checkTapeFileSearchCriteria(conn, searchCriteria);

  // If this is the listing of the contents of a tape
  if (!searchCriteria.archiveFileId && !searchCriteria.diskInstance && !searchCriteria.diskFileIds &&
    searchCriteria.vid) {
    return getTapeContentsItor(searchCriteria.vid.value());
  }

  auto archiveListingConn = m_rdbmsCatalogue->m_archiveFileListingConnPool->getConn();
  const auto tempDiskFxidsTableName = m_rdbmsCatalogue->createAndPopulateTempTableFxid(archiveListingConn,
    searchCriteria.diskFileIds);
  // Pass ownership of the connection to the Iterator object
  auto impl = std::make_unique<RdbmsCatalogueGetArchiveFilesItor>(m_log, std::move(archiveListingConn), searchCriteria,
    tempDiskFxidsTableName);
  return ArchiveFileItor(impl.release());
}

void RdbmsArchiveFileCatalogue::checkTapeFileSearchCriteria(const TapeFileSearchCriteria &searchCriteria) const {
  auto conn = m_connPool->getConn();
  checkTapeFileSearchCriteria(conn, searchCriteria);

}

void RdbmsArchiveFileCatalogue::checkTapeFileSearchCriteria(rdbms::Conn &conn,
  const TapeFileSearchCriteria &searchCriteria) const {
  if(searchCriteria.archiveFileId &&
    !RdbmsCatalogueUtils::archiveFileIdExists(conn, searchCriteria.archiveFileId.value())) {
      throw exception::UserError(std::string("Archive file with ID ") +
        std::to_string(searchCriteria.archiveFileId.value()) + " does not exist");
  }

  if(searchCriteria.diskFileIds && !searchCriteria.diskInstance) {
    throw exception::UserError(std::string("Disk file IDs are ambiguous without disk instance name"));
  }

  if (searchCriteria.fSeq && !searchCriteria.vid) {
    throw exception::UserError(std::string("fSeq makes no sense without vid"));
  }

  if(searchCriteria.vid && !RdbmsCatalogueUtils::tapeExists(conn, searchCriteria.vid.value())) {
    throw exception::UserError(std::string("Tape ") + searchCriteria.vid.value() + " does not exist");
  }
}

ArchiveFileItor RdbmsArchiveFileCatalogue::getTapeContentsItor(const std::string &vid) const {
  // Create a connection to populate the temporary table (specialised by database type)
  auto impl = std::make_unique<RdbmsCatalogueTapeContentsItor>(m_log, *m_connPool, vid);
  return ArchiveFileItor(impl.release());
}

common::dataStructures::TapeCopyToPoolMap RdbmsArchiveFileCatalogue::getTapeCopyToPoolMap(rdbms::Conn &conn,
  const catalogue::StorageClass &storageClass) const {
  common::dataStructures::TapeCopyToPoolMap copyToPoolMap;
  const char *const sql =
    "SELECT "
      "ARCHIVE_ROUTE.COPY_NB AS COPY_NB,"
      "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME "
    "FROM "
      "ARCHIVE_ROUTE "
    "INNER JOIN STORAGE_CLASS ON "
      "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
    "INNER JOIN TAPE_POOL ON "
      "ARCHIVE_ROUTE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
    "WHERE "
      "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":STORAGE_CLASS_NAME", storageClass.storageClassName);
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    const auto copyNb = static_cast<uint32_t>(rset.columnUint64("COPY_NB"));
    const std::string tapePoolName = rset.columnString("TAPE_POOL_NAME");
    copyToPoolMap[copyNb] = tapePoolName;
  }

  return copyToPoolMap;
}

uint64_t RdbmsArchiveFileCatalogue::getExpectedNbArchiveRoutes(rdbms::Conn &conn,
  const catalogue::StorageClass &storageClass) const {
  const char *const sql =
    "SELECT "
      "COUNT(*) AS NB_ROUTES "
    "FROM "
      "ARCHIVE_ROUTE "
    "INNER JOIN STORAGE_CLASS ON "
      "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
    "WHERE "
      "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":STORAGE_CLASS_NAME", storageClass.storageClassName);
  auto rset = stmt.executeQuery();
  if(!rset.next()) {
    throw exception::Exception("Result set of SELECT COUNT(*) is empty");
  }
  return rset.columnUint64("NB_ROUTES");
}

void RdbmsArchiveFileCatalogue::modifyArchiveFileStorageClassId(const uint64_t archiveFileId,
  const std::string& newStorageClassName) const {
  auto conn = m_connPool->getConn();
  if(!RdbmsCatalogueUtils::storageClassExists(conn, newStorageClassName)) {
    exception::UserError ue;
    ue.getMessage() << "Cannot modify archive file " << ": " << archiveFileId << " because storage class "
    << ":" << newStorageClassName << " does not exist";
    throw ue;
  }

  const char *const sql =
  "UPDATE ARCHIVE_FILE   "
  "SET STORAGE_CLASS_ID = ("
    "SELECT STORAGE_CLASS_ID FROM STORAGE_CLASS WHERE STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME "
  ") "
  "WHERE "
    "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";

  auto stmt = conn.createStmt(sql);
  stmt.bindString(":STORAGE_CLASS_NAME", newStorageClassName);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
}

void RdbmsArchiveFileCatalogue::modifyArchiveFileFxIdAndDiskInstance(const uint64_t archiveId, const std::string& fxId,
  const std::string &diskInstance) const {
  const char *const sql =
  "UPDATE ARCHIVE_FILE SET "
    "DISK_FILE_ID = :FXID,"
    "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME "
  "WHERE "
    "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":FXID", fxId);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveId);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.executeNonQuery();
}

//------------------------------------------------------------------------------
// moveArchiveFileToRecycleBin
//------------------------------------------------------------------------------
void RdbmsArchiveFileCatalogue::moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
  log::LogContext & lc) {
  if(!request.archiveFile){
    //The archive file does not exist in the catalogue, nothing to do with it
    return;
  }
  cta::common::dataStructures::ArchiveFile archiveFile = request.archiveFile.value();
  utils::Timer t;
  utils::Timer totalTime;
  log::TimingList tl;
  try {
    checkDeleteRequestConsistency(request,archiveFile);
    tl.insertAndReset("checkDeleteRequestConsistency",t);
  } catch(const cta::exception::Exception & ex){
    log::ScopedParamContainer spc(lc);
    spc.add("fileId", std::to_string(request.archiveFileID))
     .add("diskInstance", archiveFile.diskInstance)
     .add("requestDiskInstance", request.diskInstance)
     .add("diskFileId", archiveFile.diskFileId)
     .add("diskFileInfo.owner_uid", archiveFile.diskFileInfo.owner_uid)
     .add("diskFileInfo.gid", archiveFile.diskFileInfo.gid)
     .add("fileSize", std::to_string(archiveFile.fileSize))
     .add("creationTime", std::to_string(archiveFile.creationTime))
     .add("reconciliationTime", std::to_string(archiveFile.reconciliationTime))
     .add("diskFilePath",request.diskFilePath)
     .add("errorMessage",ex.getMessageValue())
     .add("storageClass", archiveFile.storageClass);
    archiveFile.checksumBlob.addFirstChecksumToLog(spc);
    for(const auto& tapeFile : archiveFile.tapeFiles) {
      std::stringstream tapeCopyLogStream;
      tapeCopyLogStream << "copy number: " << static_cast<int>(tapeFile.copyNb)
        << " vid: " << tapeFile.vid
        << " fSeq: " << tapeFile.fSeq
        << " blockId: " << tapeFile.blockId
        << " creationTime: " << tapeFile.creationTime
        << " fileSize: " << tapeFile.fileSize;
      spc.add("TAPE FILE", tapeCopyLogStream.str());
    }
    lc.log(log::WARNING, "Failed to move archive file to the file-recycle-log.");

    exception::UserError ue;
    ue.getMessage() << "Failed to move archive file with ID " << request.archiveFileID
      << " to the file-recycle-log. errorMessage=" << ex.getMessageValue();
    throw ue;
  }

  //All checks are good, we can move the file to the recycle-bin
  auto conn = m_connPool->getConn();
  rdbms::AutoRollback autoRollback(conn);
  copyArchiveFileToFileRecyleLogAndDelete(conn,request,lc);
  tl.insertAndReset("copyArchiveFileToFileRecyleLogAndDeleteTime",t);
  tl.insertAndReset("totalTime",totalTime);
  log::ScopedParamContainer spc(lc);
  spc.add("fileId", std::to_string(request.archiveFileID))
    .add("diskInstance", archiveFile.diskInstance)
    .add("requestDiskInstance", request.diskInstance)
    .add("diskFileId", archiveFile.diskFileId)
    .add("diskFileInfo.owner_uid", archiveFile.diskFileInfo.owner_uid)
    .add("diskFileInfo.gid", archiveFile.diskFileInfo.gid)
    .add("fileSize", std::to_string(archiveFile.fileSize))
    .add("creationTime", std::to_string(archiveFile.creationTime))
    .add("reconciliationTime", std::to_string(archiveFile.reconciliationTime))
    .add("storageClass", archiveFile.storageClass);
  archiveFile.checksumBlob.addFirstChecksumToLog(spc);
  for(const auto& tapeFile : archiveFile.tapeFiles) {
    std::stringstream tapeCopyLogStream;
    tapeCopyLogStream << "copy number: " << static_cast<int>(tapeFile.copyNb)
      << " vid: " << tapeFile.vid
      << " fSeq: " << tapeFile.fSeq
      << " blockId: " << tapeFile.blockId
      << " creationTime: " << tapeFile.creationTime
      << " fileSize: " << tapeFile.fileSize;
    spc.add("TAPE FILE", tapeCopyLogStream.str());
  }
  tl.addToLog(spc);
  lc.log(log::INFO, "In RdbmsCatalogue::moveArchiveFileToRecycleLog(): ArchiveFile moved to the file-recycle-log.");
}

void RdbmsArchiveFileCatalogue::checkDeleteRequestConsistency(
  const cta::common::dataStructures::DeleteArchiveRequest& deleteRequest,
  const cta::common::dataStructures::ArchiveFile& archiveFile) const {
  if(deleteRequest.diskInstance != archiveFile.diskInstance){
    std::ostringstream msg;
    msg << "Failed to move archive file with ID " << deleteRequest.archiveFileID
        << " to the recycle-bin because the disk instance of "
        "the request does not match that of the archived file: archiveFileId=" << archiveFile.archiveFileID
        << " requestDiskInstance=" << deleteRequest.diskInstance << " archiveFileDiskInstance=" <<
        archiveFile.diskInstance;
    log::LogContext lc(m_log);
    lc.log(log::ALERT, "Failed to move archive file to the recycle-bin because the disk instance of the request "
      "does not match that of the archived file.");
    throw cta::exception::Exception(msg.str());
  }
  if(deleteRequest.diskFileSize.has_value() && deleteRequest.diskFileSize.value() != archiveFile.fileSize) {
    std::ostringstream msg;
    msg << "Failed to move archive file with ID " << deleteRequest.archiveFileID
        << " to the recycle-bin because the disk file size of "
        "the request does not match that of the archived file: archiveFileId=" << archiveFile.archiveFileID
        << " requestDiskFileSize=" << deleteRequest.diskFileSize.value() << " archiveFileDiskFileSize=" <<
        archiveFile.fileSize;
    log::LogContext lc(m_log);
    lc.log(log::ALERT, "Failed to move archive file to the recycle-bin because the disk file size of the "
      "request does not match that of the archived file.");
    throw cta::exception::Exception(msg.str());
  }
  if (deleteRequest.checksumBlob.has_value() && deleteRequest.checksumBlob.value() != archiveFile.checksumBlob) {
    std::ostringstream msg;
    msg << "Failed to move archive file with ID " << deleteRequest.archiveFileID
        << " to the recycle-bin because the checksum of "
        "the request does not match that of the archived file: archiveFileId=" << archiveFile.archiveFileID
        << " requestChecksum=" << deleteRequest.checksumBlob.value().serialize() << " archiveFileChecksum=" <<
        archiveFile.checksumBlob.serialize();
    log::LogContext lc(m_log);
    lc.log(log::ALERT, "Failed to move archive file to the recycle-bin because the checksum of the request "
      "does not match that of the archived file.");
    throw cta::exception::Exception(msg.str());
  }
  if(deleteRequest.diskFilePath.empty()){
    std::ostringstream msg;
    msg << "Failed to move archive file with ID " << deleteRequest.archiveFileID
        << " to the recycle-bin because the disk file path has not been provided.";
    throw cta::exception::Exception(msg.str());
  }
}

void RdbmsArchiveFileCatalogue::deleteArchiveFile(rdbms::Conn& conn,
  const common::dataStructures::DeleteArchiveRequest& request) const {
  const char *const deleteArchiveFileSql =
  "DELETE FROM "
    "ARCHIVE_FILE "
  "WHERE ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";

  auto deleteArchiveFileStmt = conn.createStmt(deleteArchiveFileSql);
  deleteArchiveFileStmt.bindUint64(":ARCHIVE_FILE_ID",request.archiveFileID);
  deleteArchiveFileStmt.executeNonQuery();
}

void RdbmsArchiveFileCatalogue::insertArchiveFile(rdbms::Conn &conn, const ArchiveFileRowWithoutTimestamps &row) const {
  if(!RdbmsCatalogueUtils::storageClassExists(conn, row.storageClassName)) {
    throw exception::UserError(std::string("Storage class ") + row.diskInstance + ":" + row.storageClassName +
      " does not exist");
  }

  const time_t now = time(nullptr);
  const char *const sql =
    "INSERT INTO ARCHIVE_FILE("
      "ARCHIVE_FILE_ID,"
      "DISK_INSTANCE_NAME,"
      "DISK_FILE_ID,"
      "DISK_FILE_UID,"
      "DISK_FILE_GID,"
      "SIZE_IN_BYTES,"
      "CHECKSUM_BLOB,"
      "CHECKSUM_ADLER32,"
      "STORAGE_CLASS_ID,"
      "CREATION_TIME,"
      "RECONCILIATION_TIME)"
    "SELECT "
      ":ARCHIVE_FILE_ID,"
      ":DISK_INSTANCE_NAME,"
      ":DISK_FILE_ID,"
      ":DISK_FILE_UID,"
      ":DISK_FILE_GID,"
      ":SIZE_IN_BYTES,"
      ":CHECKSUM_BLOB,"
      ":CHECKSUM_ADLER32,"
      "STORAGE_CLASS_ID,"
      ":CREATION_TIME,"
      ":RECONCILIATION_TIME "
    "FROM "
      "STORAGE_CLASS "
    "WHERE "
      "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
  auto stmt = conn.createStmt(sql);

  stmt.bindUint64(":ARCHIVE_FILE_ID", row.archiveFileId);
  stmt.bindString(":DISK_INSTANCE_NAME", row.diskInstance);
  stmt.bindString(":DISK_FILE_ID", row.diskFileId);
  stmt.bindUint64(":DISK_FILE_UID", row.diskFileOwnerUid);
  stmt.bindUint64(":DISK_FILE_GID", row.diskFileGid);
  stmt.bindUint64(":SIZE_IN_BYTES", row.size);
  stmt.bindBlob  (":CHECKSUM_BLOB", row.checksumBlob.serialize());
  // Keep transition ADLER32 checksum up-to-date if it exists
  uint32_t adler32;
  try {
    std::string adler32hex = checksum::ChecksumBlob::ByteArrayToHex(row.checksumBlob.at(checksum::ADLER32));
    adler32 = static_cast<uint32_t>(strtoul(adler32hex.c_str(), nullptr, 16));
  } catch(exception::ChecksumTypeMismatch&) {
    adler32 = 0;
  }
  stmt.bindUint64(":CHECKSUM_ADLER32", adler32);
  stmt.bindString(":STORAGE_CLASS_NAME", row.storageClassName);
  stmt.bindUint64(":CREATION_TIME", now);
  stmt.bindUint64(":RECONCILIATION_TIME", now);

  stmt.executeNonQuery();
}

//------------------------------------------------------------------------------
// getArchiveFileRowByArchiveId
//------------------------------------------------------------------------------
std::unique_ptr<ArchiveFileRow> RdbmsArchiveFileCatalogue::getArchiveFileRowById(rdbms::Conn &conn,
  const uint64_t id) const {
  const char *const sql =
    "SELECT "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID, "
      "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
      "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID, "
      "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID, "
      "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID, "
      "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES, "
      "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB, "
      "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32, "
      "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME, "
      "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME, "
      "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME "
    "FROM "
      "ARCHIVE_FILE "
    "INNER JOIN STORAGE_CLASS ON "
      "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
    "WHERE "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":ARCHIVE_FILE_ID", id);
  auto rset = stmt.executeQuery();

  std::unique_ptr<ArchiveFileRow> row;
  if (rset.next()) {
    row = std::make_unique<ArchiveFileRow>();

    row->archiveFileId = rset.columnUint64("ARCHIVE_FILE_ID");
    row->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
    row->diskFileId = rset.columnString("DISK_FILE_ID");
    row->diskFileOwnerUid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_UID"));
    row->diskFileGid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_GID"));
    row->size = rset.columnUint64("SIZE_IN_BYTES");
    row->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"),
      static_cast<uint32_t>(rset.columnUint64("CHECKSUM_ADLER32")));
    row->storageClassName = rset.columnString("STORAGE_CLASS_NAME");
    row->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
    row->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
  }

  return row;
}


//------------------------------------------------------------------------------
// updateDiskFileId
//------------------------------------------------------------------------------
void RdbmsArchiveFileCatalogue::updateDiskFileId(const uint64_t archiveFileId, const std::string &diskInstance,
  const std::string &diskFileId) {
  const char *const sql =
    "UPDATE ARCHIVE_FILE SET "
      "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME, "
      "DISK_FILE_ID = :DISK_FILE_ID "
    "WHERE "
      "ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.bindString(":DISK_FILE_ID", diskFileId);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    std::ostringstream msg;
    msg << "Cannot update the disk file ID of the archive file with archive file ID " << archiveFileId <<
      " because the archive file does not exist";
    throw exception::UserError(msg.str());
  }
}

//------------------------------------------------------------------------------
// getArchiveFileToRetrieveByArchiveFileId
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsArchiveFileCatalogue::getArchiveFileToRetrieveByArchiveFileId(
  rdbms::Conn &conn, const uint64_t archiveFileId) const {
  const char *const sql =
    "SELECT "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
      "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
      "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
      "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
      "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
      "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
      "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
      "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
      "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
      "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
      "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
      "TAPE_FILE.VID AS VID,"
      "TAPE_FILE.FSEQ AS FSEQ,"
      "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
      "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
      "TAPE_FILE.COPY_NB AS COPY_NB,"
      "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
    "FROM "
      "ARCHIVE_FILE "
    "INNER JOIN STORAGE_CLASS ON "
      "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
    "INNER JOIN TAPE_FILE ON "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
    "INNER JOIN TAPE ON "
      "TAPE_FILE.VID = TAPE.VID "
    "WHERE "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND "
      "TAPE.TAPE_STATE IN ('ACTIVE', 'DISABLED') "
    "ORDER BY "
      "TAPE_FILE.CREATION_TIME ASC";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
  std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
  while (rset.next()) {
    if(nullptr == archiveFile.get()) {
      archiveFile = std::make_unique<common::dataStructures::ArchiveFile>();

      archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
      archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
      archiveFile->diskFileInfo.owner_uid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_UID"));
      archiveFile->diskFileInfo.gid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_GID"));
      archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
      archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"),
        static_cast<uint32_t>(rset.columnUint64("CHECKSUM_ADLER32")));
      archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
      archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
      archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
    }

    // If there is a tape file we add it to the archiveFile's list of tape files
    if(!rset.columnIsNull("VID")) {
      // Add the tape file to the archive file's in-memory structure
      common::dataStructures::TapeFile tapeFile;
      tapeFile.vid = rset.columnString("VID");
      tapeFile.fSeq = rset.columnUint64("FSEQ");
      tapeFile.blockId = rset.columnUint64("BLOCK_ID");
      tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
      tapeFile.copyNb = static_cast<uint8_t>(rset.columnUint64("COPY_NB"));
      tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
      tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience

      archiveFile->tapeFiles.push_back(tapeFile);
    }
  }

  //If there are no tape files that belong to the archive file, then return a nullptr.
  if(archiveFile.get() != nullptr && archiveFile->tapeFiles.empty()){
    archiveFile.reset(nullptr);
  }

  return archiveFile;
}

//------------------------------------------------------------------------------
// getArchiveFileToRetrieveByArchiveFileId
//------------------------------------------------------------------------------
std::list<std::pair<std::string, std::string>> RdbmsArchiveFileCatalogue::getTapeFileStateListForArchiveFileId(
  rdbms::Conn &conn, const uint64_t archiveFileId) const {
  const char *const sql =
    "SELECT "
      "TAPE_FILE.VID AS VID,"
      "TAPE.TAPE_STATE AS STATE "
    "FROM "
      "ARCHIVE_FILE "
    "INNER JOIN TAPE_FILE ON "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
    "INNER JOIN TAPE ON "
      "TAPE_FILE.VID = TAPE.VID "
    "WHERE "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID ";

  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
  std::list<std::pair<std::string, std::string>> ret;
  while (rset.next()) {
    const auto &vid = rset.columnString("VID");
    const auto &state = rset.columnString("STATE");
    ret.emplace_back(vid, state);
  }
  return ret;
}

} // namespace cta::catalogue
