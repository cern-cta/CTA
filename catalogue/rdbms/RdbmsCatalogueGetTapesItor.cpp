/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/RdbmsCatalogueGetTapesItor.hpp"

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"

namespace cta::catalogue {

namespace {

std::optional<common::dataStructures::TapeLog>
getTapeLogFromRset(const rdbms::Rset& rset, const std::string& driveColName, const std::string& timeColName) {
  const std::optional<std::string> drive = rset.columnOptionalString(driveColName);
  const std::optional<uint64_t> time = rset.columnOptionalUint64(timeColName);

  if (!drive.has_value() && !time.has_value()) {
    return std::nullopt;
  }

  if (drive.has_value() && !time.has_value()) {
    throw exception::Exception(std::string("Database column ") + driveColName + " contains " + drive.value()
                               + " but column " + timeColName + " is nullptr");
  }

  if (time.has_value() && !drive.has_value()) {
    throw exception::Exception(std::string("Database column ") + timeColName + " contains "
                               + std::to_string(time.value()) + " but column " + driveColName + " is nullptr");
  }

  common::dataStructures::TapeLog tapeLog;
  tapeLog.drive = drive.value();
  tapeLog.time = time.value();

  return tapeLog;
}

/**
   * Populates an Tape object with the current column values of the
   * specified result set.
   *
   * @param rset The result set to be used to populate the ArchiveFile object.
   * @return The populated ArchiveFile object.
   */
common::dataStructures::Tape populateTape(const rdbms::Rset& rset) {
  common::dataStructures::Tape tape;

  tape.vid = rset.columnString("VID");
  tape.mediaType = rset.columnString("MEDIA_TYPE");
  tape.vendor = rset.columnString("VENDOR");
  tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
  tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
  tape.vo = rset.columnString("VO");
  tape.encryptionKeyName = rset.columnOptionalString("ENCRYPTION_KEY_NAME");
  tape.purchaseOrder = rset.columnOptionalString("PURCHASE_ORDER");
  tape.physicalLibraryName = rset.columnOptionalString("PHYSICAL_LIBRARY_NAME");
  tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
  tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
  tape.nbMasterFiles = rset.columnUint64("NB_MASTER_FILES");
  tape.masterDataInBytes = rset.columnUint64("MASTER_DATA_IN_BYTES");
  tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
  tape.full = rset.columnBool("IS_FULL");
  tape.dirty = rset.columnBool("DIRTY");
  tape.isFromCastor = rset.columnBool("IS_FROM_CASTOR");

  tape.labelFormat = common::dataStructures::Label::validateFormat(rset.columnOptionalUint8("LABEL_FORMAT"),
                                                                   "[RdbmsCatalogue::getTapes()]");

  tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
  tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
  tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

  tape.readMountCount = rset.columnUint64("READ_MOUNT_COUNT");
  tape.writeMountCount = rset.columnUint64("WRITE_MOUNT_COUNT");

  tape.verificationStatus = rset.columnOptionalString("VERIFICATION_STATUS");

  auto optionalComment = rset.columnOptionalString("USER_COMMENT");
  tape.comment = optionalComment.value_or("");

  tape.setState(rset.columnString("TAPE_STATE"));
  tape.stateReason = rset.columnOptionalString("STATE_REASON");
  tape.stateUpdateTime = rset.columnUint64("STATE_UPDATE_TIME");
  tape.stateModifiedBy = rset.columnString("STATE_MODIFIED_BY");

  tape.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
  tape.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
  tape.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
  tape.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
  tape.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
  tape.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

  return tape;
}
}  // anonymous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogueGetTapesItor::RdbmsCatalogueGetTapesItor(log::Logger& log,
                                                       rdbms::Conn&& conn,
                                                       const TapeSearchCriteria& searchCriteria)
    : m_log(log),
      m_searchCriteria(searchCriteria),
      m_conn(std::move(conn)),
      m_splitByDiskFileId(searchCriteria.diskFileIds.has_value()) {
  std::vector<common::dataStructures::Tape> tapes;
  std::string sql = R"SQL(
    SELECT
      TAPE.VID AS VID,
      MEDIA_TYPE.MEDIA_TYPE_NAME AS MEDIA_TYPE,
      TAPE.VENDOR AS VENDOR,
      LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,
      TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,
      TAPE.ENCRYPTION_KEY_NAME AS ENCRYPTION_KEY_NAME,
      MEDIA_TYPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,
      TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,
      TAPE.NB_MASTER_FILES AS NB_MASTER_FILES,
      TAPE.MASTER_DATA_IN_BYTES AS MASTER_DATA_IN_BYTES,
      TAPE.LAST_FSEQ AS LAST_FSEQ,
      TAPE.IS_FULL AS IS_FULL,
      TAPE.DIRTY AS DIRTY,
      TAPE.PURCHASE_ORDER AS PURCHASE_ORDER,
      PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_NAME AS PHYSICAL_LIBRARY_NAME,

      TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,

      TAPE.LABEL_FORMAT AS LABEL_FORMAT,

      TAPE.LABEL_DRIVE AS LABEL_DRIVE,
      TAPE.LABEL_TIME AS LABEL_TIME,

      TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,
      TAPE.LAST_READ_TIME AS LAST_READ_TIME,

      TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,
      TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,

      TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,
      TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,

      TAPE.VERIFICATION_STATUS AS VERIFICATION_STATUS,

      TAPE.USER_COMMENT AS USER_COMMENT,

      TAPE.TAPE_STATE AS TAPE_STATE,
      TAPE.STATE_REASON AS STATE_REASON,
      TAPE.STATE_UPDATE_TIME AS STATE_UPDATE_TIME,
      TAPE.STATE_MODIFIED_BY AS STATE_MODIFIED_BY,

      TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,

      TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME
    FROM
      TAPE
    INNER JOIN TAPE_POOL ON
      TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID
    INNER JOIN LOGICAL_LIBRARY ON
      TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID
    LEFT JOIN PHYSICAL_LIBRARY ON
      LOGICAL_LIBRARY.PHYSICAL_LIBRARY_ID = PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_ID
    INNER JOIN MEDIA_TYPE ON
      TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID
    INNER JOIN VIRTUAL_ORGANIZATION ON
      TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID
  )SQL";

  if (m_searchCriteria.vid.has_value() || m_searchCriteria.mediaType.has_value() || m_searchCriteria.vendor.has_value()
      || m_searchCriteria.logicalLibrary.has_value() || m_searchCriteria.tapePool.has_value()
      || m_searchCriteria.vo.has_value() || m_searchCriteria.capacityInBytes.has_value()
      || m_searchCriteria.full.has_value() || m_searchCriteria.diskFileIds.has_value()
      || m_searchCriteria.state.has_value() || m_searchCriteria.fromCastor.has_value()
      || m_searchCriteria.purchaseOrder.has_value() || m_searchCriteria.physicalLibraryName.has_value()
      || m_searchCriteria.checkMissingFileCopies.has_value()) {
    sql += R"SQL( WHERE )SQL";
  }

  bool addedAWhereConstraint = false;

  if (m_searchCriteria.vid.has_value()) {
    sql += R"SQL(
      TAPE.VID = :VID
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.mediaType.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      MEDIA_TYPE.MEDIA_TYPE_NAME = :MEDIA_TYPE
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.vendor.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      TAPE.VENDOR = :VENDOR
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.logicalLibrary.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.tapePool.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.vo.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME = :VO
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.capacityInBytes.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      MEDIA_TYPE.CAPACITY_IN_BYTES = :CAPACITY_IN_BYTES
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.full.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      TAPE.IS_FULL = :IS_FULL
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.diskFileIds.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      VID IN (
        SELECT DISTINCT A.VID
        FROM
          TAPE_FILE A, ARCHIVE_FILE B
        WHERE
          A.ARCHIVE_FILE_ID = B.ARCHIVE_FILE_ID AND
          B.DISK_FILE_ID IN (
    )SQL";

    std::size_t diskFileIdsPerQuery = std::min(m_searchCriteria.diskFileIds->size(), MAX_DISK_FILE_ID_IN_QUERY);
    for (std::size_t i = 0; i < diskFileIdsPerQuery; ++i) {
      if (i > 0) {
        sql += R"SQL(, )SQL";
      }
      sql += R"SQL(:DISK_FID)SQL";
      sql += std::to_string(i);
    }
    sql += R"SQL()))SQL";

    addedAWhereConstraint = true;
  }

  if (m_searchCriteria.state.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      TAPE.TAPE_STATE = :TAPE_STATE
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.fromCastor.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      TAPE.IS_FROM_CASTOR = :FROM_CASTOR
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.purchaseOrder.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      TAPE.PURCHASE_ORDER = :PURCHASE_ORDER
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.physicalLibraryName.has_value()) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME
    )SQL";
    addedAWhereConstraint = true;
  }
  if (m_searchCriteria.checkMissingFileCopies.value_or(false)) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      VID IN (
        SELECT TF.VID FROM (
          SELECT AF.ARCHIVE_FILE_ID, SC.NB_COPIES, COUNT(TF.ARCHIVE_FILE_ID) AS NB_TAPE_COPIES
          FROM
            ARCHIVE_FILE AF
            INNER JOIN STORAGE_CLASS SC ON AF.STORAGE_CLASS_ID = SC.STORAGE_CLASS_ID
            INNER JOIN TAPE_FILE TF ON AF.ARCHIVE_FILE_ID = TF.ARCHIVE_FILE_ID
          WHERE
            SC.NB_COPIES > 1 AND AF.CREATION_TIME <= :MAX_CREATION_TIME
          GROUP BY
            AF.ARCHIVE_FILE_ID, SC.NB_COPIES
          HAVING
            SC.NB_COPIES <> COUNT(TF.ARCHIVE_FILE_ID)
        ) MISSING_COPIES
        INNER JOIN TAPE_FILE TF ON MISSING_COPIES.ARCHIVE_FILE_ID = TF.ARCHIVE_FILE_ID
      )
    )SQL";
  }

  sql += R"SQL( ORDER BY TAPE.VID )SQL";

  m_stmt = m_conn.createStmt(sql);

  if (m_searchCriteria.vid.has_value()) {
    m_stmt.bindString(":VID", m_searchCriteria.vid.value());
  }
  if (m_searchCriteria.mediaType.has_value()) {
    m_stmt.bindString(":MEDIA_TYPE", m_searchCriteria.mediaType.value());
  }
  if (m_searchCriteria.vendor.has_value()) {
    m_stmt.bindString(":VENDOR", m_searchCriteria.vendor.value());
  }
  if (m_searchCriteria.logicalLibrary.has_value()) {
    m_stmt.bindString(":LOGICAL_LIBRARY_NAME", m_searchCriteria.logicalLibrary.value());
  }
  if (m_searchCriteria.tapePool.has_value()) {
    m_stmt.bindString(":TAPE_POOL_NAME", m_searchCriteria.tapePool.value());
  }
  if (m_searchCriteria.vo.has_value()) {
    m_stmt.bindString(":VO", m_searchCriteria.vo.value());
  }
  if (m_searchCriteria.capacityInBytes.has_value()) {
    m_stmt.bindUint64(":CAPACITY_IN_BYTES", m_searchCriteria.capacityInBytes.value());
  }
  if (m_searchCriteria.full.has_value()) {
    m_stmt.bindBool(":IS_FULL", m_searchCriteria.full.value());
  }
  if (m_searchCriteria.fromCastor.has_value()) {
    m_stmt.bindBool(":FROM_CASTOR", m_searchCriteria.fromCastor.value());
  }
  if (m_searchCriteria.purchaseOrder.has_value()) {
    m_stmt.bindString(":PURCHASE_ORDER", m_searchCriteria.purchaseOrder.value());
  }
  if (m_searchCriteria.physicalLibraryName.has_value()) {
    m_stmt.bindString(":PHYSICAL_LIBRARY_NAME", m_searchCriteria.physicalLibraryName.value());
  }
  try {
    if (m_searchCriteria.state) {
      m_stmt.bindString(":TAPE_STATE",
                        cta::common::dataStructures::Tape::stateToString(m_searchCriteria.state.value()));
    }
  } catch (cta::exception::Exception&) {
    throw cta::exception::UserError(std::string("The state provided does not exist. Possible values are: ")
                                    + cta::common::dataStructures::Tape::getAllPossibleStates());
  }
  if (m_searchCriteria.checkMissingFileCopies.value_or(false)) {
    uint64_t max_creation_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    max_creation_time -= m_searchCriteria.missingFileCopiesMinAgeSecs;
    m_stmt.bindUint64(":MAX_CREATION_TIME", max_creation_time);
  }

  if (m_searchCriteria.diskFileIds.has_value()) {
    std::size_t diskFileIdsPerQuery = std::min(m_searchCriteria.diskFileIds->size(), MAX_DISK_FILE_ID_IN_QUERY);
    for (std::size_t i = 0; i < diskFileIdsPerQuery; ++i) {
      m_stmt.bindString(":DISK_FID" + std::to_string(i), m_searchCriteria.diskFileIds->at(i));
    }
    m_diskFileIdIdx = diskFileIdsPerQuery;
  }

  m_rset = m_stmt.executeQuery();
  m_rsetIsEmpty = !nextValidRset();
  if (m_rsetIsEmpty) {
    releaseDbResources();
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RdbmsCatalogueGetTapesItor::~RdbmsCatalogueGetTapesItor() {
  releaseDbResources();
}

//------------------------------------------------------------------------------
// releaseDbResources
//------------------------------------------------------------------------------
void RdbmsCatalogueGetTapesItor::releaseDbResources() noexcept {
  m_rset.reset();
  m_stmt.reset();
  m_conn.reset();
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool RdbmsCatalogueGetTapesItor::hasMore() {
  m_hasMoreHasBeenCalled = true;

  if (m_rsetIsEmpty) {
    return false;
  } else {
    return true;
  }
}

bool RdbmsCatalogueGetTapesItor::nextValidRset() {
  while (m_rset.next()) {
    if (m_splitByDiskFileId) {
      auto vid = m_rset.columnString("VID");
      if (!m_vidsInList.contains(vid)) {
        return true;
      }
      // Else, continue the loop
    } else {
      return true;
      // Return immediately
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
common::dataStructures::Tape RdbmsCatalogueGetTapesItor::next() {
  if (!m_hasMoreHasBeenCalled) {
    throw exception::Exception("hasMore() must be called before next()");
  }
  m_hasMoreHasBeenCalled = false;

  auto tape = populateTape(m_rset);
  m_rsetIsEmpty = !nextValidRset();

  if (m_splitByDiskFileId) {
    if (m_rsetIsEmpty && m_diskFileIdIdx < m_searchCriteria.diskFileIds->size()) {
      m_stmt.resetQuery();
      std::size_t i = 0;
      for (; i < MAX_DISK_FILE_ID_IN_QUERY && m_diskFileIdIdx < m_searchCriteria.diskFileIds->size();
           ++i, ++m_diskFileIdIdx) {
        m_stmt.bindString(":DISK_FID" + std::to_string(i), m_searchCriteria.diskFileIds->at(m_diskFileIdIdx));
      }
      // If we have reached the end of the list of disk file IDs, fill the remaining parameters with the same value
      for (; i < MAX_DISK_FILE_ID_IN_QUERY; ++i) {
        m_stmt.bindString(":DISK_FID" + std::to_string(i), m_searchCriteria.diskFileIds->back());
      }

      m_rset = m_stmt.executeQuery();
      m_rsetIsEmpty = !nextValidRset();
    }
    m_vidsInList.insert(tape.vid);
  }

  if (m_rsetIsEmpty) {
    releaseDbResources();
  }

  return tape;
}

}  // namespace cta::catalogue
