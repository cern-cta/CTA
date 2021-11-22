/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/PostgresCatalogue.hpp"
#include "catalogue/retryOnLostConnection.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/UserError.hpp"
#include "common/make_unique.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/rdbms.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"
#include "common/log/TimingList.hpp"
#include "InsertFileRecycleLog.hpp"
#include <algorithm>

namespace cta {
namespace catalogue {

namespace {
  /**
   * Structure used to assemble a batch of rows to insert into the TAPE_FILE
   * table.
   */
  struct TapeFileBatch {
    size_t nbRows;
    rdbms::wrapper::PostgresColumn vid;
    rdbms::wrapper::PostgresColumn fSeq;
    rdbms::wrapper::PostgresColumn blockId;
    rdbms::wrapper::PostgresColumn fileSize;
    rdbms::wrapper::PostgresColumn copyNb;
    rdbms::wrapper::PostgresColumn creationTime;
    rdbms::wrapper::PostgresColumn archiveFileId;

    /**
     * Constructor.
     *
     * @param nbRowsValue  The Number of rows to be inserted.
     */
    TapeFileBatch(const size_t nbRowsValue):
      nbRows(nbRowsValue),
      vid("VID", nbRows),
      fSeq("FSEQ", nbRows),
      blockId("BLOCK_ID", nbRows),
      fileSize("LOGICAL_SIZE_IN_BYTES", nbRows),
      copyNb("COPY_NB", nbRows),
      creationTime("CREATION_TIME", nbRows),
      archiveFileId("ARCHIVE_FILE_ID", nbRows) {
    }
  }; // struct TapeFileBatch

  /**
   * Structure used to assemble a batch of rows to insert into the TEMP_ARCHIVE_FILE_BATCH
   * table.
   */
  struct ArchiveFileBatch {
    size_t nbRows;
    rdbms::wrapper::PostgresColumn archiveFileId;
    rdbms::wrapper::PostgresColumn diskInstance;
    rdbms::wrapper::PostgresColumn diskFileId;
    rdbms::wrapper::PostgresColumn diskFileUser;
    rdbms::wrapper::PostgresColumn diskFileGroup;
    rdbms::wrapper::PostgresColumn size;
    rdbms::wrapper::PostgresColumn checksumBlob;
    rdbms::wrapper::PostgresColumn checksumAdler32;
    rdbms::wrapper::PostgresColumn storageClassName;
    rdbms::wrapper::PostgresColumn creationTime;
    rdbms::wrapper::PostgresColumn reconciliationTime;

    /**
     * Constructor.
     *
     * @param nbRowsValue  The Number of rows to be inserted.
     */
    ArchiveFileBatch(const size_t nbRowsValue):
      nbRows(nbRowsValue),
      archiveFileId("ARCHIVE_FILE_ID", nbRows),
      diskInstance("DISK_INSTANCE_NAME", nbRows),
      diskFileId("DISK_FILE_ID", nbRows),
      diskFileUser("DISK_FILE_UID", nbRows),
      diskFileGroup("DISK_FILE_GID", nbRows),
      size("SIZE_IN_BYTES", nbRows),
      checksumBlob("CHECKSUM_BLOB", nbRows),
      checksumAdler32("CHECKSUM_ADLER32", nbRows),
      storageClassName("STORAGE_CLASS_NAME", nbRows),
      creationTime("CREATION_TIME", nbRows),
      reconciliationTime("RECONCILIATION_TIME", nbRows) {
    }
  }; // struct ArchiveFileBatch

  /**
   * Structure used to assemble a batch of rows to insert into the
   * TAPE_FILE_BATCH temporary table.
   */
  struct TempTapeFileBatch {
    size_t nbRows;
    rdbms::wrapper::PostgresColumn archiveFileId;

    /**
     * Constructor.
     *
     * @param nbRowsValue  The Number of rows to be inserted.
     */
    TempTapeFileBatch(const size_t nbRowsValue):
      nbRows(nbRowsValue),
      archiveFileId("ARCHIVE_FILE_ID", nbRows) {
    }
  }; // struct TempTapeFileBatch
} // anonymous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresCatalogue::PostgresCatalogue(
  log::Logger &log,
  const rdbms::Login &login,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns):
  RdbmsCatalogue(
    log,
    rdbms::Login(rdbms::Login::DBTYPE_POSTGRESQL,
                 login.username, login.password, login.database,
                 login.hostname, login.port),
    nbConns,
    nbArchiveFileListingConns) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
PostgresCatalogue::~PostgresCatalogue() {
}

//------------------------------------------------------------------------------
// createAndPopulateTempTableFxid
//------------------------------------------------------------------------------
std::string PostgresCatalogue::createAndPopulateTempTableFxid(rdbms::Conn &conn, const optional<std::vector<std::string>> &diskFileIds) const {
  const std::string tempTableName = "TEMP_DISK_FXIDS";

  if(diskFileIds) {
    try {
      std::string sql = "CREATE TEMPORARY TABLE " + tempTableName + "(DISK_FILE_ID VARCHAR(100))";
      try {
        conn.executeNonQuery(sql);
      } catch(exception::Exception &ex) {
        // Postgres does not drop temporary tables until the end of the session; trying to create another
        // temporary table in the same unit test will fail. If this happens, truncate the table and carry on.
        sql = "TRUNCATE TABLE " + tempTableName;
        conn.executeNonQuery(sql);
      }

      sql = "INSERT INTO " + tempTableName + " VALUES(:DISK_FILE_ID)";
      auto stmt = conn.createStmt(sql);
      for(auto &diskFileId : diskFileIds.value()) {
        stmt.bindString(":DISK_FILE_ID", diskFileId);
        stmt.executeNonQuery();
      }
    } catch(exception::Exception &ex) {
      ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
      throw;
    }
  }
  return tempTableName;
}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t PostgresCatalogue::getNextArchiveFileId(rdbms::Conn &conn) {
  try {
    const char *const sql =
      "select NEXTVAL('ARCHIVE_FILE_ID_SEQ') AS ARCHIVE_FILE_ID";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set is unexpectedly empty");
    }
    return rset.columnUint64("ARCHIVE_FILE_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextLogicalLibraryId
//------------------------------------------------------------------------------
uint64_t PostgresCatalogue::getNextLogicalLibraryId(rdbms::Conn &conn) {
  try {
    const char *const sql =
      "select NEXTVAL('LOGICAL_LIBRARY_ID_SEQ') AS LOGICAL_LIBRARY_ID";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set is unexpectedly empty");
    }
    return rset.columnUint64("LOGICAL_LIBRARY_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextVirtualOrganizationId
//------------------------------------------------------------------------------
uint64_t PostgresCatalogue::getNextVirtualOrganizationId(rdbms::Conn &conn) {
  try {
    const char *const sql =
      "select NEXTVAL('VIRTUAL_ORGANIZATION_ID_SEQ') AS VIRTUAL_ORGANIZATION_ID";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set is unexpectedly empty");
    }
    return rset.columnUint64("VIRTUAL_ORGANIZATION_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextMediaTypeId
//------------------------------------------------------------------------------
uint64_t PostgresCatalogue::getNextMediaTypeId(rdbms::Conn &conn) {
  try {
    const char *const sql = "select NEXTVAL('MEDIA_TYPE_ID_SEQ') AS MEDIA_TYPE_ID";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set is unexpectedly empty");
    }
    return rset.columnUint64("MEDIA_TYPE_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextStorageClassId
//------------------------------------------------------------------------------
uint64_t PostgresCatalogue::getNextStorageClassId(rdbms::Conn &conn) {
  try {
    const char *const sql =
      "select NEXTVAL('STORAGE_CLASS_ID_SEQ') AS STORAGE_CLASS_ID";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set is unexpectedly empty");
    }
    return rset.columnUint64("STORAGE_CLASS_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextTapePoolId
//------------------------------------------------------------------------------
uint64_t PostgresCatalogue::getNextTapePoolId(rdbms::Conn &conn) {
  try {
    const char *const sql =
      "select NEXTVAL('TAPE_POOL_ID_SEQ') AS TAPE_POOL_ID";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set is unexpectedly empty");
    }
    return rset.columnUint64("TAPE_POOL_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextFileRecyleLogId
//------------------------------------------------------------------------------
uint64_t PostgresCatalogue::getNextFileRecyleLogId(rdbms::Conn &conn) {
  try {
    const char *const sql =
      "select NEXTVAL('FILE_RECYCLE_LOG_ID_SEQ') AS FILE_RECYCLE_LOG_ID";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set is unexpectedly empty");
    }
    return rset.columnUint64("FILE_RECYCLE_LOG_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// selectTapeForUpdateAndGetNextFSeq
//------------------------------------------------------------------------------
uint64_t PostgresCatalogue::selectTapeForUpdateAndGetLastFSeq(rdbms::Conn &conn, const std::string &vid) const {
  try {
    const char *const sql =
      "SELECT "
        "LAST_FSEQ AS LAST_FSEQ "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :VID "
      "FOR UPDATE";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    if (!rset.next()) {
      throw exception::Exception(std::string("The tape with VID " + vid + " does not exist"));
    }

    return rset.columnUint64("LAST_FSEQ");
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// filesWrittenToTape
//------------------------------------------------------------------------------
void PostgresCatalogue::filesWrittenToTape(const std::set<TapeItemWrittenPointer> &events) {
  try {
    if (events.empty()) {
      return;
    }

    auto firstEventItor = events.begin();
    const auto &firstEvent = **firstEventItor;
    checkTapeItemWrittenFieldsAreSet(__FUNCTION__, firstEvent);
    const time_t now = time(nullptr);
    auto conn = m_connPool.getConn();
    rdbms::AutoRollback autoRollback(conn);

    // Start DB transaction and create temporary tables TEMP_ARCHIVE_FILE_BATCH and TEMP_TAPE_FILE_BATCH.
    // These two tables will exist only for the duration of the transaction.
    // Set deferrable for second (disk instance, disk file id) constraint of the ARCHIVE_FILE table
    // to avoid violation in the case of concurrent inserts of a previously not existing archive file.
    beginCreateTemporarySetDeferred(conn);

    const uint64_t lastFSeq = selectTapeForUpdateAndGetLastFSeq(conn, firstEvent.vid);
    uint64_t expectedFSeq = lastFSeq + 1;
    uint64_t totalLogicalBytesWritten = 0;

    // We have a mix of files and items. Only files will be recorded, but items
    // allow checking fSeq coherency.
    // determine the number of files
    size_t filesCount=std::count_if(events.cbegin(), events.cend(),
        [](const TapeItemWrittenPointer &e) -> bool {return typeid(*e)==typeid(TapeFileWritten);});
    TapeFileBatch tapeFileBatch(filesCount);

    std::set<TapeFileWritten> fileEvents;

    for (const auto &eventP: events) {
      // Check for all item types.
      const auto &event = *eventP;
      checkTapeItemWrittenFieldsAreSet(__FUNCTION__, event);

      if (event.vid != firstEvent.vid) {
        throw exception::Exception(std::string("VID mismatch: expected=") + firstEvent.vid + " actual=" + event.vid);
      }

      if (expectedFSeq != event.fSeq) {
        exception::TapeFseqMismatch ex;
        ex.getMessage() << "FSeq mismatch for tape " << firstEvent.vid << ": expected=" << expectedFSeq << " actual=" <<
          event.fSeq;
        throw ex;
      }
      expectedFSeq++;

      try {
        // If this is a file (as opposed to a placeholder), do the full processing.
        const auto &fileEvent=dynamic_cast<const TapeFileWritten &>(event);

        checkTapeFileWrittenFieldsAreSet(__FUNCTION__, fileEvent);

        totalLogicalBytesWritten += fileEvent.size;

        fileEvents.insert(fileEvent);
      } catch (std::bad_cast&) {}
    }

    // Update the tape because all the necessary information is now available
    auto lastEventItor = events.cend();
    lastEventItor--;
    const TapeItemWritten &lastEvent = **lastEventItor;
    updateTape(conn, lastEvent.vid, lastEvent.fSeq, totalLogicalBytesWritten,
      lastEvent.tapeDrive);

    // If we had only placeholders and no file recorded, we are done (but we still commit the update of the tape's fSeq).
    if (fileEvents.empty()) {
      conn.commit();
      return;
    }

    // Create the archive file entries, skipping those that already exist
    // However we don't currently lock existing rows, so this transaction may
    // still fail later, in the face of certain concurrent modifications such
    // as the deletion of one of the existing archive files for which we are
    // inserting another tape file.
    idempotentBatchInsertArchiveFiles(conn, fileEvents);

    insertTapeFileBatchIntoTempTable(conn, fileEvents);

    // Verify that the archive file entries in the catalogue database agree with
    // the tape file written events
    const auto fileSizesAndChecksums = selectArchiveFileSizesAndChecksums(conn, fileEvents);
    for (const auto &event: fileEvents) {
      const auto fileSizeAndChecksumItor = fileSizesAndChecksums.find(event.archiveFileId);

      std::ostringstream fileContext;
      fileContext << "archiveFileId=" << event.archiveFileId << ", diskInstanceName=" << event.diskInstance <<
        ", diskFileId=" << event.diskFileId;

      // This should never happen
      if(fileSizesAndChecksums.end() == fileSizeAndChecksumItor) {
        exception::Exception ex;
        ex.getMessage() << __FUNCTION__ << ": Failed to find archive file entry in the catalogue: " << fileContext.str();
        throw ex;
      }

      const auto &fileSizeAndChecksum = fileSizeAndChecksumItor->second;

      if(fileSizeAndChecksum.fileSize != event.size) {
        catalogue::FileSizeMismatch ex;
        ex.getMessage() << __FUNCTION__ << ": File size mismatch: expected=" << fileSizeAndChecksum.fileSize <<
          ", actual=" << event.size << ": " << fileContext.str();
        throw ex;
      }

      fileSizeAndChecksum.checksumBlob.validate(event.checksumBlob);
    }

    // Store the value of each field
    uint32_t i = 0;
    for (const auto &event: fileEvents) {
      tapeFileBatch.vid.setFieldValue(i, event.vid);
      tapeFileBatch.fSeq.setFieldValue(i, event.fSeq);
      tapeFileBatch.blockId.setFieldValue(i, event.blockId);
      tapeFileBatch.fileSize.setFieldValue(i, event.size);
      tapeFileBatch.copyNb.setFieldValue(i, event.copyNb);
      tapeFileBatch.creationTime.setFieldValue(i, now);
      tapeFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
      i++;
    }

    const char *const sql =
    "CREATE TEMPORARY TABLE TEMP_TAPE_FILE_INSERTION_BATCH ("                        "\n"
      "LIKE TAPE_FILE) "                                                             "\n"
      "ON COMMIT DROP;"                                                              "\n"
    "COPY TEMP_TAPE_FILE_INSERTION_BATCH("                                           "\n"
      "VID,"                                                                         "\n"
      "FSEQ,"                                                                        "\n"
      "BLOCK_ID,"                                                                    "\n"
      "LOGICAL_SIZE_IN_BYTES,"                                                       "\n"
      "COPY_NB,"                                                                     "\n"
      "CREATION_TIME,"                                                               "\n"
      "ARCHIVE_FILE_ID) "                                                            "\n"
    "FROM STDIN; --"                                                                 "\n"
      "-- :VID,"                                                                     "\n"
      "-- :FSEQ,"                                                                    "\n"
      "-- :BLOCK_ID,"                                                                "\n"
      "-- :LOGICAL_SIZE_IN_BYTES,"                                                   "\n"
      "-- :COPY_NB,"                                                                 "\n"
      "-- :CREATION_TIME,"                                                           "\n"
      "-- :ARCHIVE_FILE_ID;"                                                         "\n";

    auto stmt = conn.createStmt(sql);
    rdbms::wrapper::PostgresStmt &postgresStmt = dynamic_cast<rdbms::wrapper::PostgresStmt &>(stmt.getStmt());
    postgresStmt.setColumn(tapeFileBatch.vid);
    postgresStmt.setColumn(tapeFileBatch.fSeq);
    postgresStmt.setColumn(tapeFileBatch.blockId);
    postgresStmt.setColumn(tapeFileBatch.fileSize);
    postgresStmt.setColumn(tapeFileBatch.copyNb);
    postgresStmt.setColumn(tapeFileBatch.creationTime);
    postgresStmt.setColumn(tapeFileBatch.archiveFileId);

    postgresStmt.executeCopyInsert(tapeFileBatch.nbRows);

    auto recycledFiles = insertOldCopiesOfFilesIfAnyOnFileRecycleLog(conn);

    {
      //Insert the tapefiles from the TEMP_TAPE_FILE_INSERTION_BATCH
      const char * const insertTapeFileSql =
      "INSERT INTO TAPE_FILE (VID, FSEQ, BLOCK_ID, LOGICAL_SIZE_IN_BYTES,"            "\n"
      "COPY_NB, CREATION_TIME, ARCHIVE_FILE_ID) "                                     "\n"
      "SELECT VID, FSEQ, BLOCK_ID, LOGICAL_SIZE_IN_BYTES,"                            "\n"
      "COPY_NB, CREATION_TIME, ARCHIVE_FILE_ID FROM TEMP_TAPE_FILE_INSERTION_BATCH;"  "\n";
      conn.executeNonQuery(insertTapeFileSql);
    }

    for(auto & recycledFile: recycledFiles){
      const char * const deleteTapeFileSql =
      "DELETE FROM TAPE_FILE WHERE TAPE_FILE.VID = :VID AND TAPE_FILE.FSEQ = :FSEQ";
      auto deleteTapeFileStmt = conn.createStmt(deleteTapeFileSql);
      deleteTapeFileStmt.bindString(":VID",recycledFile.vid);
      deleteTapeFileStmt.bindUint64(":FSEQ",recycledFile.fSeq);
      deleteTapeFileStmt.executeNonQuery();
    }

    autoRollback.cancel();
    conn.commit();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// idempotentBatchInsertArchiveFiles
//------------------------------------------------------------------------------
void PostgresCatalogue::idempotentBatchInsertArchiveFiles(rdbms::Conn &conn,
   const std::set<TapeFileWritten> &events) const {
  try {
    ArchiveFileBatch archiveFileBatch(events.size());
    const time_t now = time(nullptr);

    // Store the value of each field
    uint32_t i = 0;
    for (const auto &event: events) {
      archiveFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
      archiveFileBatch.diskInstance.setFieldValue(i, event.diskInstance);
      archiveFileBatch.diskFileId.setFieldValue(i, event.diskFileId);
      archiveFileBatch.diskFileUser.setFieldValue(i, event.diskFileOwnerUid);
      archiveFileBatch.diskFileGroup.setFieldValue(i, event.diskFileGid);
      archiveFileBatch.size.setFieldValue(i, event.size);
      archiveFileBatch.checksumBlob.setFieldByteA(conn, i, event.checksumBlob.serialize());
      // Keep transition ADLER32 checksum up-to-date if it exists
      std::string adler32str;
      try {
        std::string adler32hex = checksum::ChecksumBlob::ByteArrayToHex(event.checksumBlob.at(checksum::ADLER32));
        uint32_t adler32 = strtoul(adler32hex.c_str(), 0, 16);
        adler32str = std::to_string(adler32);
      } catch(exception::ChecksumTypeMismatch &ex) {
        adler32str = "0";
      }
      archiveFileBatch.checksumAdler32.setFieldValue(i, adler32str);
      archiveFileBatch.storageClassName.setFieldValue(i, event.storageClassName);
      archiveFileBatch.creationTime.setFieldValue(i, now);
      archiveFileBatch.reconciliationTime.setFieldValue(i, now);
      i++;
    }

    const char *const sql =
      "COPY TEMP_ARCHIVE_FILE_BATCH("
        "ARCHIVE_FILE_ID,"
        "DISK_INSTANCE_NAME,"
        "DISK_FILE_ID,"
        "DISK_FILE_UID,"
        "DISK_FILE_GID,"
        "SIZE_IN_BYTES,"
        "CHECKSUM_BLOB,"
        "CHECKSUM_ADLER32,"
        "STORAGE_CLASS_NAME,"
        "CREATION_TIME,"
        "RECONCILIATION_TIME) "
      "FROM STDIN --"
        ":ARCHIVE_FILE_ID,"
        ":DISK_INSTANCE_NAME,"
        ":DISK_FILE_ID,"
        ":DISK_FILE_UID,"
        ":DISK_FILE_GID,"
        ":SIZE_IN_BYTES,"
        ":CHECKSUM_BLOB,"
        ":CHECKSUM_ADLER32,"
        ":STORAGE_CLASS_NAME,"
        ":CREATION_TIME,"
        ":RECONCILIATION_TIME";

    auto stmt = conn.createStmt(sql);
    rdbms::wrapper::PostgresStmt &postgresStmt = dynamic_cast<rdbms::wrapper::PostgresStmt &>(stmt.getStmt());

    postgresStmt.setColumn(archiveFileBatch.archiveFileId);
    postgresStmt.setColumn(archiveFileBatch.diskInstance);
    postgresStmt.setColumn(archiveFileBatch.diskFileId);
    postgresStmt.setColumn(archiveFileBatch.diskFileUser);
    postgresStmt.setColumn(archiveFileBatch.diskFileGroup);
    postgresStmt.setColumn(archiveFileBatch.size);
    postgresStmt.setColumn(archiveFileBatch.checksumBlob);
    postgresStmt.setColumn(archiveFileBatch.checksumAdler32);
    postgresStmt.setColumn(archiveFileBatch.storageClassName);
    postgresStmt.setColumn(archiveFileBatch.creationTime);
    postgresStmt.setColumn(archiveFileBatch.reconciliationTime);

    postgresStmt.executeCopyInsert(archiveFileBatch.nbRows);

    const char *const sql_insert =
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
        "RECONCILIATION_TIME) "
      "SELECT "
        "A.ARCHIVE_FILE_ID,"
        "A.DISK_INSTANCE_NAME,"
        "A.DISK_FILE_ID,"
        "A.DISK_FILE_UID,"
        "A.DISK_FILE_GID,"
        "A.SIZE_IN_BYTES,"
        "A.CHECKSUM_BLOB,"
        "A.CHECKSUM_ADLER32,"
        "S.STORAGE_CLASS_ID,"
        "A.CREATION_TIME,"
        "A.RECONCILIATION_TIME "
      "FROM TEMP_ARCHIVE_FILE_BATCH AS A, STORAGE_CLASS AS S "
        "WHERE A.STORAGE_CLASS_NAME = S.STORAGE_CLASS_NAME "
      "ORDER BY A.ARCHIVE_FILE_ID "
      "ON CONFLICT (ARCHIVE_FILE_ID) DO NOTHING";

    // Concerns for bulk insertion in archive_file: deadlock with concurrent
    // inserts of previously not-existing entry for the same archive file,
    // hence insert with ORDER BY to define an update order.

    auto stmt_insert = conn.createStmt(sql_insert);
    stmt_insert.executeNonQuery();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<cta::catalogue::InsertFileRecycleLog> PostgresCatalogue::insertOldCopiesOfFilesIfAnyOnFileRecycleLog(rdbms::Conn& conn){
  std::list<cta::catalogue::InsertFileRecycleLog> fileRecycleLogsToInsert;
  try {
    //Get the TAPE_FILE entry to put on the file recycle log
    {
      const char *const sql =
        "SELECT "
          "TAPE_FILE.VID AS VID,"
          "TAPE_FILE.FSEQ AS FSEQ,"
          "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
          "TAPE_FILE.COPY_NB AS COPY_NB,"
          "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
          "TAPE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID "
        "FROM "
          "TAPE_FILE "
        "JOIN "
          "TEMP_TAPE_FILE_INSERTION_BATCH "
        "ON "
          "TEMP_TAPE_FILE_INSERTION_BATCH.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID AND TEMP_TAPE_FILE_INSERTION_BATCH.COPY_NB = TAPE_FILE.COPY_NB "
        "WHERE "
          "TAPE_FILE.VID != TEMP_TAPE_FILE_INSERTION_BATCH.VID OR TAPE_FILE.FSEQ != TEMP_TAPE_FILE_INSERTION_BATCH.FSEQ";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      while(rset.next()){
        cta::catalogue::InsertFileRecycleLog fileRecycleLog;
        fileRecycleLog.vid = rset.columnString("VID");
        fileRecycleLog.fSeq = rset.columnUint64("FSEQ");
        fileRecycleLog.blockId = rset.columnUint64("BLOCK_ID");
        fileRecycleLog.copyNb = rset.columnUint8("COPY_NB");
        fileRecycleLog.tapeFileCreationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        fileRecycleLog.archiveFileId = rset.columnUint64("ARCHIVE_FILE_ID");
        fileRecycleLog.reasonLog = InsertFileRecycleLog::getRepackReasonLog();
        fileRecycleLog.recycleLogTime = time(nullptr);
        fileRecycleLogsToInsert.push_back(fileRecycleLog);
      }
    }
    {
      for(auto & fileRecycleLog: fileRecycleLogsToInsert){
        insertFileInFileRecycleLog(conn,fileRecycleLog);
      }
      return fileRecycleLogsToInsert;
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// selectArchiveFileSizeAndChecksum
//------------------------------------------------------------------------------
std::map<uint64_t, PostgresCatalogue::FileSizeAndChecksum> PostgresCatalogue::selectArchiveFileSizesAndChecksums(
  rdbms::Conn &conn, const std::set<TapeFileWritten> &events) const {
  try {
    std::vector<uint64_t> archiveFileIdList(events.size());
    for (const auto &event: events) {
      archiveFileIdList.push_back(event.archiveFileId);
    }

    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32 "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN TEMP_TAPE_FILE_BATCH ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TEMP_TAPE_FILE_BATCH.ARCHIVE_FILE_ID";
    auto stmt = conn.createStmt(sql);

    auto rset = stmt.executeQuery();

    std::map<uint64_t, FileSizeAndChecksum> fileSizesAndChecksums;
    while (rset.next()) {
      const uint64_t archiveFileId = rset.columnUint64("ARCHIVE_FILE_ID");

      if (fileSizesAndChecksums.end() != fileSizesAndChecksums.find(archiveFileId)) {
        exception::Exception ex;
        ex.getMessage() << __FUNCTION__ << " failed: "
          "Found duplicate archive file identifier in batch of files written to tape: archiveFileId=" << archiveFileId;
        throw ex;
      }

      FileSizeAndChecksum fileSizeAndChecksum;
      fileSizeAndChecksum.fileSize = rset.columnUint64("SIZE_IN_BYTES");
      fileSizeAndChecksum.checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
      fileSizesAndChecksums[archiveFileId] = fileSizeAndChecksum;
    }

    return fileSizesAndChecksums;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// insertArchiveFilesIntoTempTable
//------------------------------------------------------------------------------
void PostgresCatalogue::insertTapeFileBatchIntoTempTable(rdbms::Conn &conn,
   const std::set<TapeFileWritten> &events) const {
  try {
    TempTapeFileBatch tempTapeFileBatch(events.size());

    // Store the value of each field
    uint32_t i = 0;
    for (const auto &event: events) {
      tempTapeFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
      i++;
    }

    const char *const sql =
      "COPY TEMP_TAPE_FILE_BATCH("
        "ARCHIVE_FILE_ID) "
      "FROM STDIN --"
        ":ARCHIVE_FILE_ID";

    auto stmt = conn.createStmt(sql);
    rdbms::wrapper::PostgresStmt &postgresStmt = dynamic_cast<rdbms::wrapper::PostgresStmt &>(stmt.getStmt());

    postgresStmt.setColumn(tempTapeFileBatch.archiveFileId);
    postgresStmt.executeCopyInsert(tempTapeFileBatch.nbRows);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteArchiveFile
//------------------------------------------------------------------------------
void PostgresCatalogue::DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName, const uint64_t archiveFileId,
  log::LogContext &lc) {
  try {
    const char *selectSql =
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
      "FOR UPDATE OF ARCHIVE_FILE";
    utils::Timer t;
    auto conn = m_connPool.getConn();
    rdbms::AutoRollback autoRollback(conn);
    conn.executeNonQuery("BEGIN");

    const auto getConnTime = t.secs(utils::Timer::resetCounter);
    auto selectStmt = conn.createStmt(selectSql);
    const auto createStmtTime = t.secs();
    selectStmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    t.reset();
    rdbms::Rset selectRset = selectStmt.executeQuery();
    const auto selectFromArchiveFileTime = t.secs();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    std::set<std::string> vidsToSetDirty;
    while(selectRset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = cta::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = selectRset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = selectRset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = selectRset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.owner_uid = selectRset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = selectRset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = selectRset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(selectRset.columnBlob("CHECKSUM_BLOB"), selectRset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = selectRset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = selectRset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = selectRset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!selectRset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = selectRset.columnString("VID");
        vidsToSetDirty.insert(tapeFile.vid);
        tapeFile.fSeq = selectRset.columnUint64("FSEQ");
        tapeFile.blockId = selectRset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = selectRset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = selectRset.columnUint64("COPY_NB");
        tapeFile.creationTime = selectRset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience

        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    if(nullptr == archiveFile.get()) {
      log::ScopedParamContainer spc(lc);
      spc.add("fileId", archiveFileId);
      lc.log(log::WARNING, "Ignoring request to delete archive file because it does not exist in the catalogue");
      return;
    }

    if(diskInstanceName != archiveFile->diskInstance) {
      log::ScopedParamContainer spc(lc);
      spc.add("fileId", std::to_string(archiveFile->archiveFileID))
         .add("diskInstance", archiveFile->diskInstance)
         .add("requestDiskInstance", diskInstanceName)
         .add("diskFileId", archiveFile->diskFileId)
         .add("diskFileInfo.owner_uid", archiveFile->diskFileInfo.owner_uid)
         .add("diskFileInfo.gid", archiveFile->diskFileInfo.gid)
         .add("fileSize", std::to_string(archiveFile->fileSize))
         .add("creationTime", std::to_string(archiveFile->creationTime))
         .add("reconciliationTime", std::to_string(archiveFile->reconciliationTime))
         .add("storageClass", archiveFile->storageClass)
         .add("getConnTime", getConnTime)
         .add("createStmtTime", createStmtTime)
         .add("selectFromArchiveFileTime", selectFromArchiveFileTime);
      archiveFile->checksumBlob.addFirstChecksumToLog(spc);
      for(auto it=archiveFile->tapeFiles.begin(); it!=archiveFile->tapeFiles.end(); it++) {
        std::stringstream tapeCopyLogStream;
        tapeCopyLogStream << "copy number: " << it->copyNb
          << " vid: " << it->vid
          << " fSeq: " << it->fSeq
          << " blockId: " << it->blockId
          << " creationTime: " << it->creationTime
          << " fileSize: " << it->fileSize
          << " checksumBlob: " << it->checksumBlob //this shouldn't be here: repeated field
          << " copyNb: " << it->copyNb;
        spc.add("TAPE FILE", tapeCopyLogStream.str());
      }
      lc.log(log::WARNING, "Failed to delete archive file because the disk instance of the request does not match that "
        "of the archived file");

      exception::UserError ue;
      ue.getMessage() << "Failed to delete archive file with ID " << archiveFileId << " because the disk instance of "
        "the request does not match that of the archived file: archiveFileId=" << archiveFileId << " requestDiskInstance=" << diskInstanceName << " archiveFileDiskInstance=" <<
        archiveFile->diskInstance;
      throw ue;
    }

    t.reset();
    {
      const char *const sql = "DELETE FROM TAPE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt.executeNonQuery();
    }

    const auto deleteFromTapeFileTime = t.secs(utils::Timer::resetCounter);

    for(auto &vidToSetDirty: vidsToSetDirty){
      //We deleted the TAPE_FILE so the tapes containing them should be set as dirty
      setTapeDirty(conn,vidToSetDirty);
    }

    const auto setTapeDirtyTime = t.secs(utils::Timer::resetCounter);

    {
      const char *const sql = "DELETE FROM ARCHIVE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt.executeNonQuery();
    }
    const auto deleteFromArchiveFileTime = t.secs(utils::Timer::resetCounter);

    conn.commit();
    autoRollback.cancel();
    const auto commitTime = t.secs();

    log::ScopedParamContainer spc(lc);
    spc.add("fileId", std::to_string(archiveFile->archiveFileID))
       .add("diskInstance", archiveFile->diskInstance)
       .add("diskFileId", archiveFile->diskFileId)
       .add("diskFileInfo.owner_uid", archiveFile->diskFileInfo.owner_uid)
       .add("diskFileInfo.gid", archiveFile->diskFileInfo.gid)
       .add("fileSize", std::to_string(archiveFile->fileSize))
       .add("creationTime", std::to_string(archiveFile->creationTime))
       .add("reconciliationTime", std::to_string(archiveFile->reconciliationTime))
       .add("storageClass", archiveFile->storageClass)
       .add("getConnTime", getConnTime)
       .add("createStmtTime", createStmtTime)
       .add("selectFromArchiveFileTime", selectFromArchiveFileTime)
       .add("deleteFromTapeFileTime", deleteFromTapeFileTime)
       .add("deleteFromArchiveFileTime", deleteFromArchiveFileTime)
       .add("setTapeDirtyTime",setTapeDirtyTime)
       .add("commitTime", commitTime);
    archiveFile->checksumBlob.addFirstChecksumToLog(spc);
    for(auto it=archiveFile->tapeFiles.begin(); it!=archiveFile->tapeFiles.end(); it++) {
      std::stringstream tapeCopyLogStream;
      tapeCopyLogStream << "copy number: " << it->copyNb
        << " vid: " << it->vid
        << " fSeq: " << it->fSeq
        << " blockId: " << it->blockId
        << " creationTime: " << it->creationTime
        << " fileSize: " << it->fileSize
        << " checksumBlob: " << it->checksumBlob //this shouldn't be here: repeated field
        << " copyNb: " << static_cast<int>(it->copyNb); //this shouldn't be here: repeated field
      spc.add("TAPE FILE", tapeCopyLogStream.str());
    }
    lc.log(log::INFO, "Archive file deleted from CTA catalogue");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// beginCreateTemporarySetDeferred
//------------------------------------------------------------------------------
void PostgresCatalogue::beginCreateTemporarySetDeferred(rdbms::Conn &conn) const {
  conn.executeNonQuery("BEGIN");
  conn.executeNonQuery("CREATE TEMPORARY TABLE TEMP_ARCHIVE_FILE_BATCH (LIKE ARCHIVE_FILE) ON COMMIT DROP");
  conn.executeNonQuery("ALTER TABLE TEMP_ARCHIVE_FILE_BATCH ADD COLUMN STORAGE_CLASS_NAME VARCHAR(100)");
  conn.executeNonQuery("ALTER TABLE TEMP_ARCHIVE_FILE_BATCH ALTER COLUMN STORAGE_CLASS_ID DROP NOT NULL");
  conn.executeNonQuery("ALTER TABLE TEMP_ARCHIVE_FILE_BATCH ALTER COLUMN IS_DELETED DROP NOT NULL");
  conn.executeNonQuery("CREATE INDEX TEMP_A_F_B_ARCHIVE_FILE_ID_I ON TEMP_ARCHIVE_FILE_BATCH(ARCHIVE_FILE_ID)");
  conn.executeNonQuery("CREATE INDEX TEMP_A_F_B_DIN_SCN_I ON TEMP_ARCHIVE_FILE_BATCH(DISK_INSTANCE_NAME, STORAGE_CLASS_NAME)");
  conn.executeNonQuery("CREATE TEMPORARY TABLE TEMP_TAPE_FILE_BATCH(ARCHIVE_FILE_ID NUMERIC(20,0)) ON COMMIT DROP");
  conn.executeNonQuery("CREATE INDEX TEMP_T_F_B_ARCHIVE_FILE_ID_I ON TEMP_TAPE_FILE_BATCH(ARCHIVE_FILE_ID)");
  conn.executeNonQuery("SET CONSTRAINTS ARCHIVE_FILE_DIN_DFI_UN DEFERRED");
}

//------------------------------------------------------------------------------
// copyArchiveFileToRecycleBinAndDelete
//------------------------------------------------------------------------------
void PostgresCatalogue::copyArchiveFileToFileRecyleLogAndDelete(rdbms::Conn & conn, const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do an INSERT INTO and a DELETE FROM
    //in a single transaction
    conn.executeNonQuery("BEGIN");
    copyArchiveFileToFileRecycleLog(conn,request);
    tl.insertAndReset("insertToRecycleBinTime",t);
    setTapeDirty(conn,request.archiveFileID);
    tl.insertAndReset("setTapeDirtyTime",t);
    deleteTapeFiles(conn,request);
    tl.insertAndReset("deleteTapeFilesTime",t);
    RdbmsCatalogue::deleteArchiveFile(conn,request);
    tl.insertAndReset("deleteArchiveFileTime",t);
    conn.commit();
    tl.insertAndReset("commitTime",t);
    log::ScopedParamContainer spc(lc);
    spc.add("archiveFileId",request.archiveFileID);
    spc.add("diskFileId",request.diskFileId);
    spc.add("diskFilePath",request.diskFilePath);
    spc.add("diskInstance",request.diskInstance);
    tl.addToLog(spc);
    lc.log(log::INFO,"In PostgresCatalogue::copyArchiveFileToRecycleBinAndDelete: ArchiveFile moved to the recycle-bin.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteTapeFilesAndArchiveFileFromRecycleBin
//------------------------------------------------------------------------------
void PostgresCatalogue::deleteTapeFilesAndArchiveFileFromRecycleBin(rdbms::Conn& conn, const uint64_t archiveFileId, log::LogContext& lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do two delete in one transaction
    conn.executeNonQuery("BEGIN");
    deleteTapeFilesFromRecycleBin(conn,archiveFileId);
    tl.insertAndReset("deleteTapeFilesTime",t);
    deleteArchiveFileFromRecycleBin(conn,archiveFileId);
    tl.insertAndReset("deleteArchiveFileTime",t);
    conn.commit();
    tl.insertAndReset("commitTime",t);
    log::ScopedParamContainer spc(lc);
    spc.add("archiveFileId",archiveFileId);
    tl.addToLog(spc);
    lc.log(log::INFO,"In PostgresCatalogue::deleteTapeFilesAndArchiveFileFromRecycleBin: tape files and archiveFiles deleted from the recycle-bin.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// copyTapeFileToFileRecyleLogAndDelete
//------------------------------------------------------------------------------
void PostgresCatalogue::copyTapeFileToFileRecyleLogAndDelete(rdbms::Conn & conn, const cta::common::dataStructures::ArchiveFile &file,
                                                            const std::string &reason, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do an INSERT INTO and a DELETE FROM
    //in a single transaction
    conn.executeNonQuery("BEGIN");
    copyTapeFilesToFileRecycleLog(conn, file, reason);
    tl.insertAndReset("insertToRecycleBinTime",t);
    setTapeDirty(conn, file.archiveFileID);
    tl.insertAndReset("setTapeDirtyTime",t);
    deleteTapeFiles(conn,file);
    tl.insertAndReset("deleteTapeFilesTime",t);
    conn.commit();
    tl.insertAndReset("commitTime",t);
    log::ScopedParamContainer spc(lc);
    spc.add("archiveFileId", file.archiveFileID);
    spc.add("diskFileId", file.diskFileId);
    spc.add("diskFilePath", file.diskFileInfo.path);
    spc.add("diskInstance", file.diskInstance);
    tl.addToLog(spc);
    lc.log(log::INFO,"In PostgresCatalogue::copyArchiveFileToRecycleBinAndDelete: ArchiveFile moved to the recycle-bin.");

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// restoreEntryInRecycleLog
//------------------------------------------------------------------------------
void PostgresCatalogue::restoreEntryInRecycleLog(rdbms::Conn & conn, FileRecycleLogItor &fileRecycleLogItor,
  const std::string &newFid, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;

    if (!fileRecycleLogItor.hasMore()) {
      throw cta::exception::UserError("No file in the recycle bin matches the parameters passed");
    }
    auto fileRecycleLog = fileRecycleLogItor.next();
    if (fileRecycleLogItor.hasMore()) {
      //stop restoring more than one file at once
      throw cta::exception::UserError("More than one recycle bin file matches the parameters passed");
    }

    //We currently do all file copies restoring in a single transaction
    conn.executeNonQuery("BEGIN");
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFilePtr = getArchiveFileById(conn, fileRecycleLog.archiveFileId);
    if (!archiveFilePtr) {
      restoreArchiveFileInRecycleLog(conn, fileRecycleLog, newFid, lc);
    } else {
      if (archiveFilePtr->tapeFiles.find(fileRecycleLog.copyNb) != archiveFilePtr->tapeFiles.end()) {
        //copy with same copy_nb exists, cannot restore
        UserSpecifiedExistingDeletedFileCopy ex;
        ex.getMessage() << "Cannot restore file copy with archiveFileId " << std::to_string(fileRecycleLog.archiveFileId)
        << " and copy_nb " << std::to_string(fileRecycleLog.copyNb) << " because a tapefile with same archiveFileId and copy_nb already exists";
        throw ex;
      }
    }

    restoreFileCopyInRecycleLog(conn, fileRecycleLog, lc);
    conn.commit();

    log::ScopedParamContainer spc(lc);
    tl.insertAndReset("commitTime",t);
    tl.addToLog(spc);
    lc.log(log::INFO,"In PostgresCatalogue::restoreEntryInRecycleLog: all file copies successfully restored.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// restoreFileCopyInRecycleLog
//------------------------------------------------------------------------------
void PostgresCatalogue::restoreFileCopyInRecycleLog(rdbms::Conn & conn, const common::dataStructures::FileRecycleLog &fileRecycleLog, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    cta::common::dataStructures::TapeFile tapeFile;
    tapeFile.vid = fileRecycleLog.vid;
    tapeFile.fSeq = fileRecycleLog.fSeq;
    tapeFile.copyNb = fileRecycleLog.copyNb;
    tapeFile.blockId = fileRecycleLog.blockId;
    tapeFile.fileSize = fileRecycleLog.sizeInBytes;
    tapeFile.creationTime = fileRecycleLog.tapeFileCreationTime;

    insertTapeFile(conn, tapeFile, fileRecycleLog.archiveFileId);
    tl.insertAndReset("insertTapeFileTime",t);

    deleteTapeFileCopyFromRecycleBin(conn, fileRecycleLog);
    tl.insertAndReset("deleteTapeFileCopyFromRecycleBinTime",t);

    log::ScopedParamContainer spc(lc);
    spc.add("vid", tapeFile.vid);
    spc.add("archiveFileId", fileRecycleLog.archiveFileId);
    spc.add("fSeq", tapeFile.fSeq);
    spc.add("copyNb", tapeFile.copyNb);
    spc.add("fileSize", tapeFile.fileSize);
    tl.addToLog(spc);
    lc.log(log::INFO,"In PostgresCatalogue::restoreFileCopyInRecycleLog: File restored from the recycle log.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


} // namespace catalogue
} // namespace cta
