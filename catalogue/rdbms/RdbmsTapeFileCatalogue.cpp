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

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "catalogue/InsertFileRecycleLog.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsMountPolicyCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsTapeFileCatalogue::RdbmsTapeFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {
}

void RdbmsTapeFileCatalogue::deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) {
  log::LogContext lc(m_log);
  auto conn = m_connPool->getConn();
  copyTapeFileToFileRecyleLogAndDelete(conn, file, reason, lc);
}

void RdbmsTapeFileCatalogue::copyTapeFileToFileRecyleLogAndDelete(rdbms::Conn & conn,
  const cta::common::dataStructures::ArchiveFile &file, const std::string &reason, log::LogContext & lc) const {
  try {
    utils::Timer timer;
    log::TimingList timingList;
    copyTapeFileToFileRecyleLogAndDeleteTransaction(conn, file, reason, &timer, &timingList, lc);
    timingList.insertAndReset("commitTime", timer);
    log::ScopedParamContainer spc(lc);
    spc.add("archiveFileId", file.archiveFileID);
    spc.add("diskFileId", file.diskFileId);
    spc.add("diskFilePath", file.diskFileInfo.path);
    spc.add("diskInstance", file.diskInstance);
    timingList.addToLog(spc);
    lc.log(log::INFO, "In RdbmsFileRecycleLogCatalogue::copyArchiveFileToRecycleBinAndDelete: "
      "ArchiveFile moved to the recycle-bin.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeFileCatalogue::checkTapeItemWrittenFieldsAreSet(const std::string& callingFunc,
  const TapeItemWritten& event) const {
  try {
    if(event.vid.empty()) throw exception::Exception("vid is an empty string");
    if(0 == event.fSeq) throw exception::Exception("fSeq is 0");
    if(event.tapeDrive.empty()) throw exception::Exception("tapeDrive is an empty string");
  } catch (exception::Exception &ex) {
    throw exception::Exception(callingFunc + " failed: TapeItemWrittenEvent is invalid: " + ex.getMessage().str());
  }
}

void RdbmsTapeFileCatalogue::checkTapeFileWrittenFieldsAreSet(const std::string &callingFunc,
  const TapeFileWritten &event)
  const {
  try {
    if(event.diskInstance.empty()) throw exception::Exception("diskInstance is an empty string");
    if(event.diskFileId.empty()) throw exception::Exception("diskFileId is an empty string");
    if(0 == event.diskFileOwnerUid) throw exception::Exception("diskFileOwnerUid is 0");
    if(0 == event.size) throw exception::Exception("size is 0");
    if(event.checksumBlob.length() == 0) throw exception::Exception("checksumBlob is an empty string");
    if(event.storageClassName.empty()) throw exception::Exception("storageClassName is an empty string");
    if(event.vid.empty()) throw exception::Exception("vid is an empty string");
    if(0 == event.fSeq) throw exception::Exception("fSeq is 0");
    if(0 == event.blockId && event.fSeq != 1) throw exception::Exception("blockId is 0 and fSeq is not 1");
    if(0 == event.copyNb) throw exception::Exception("copyNb is 0");
    if(event.tapeDrive.empty()) throw exception::Exception("tapeDrive is an empty string");
  } catch (exception::Exception &ex) {
    throw exception::Exception(callingFunc + " failed: TapeFileWrittenEvent is invalid: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// insertTapeFile
//------------------------------------------------------------------------------
void RdbmsTapeFileCatalogue::insertTapeFile(rdbms::Conn &conn, const common::dataStructures::TapeFile &tapeFile,
  const uint64_t archiveFileId) {
  rdbms::AutoRollback autoRollback(conn);
  try{
    const auto fileRecycleLogCatalogue = static_cast<RdbmsFileRecycleLogCatalogue*>(
      m_rdbmsCatalogue->FileRecycleLog().get());
    std::list<InsertFileRecycleLog> insertedFilesRecycleLog
      = fileRecycleLogCatalogue->insertOldCopiesOfFilesIfAnyOnFileRecycleLog(conn,tapeFile,archiveFileId);
    {
      const time_t now = time(nullptr);
      const char *const sql =
        "INSERT INTO TAPE_FILE("
          "VID,"
          "FSEQ,"
          "BLOCK_ID,"
          "LOGICAL_SIZE_IN_BYTES,"
          "COPY_NB,"
          "CREATION_TIME,"
          "ARCHIVE_FILE_ID)"
        "VALUES("
          ":VID,"
          ":FSEQ,"
          ":BLOCK_ID,"
          ":LOGICAL_SIZE_IN_BYTES,"
          ":COPY_NB,"
          ":CREATION_TIME,"
          ":ARCHIVE_FILE_ID)";
      auto stmt = conn.createStmt(sql);

      stmt.bindString(":VID", tapeFile.vid);
      stmt.bindUint64(":FSEQ", tapeFile.fSeq);
      stmt.bindUint64(":BLOCK_ID", tapeFile.blockId);
      stmt.bindUint64(":LOGICAL_SIZE_IN_BYTES", tapeFile.fileSize);
      stmt.bindUint64(":COPY_NB", tapeFile.copyNb);
      stmt.bindUint64(":CREATION_TIME", now);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt.executeNonQuery();
    }
    {
      for(auto& fileRecycleLog: insertedFilesRecycleLog){
        const char *const sql =
        "DELETE FROM "
          "TAPE_FILE "
        "WHERE "
          "VID=:VID AND "
          "FSEQ=:FSEQ";
        auto stmt = conn.createStmt(sql);
        stmt.bindString(":VID",fileRecycleLog.vid);
        stmt.bindUint64(":FSEQ",fileRecycleLog.fSeq);
        stmt.executeNonQuery();
      }
    }
    conn.commit();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeFileCatalogue::deleteTapeFiles(rdbms::Conn & conn,
  const common::dataStructures::DeleteArchiveRequest& request) const {
  try {
    //Delete the tape files after.
    const char *const deleteTapeFilesSql =
    "DELETE FROM "
      "TAPE_FILE "
    "WHERE TAPE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";

    auto deleteTapeFilesStmt = conn.createStmt(deleteTapeFilesSql);
    deleteTapeFilesStmt.bindUint64(":ARCHIVE_FILE_ID",request.archiveFileID);
    deleteTapeFilesStmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeFileCatalogue::deleteTapeFiles(rdbms::Conn & conn,
  const common::dataStructures::ArchiveFile& file) const {
  try {
    for(auto &tapeFile: file.tapeFiles) {

      //Delete the tape file.
      const char *const deleteTapeFilesSql =
      "DELETE FROM "
        "TAPE_FILE "
      "WHERE "
       "TAPE_FILE.VID = :VID AND "
       "TAPE_FILE.FSEQ = :FSEQ";

      auto deleteTapeFilesStmt = conn.createStmt(deleteTapeFilesSql);
      deleteTapeFilesStmt.bindString(":VID", tapeFile.vid);
      deleteTapeFilesStmt.bindUint64(":FSEQ", tapeFile.fSeq);
      deleteTapeFilesStmt.executeNonQuery();
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

common::dataStructures::RetrieveFileQueueCriteria RdbmsTapeFileCatalogue::prepareToRetrieveFile(
  const std::string &diskInstanceName, const uint64_t archiveFileId,
  const common::dataStructures::RequesterIdentity &user, const std::optional<std::string>& activity,
  log::LogContext &lc, const std::optional<std::string> &mountPolicyName) {
  try {
    cta::utils::Timer t;
    common::dataStructures::RetrieveFileQueueCriteria criteria;
    {
      auto conn = m_connPool->getConn();
      const auto getConnTime = t.secs(utils::Timer::resetCounter);
      const auto archiveFileCatalogue = static_cast<RdbmsArchiveFileCatalogue*>(m_rdbmsCatalogue->ArchiveFile().get());
      auto archiveFile = archiveFileCatalogue->getArchiveFileToRetrieveByArchiveFileId(conn, archiveFileId);
      const auto getArchiveFileTime = t.secs(utils::Timer::resetCounter);
      if(nullptr == archiveFile.get()) {
        exception::UserError ex;
        auto tapeFileStateList = archiveFileCatalogue->getTapeFileStateListForArchiveFileId(conn, archiveFileId);
        if (tapeFileStateList.empty()) {
          ex.getMessage() << "File with archive file ID " << archiveFileId << " does not exist in CTA namespace";
          throw ex;
        }
        const auto nonBrokenState = std::find_if(std::begin(tapeFileStateList), std::end(tapeFileStateList),
          [](std::pair<std::string, std::string> state) {
            return (state.second != "BROKEN")
                   && (state.second != "BROKEN_PENDING")
                   && (state.second != "EXPORTED")
                   && (state.second != "EXPORTED_PENDING");
          });

        if (nonBrokenState != std::end(tapeFileStateList)) {
          ex.getMessage() << "WARNING: File with archive file ID " << archiveFileId
            << " exits in CTA namespace but is temporarily unavailable on " << nonBrokenState->second << " tape "
            << nonBrokenState->first;
          throw ex;
        }
        const auto brokenState = tapeFileStateList.front();
        //All tape files are on broken tapes, just generate an error about the first
        ex.getMessage() << "ERROR: File with archive file ID " << archiveFileId
          << " exits in CTA namespace but is permanently unavailable on " << brokenState.second << " tape "
          << brokenState.first;
        throw ex;
      }
      if (mountPolicyName.has_value() && !mountPolicyName.value().empty()) {
          const auto mountPolicyCatalogue = static_cast<RdbmsMountPolicyCatalogue*>(m_rdbmsCatalogue->MountPolicy().get());
          std::optional<common::dataStructures::MountPolicy> mountPolicy = mountPolicyCatalogue->getMountPolicy(conn, mountPolicyName.value());
          if (mountPolicy) {
            criteria.archiveFile = *archiveFile;
            criteria.mountPolicy = mountPolicy.value();
            return criteria;
          } else {
            log::ScopedParamContainer spc(lc);
            spc.add("mountPolicyName", mountPolicyName.value())
               .add("archiveFileId", archiveFileId);
            lc.log(log::WARNING, "Catalogue::prepareToRetrieve Could not find specified mount policy, falling back to querying mount rules");
          }
      }

      if(diskInstanceName != archiveFile->diskInstance) {
        exception::UserError ue;
        ue.getMessage() << "Cannot retrieve file because the disk instance of the request does not match that of the"
          " archived file: archiveFileId=" << archiveFileId <<
          " requestDiskInstance=" << diskInstanceName << " archiveFileDiskInstance=" << archiveFile->diskInstance;
        throw ue;
      }

      t.reset();
      RequesterAndGroupMountPolicies mountPolicies;
      const auto mountPolicyCatalogue = static_cast<RdbmsMountPolicyCatalogue*>(m_rdbmsCatalogue->MountPolicy().get());
      if (activity) {
        mountPolicies = mountPolicyCatalogue->getMountPolicies(conn, diskInstanceName, user.name, user.group, activity.value());
      } else {
        mountPolicies = mountPolicyCatalogue->getMountPolicies(conn, diskInstanceName, user.name, user.group);
      }

      const auto getMountPoliciesTime = t.secs(utils::Timer::resetCounter);

      log::ScopedParamContainer spc(lc);
      spc.add("getConnTime", getConnTime)
         .add("getArchiveFileTime", getArchiveFileTime)
         .add("getMountPoliciesTime", getMountPoliciesTime);
      lc.log(log::INFO, "Catalogue::prepareToRetrieve internal timings");

      // Requester activity mount policies overrule requester mount policies
      // Requester mount policies overrule requester group mount policies
      common::dataStructures::MountPolicy mountPolicy;
      if (!mountPolicies.requesterActivityMountPolicies.empty()) {
        //More than one may match the activity, so choose the one with highest retrieve priority
        mountPolicy = *std::max_element(mountPolicies.requesterActivityMountPolicies.begin(),
          mountPolicies.requesterActivityMountPolicies.end(),
          [](const  common::dataStructures::MountPolicy &p1,  common::dataStructures::MountPolicy &p2) {
            return p1.retrievePriority < p2.retrievePriority;
          });
      } else if(!mountPolicies.requesterMountPolicies.empty()) {
        mountPolicy = mountPolicies.requesterMountPolicies.front();
      } else if(!mountPolicies.requesterGroupMountPolicies.empty()) {
        mountPolicy = mountPolicies.requesterGroupMountPolicies.front();
      } else {
        mountPolicies = mountPolicyCatalogue->getMountPolicies(conn, diskInstanceName, "default", user.group);

        if(!mountPolicies.requesterMountPolicies.empty()) {
          mountPolicy = mountPolicies.requesterMountPolicies.front();
        } else {
          exception::UserError ue;
          ue.getMessage()
            << "Cannot retrieve file because there are no mount rules for the requester, activity or their group:"
            << " archiveFileId=" << archiveFileId << " requester=" << diskInstanceName << ":" << user.name << ":"
            << user.group;
          if (activity) {
            ue.getMessage() << " activity=" << activity.value();
          }
          throw ue;
        }
      }
      criteria.archiveFile = *archiveFile;
      criteria.mountPolicy = mountPolicy;
    }
    return criteria;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace cta::catalogue
