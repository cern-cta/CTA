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

#include <list>
#include <memory>
#include <string>

#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsLogicalLibraryCatalogue.hpp"
#include "catalogue/rdbms/RdbmsMediaTypeCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapeCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapePoolCatalogue.hpp"
#include "catalogue/TapeForWriting.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/log/TimingList.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {

RdbmsTapeCatalogue::RdbmsTapeCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}

void RdbmsTapeCatalogue::createTape(const common::dataStructures::SecurityIdentity &admin,
  const CreateTapeAttributes & tape) {
  // CTA hard code this field to FALSE
  const bool isFromCastor = false;
  try {
    std::string vid = tape.vid;
    std::string mediaTypeName = tape.mediaType;
    std::string vendor = tape.vendor;
    std::string logicalLibraryName = tape.logicalLibraryName;
    std::string tapePoolName = tape.tapePoolName;
    bool full = tape.full;
    // Translate an empty comment string to a NULL database value
    const std::optional<std::string> tapeComment = tape.comment && tape.comment->empty() ? std::nullopt : tape.comment;
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(tapeComment, &m_log);
    const std::optional<std::string> stateReason = tape.stateReason
      && cta::utils::trimString(tape.stateReason.value()).empty() ? std::nullopt : tape.stateReason;
    const auto trimmedReason = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(stateReason, &m_log);
    if(vid.empty()) {
      throw UserSpecifiedAnEmptyStringVid("Cannot create tape because the VID is an empty string");
    }

    if(!utils::isUpper(vid)) {
      throw UserSpecifiedAnEmptyStringVid("Cannot create tape because the VID has non uppercase characters");
    }

    if(mediaTypeName.empty()) {
      throw UserSpecifiedAnEmptyStringMediaType("Cannot create tape because the media type is an empty string");
    }

    if(vendor.empty()) {
      throw UserSpecifiedAnEmptyStringVendor("Cannot create tape because the vendor is an empty string");
    }

    if(logicalLibraryName.empty()) {
      throw UserSpecifiedAnEmptyStringLogicalLibraryName("Cannot create tape because the logical library name is an"
        " empty string");
    }

    if(tapePoolName.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot create tape because the tape pool name is an empty string");
    }

    std::string tapeState;
    try {
      tapeState = common::dataStructures::Tape::stateToString(tape.state);
    } catch(cta::exception::Exception &ex) {
      std::string errorMsg = "Cannot create tape because the state specified does not exist. Possible values for state are: "
        + common::dataStructures::Tape::getAllPossibleStates();
      throw UserSpecifiedANonExistentTapeState(errorMsg);
    }

    if(tape.state != common::dataStructures::Tape::ACTIVE){
      if(!stateReason){
        throw UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive("Cannot create tape because no reason has been "
          "provided for the state " + tapeState);
      }
    }

    auto conn = m_connPool->getConn();
    if(RdbmsCatalogueUtils::tapeExists(conn, vid)) {
      throw exception::UserError(std::string("Cannot create tape ") + vid +
        " because a tape with the same volume identifier already exists");
    }
    const auto logicLibCatalogue = static_cast<RdbmsLogicalLibraryCatalogue*>(m_rdbmsCatalogue->LogicalLibrary().get());
    const auto logicalLibraryId = logicLibCatalogue->getLogicalLibraryId(conn, logicalLibraryName);
    if(!logicalLibraryId) {
      throw exception::UserError(std::string("Cannot create tape ") + vid + " because logical library " +
        logicalLibraryName + " does not exist");
    }
    const auto tapePoolCatalogue = static_cast<RdbmsTapePoolCatalogue*>(m_rdbmsCatalogue->TapePool().get());
    const auto tapePoolId = tapePoolCatalogue->getTapePoolId(conn, tapePoolName);
    if(!tapePoolId) {
      throw exception::UserError(std::string("Cannot create tape ") + vid + " because tape pool " +
      tapePoolName + " does not exist");
    }
    const auto mediaTypeCatalogue = static_cast<RdbmsMediaTypeCatalogue*>(m_rdbmsCatalogue->MediaType().get());
    const auto mediaTypeId = mediaTypeCatalogue->getMediaTypeId(conn, mediaTypeName);
    if(!mediaTypeId) {
      throw exception::UserError(std::string("Cannot create tape ") + vid + " because media type " +
        mediaTypeName + " does not exist");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO TAPE("
        "VID, "
        "MEDIA_TYPE_ID, "
        "VENDOR, "
        "LOGICAL_LIBRARY_ID, "
        "TAPE_POOL_ID, "
        "DATA_IN_BYTES, "
        "LAST_FSEQ, "
        "IS_FULL, "
        "IS_FROM_CASTOR, "

        "USER_COMMENT, "

        "TAPE_STATE, "
        "STATE_REASON, "
        "STATE_UPDATE_TIME, "
        "STATE_MODIFIED_BY, "

        "CREATION_LOG_USER_NAME, "
        "CREATION_LOG_HOST_NAME, "
        "CREATION_LOG_TIME, "

        "LAST_UPDATE_USER_NAME, "
        "LAST_UPDATE_HOST_NAME, "
        "LAST_UPDATE_TIME) "
      "VALUES("
        ":VID, "
        ":MEDIA_TYPE_ID, "
        ":VENDOR, "
        ":LOGICAL_LIBRARY_ID, "
        ":TAPE_POOL_ID, "
        ":DATA_IN_BYTES, "
        ":LAST_FSEQ, "
        ":IS_FULL, "
        ":IS_FROM_CASTOR, "

        ":USER_COMMENT, "

        ":TAPE_STATE, "
        ":STATE_REASON, "
        ":STATE_UPDATE_TIME, "
        ":STATE_MODIFIED_BY, "

        ":CREATION_LOG_USER_NAME, "
        ":CREATION_LOG_HOST_NAME, "
        ":CREATION_LOG_TIME, "

        ":LAST_UPDATE_USER_NAME, "
        ":LAST_UPDATE_HOST_NAME, "
        ":LAST_UPDATE_TIME"
      ")";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":VID", vid);
    stmt.bindUint64(":MEDIA_TYPE_ID", mediaTypeId.value());
    stmt.bindString(":VENDOR", vendor);
    stmt.bindUint64(":LOGICAL_LIBRARY_ID", logicalLibraryId.value());
    stmt.bindUint64(":TAPE_POOL_ID", tapePoolId.value());
    stmt.bindUint64(":DATA_IN_BYTES", 0);
    stmt.bindUint64(":LAST_FSEQ", 0);
    stmt.bindBool(":IS_FULL", full);
    stmt.bindBool(":IS_FROM_CASTOR", isFromCastor);

    stmt.bindString(":USER_COMMENT", trimmedComment);

    std::string stateModifiedBy = RdbmsCatalogueUtils::generateTapeStateModifiedBy(admin);
    stmt.bindString(":TAPE_STATE",cta::common::dataStructures::Tape::stateToString(tape.state));
    stmt.bindString(":STATE_REASON",trimmedReason);
    stmt.bindUint64(":STATE_UPDATE_TIME",now);
    stmt.bindString(":STATE_MODIFIED_BY", stateModifiedBy);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("mediaType", mediaTypeName)
       .add("vendor", vendor)
       .add("logicalLibraryName", logicalLibraryName)
       .add("tapePoolName", tapePoolName)
       .add("isFull", full ? 1 : 0)
       .add("isFromCastor", isFromCastor ? 1 : 0)
       .add("userComment", tape.comment ? tape.comment.value() : "")
       .add("tapeState",cta::common::dataStructures::Tape::stateToString(tape.state))
       .add("stateReason",stateReason ? stateReason.value() : "")
       .add("stateUpdateTime",now)
       .add("stateModifiedBy",stateModifiedBy)
       .add("creationLogUserName", admin.username)
       .add("creationLogHostName", admin.host)
       .add("creationLogTime", now);
    lc.log(log::INFO, "Catalogue - user created tape");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::deleteTape(const std::string &vid) {
  try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :DELETE_VID AND "
        "NOT EXISTS (SELECT VID FROM TAPE_FILE WHERE VID = :SELECT_VID) AND "
        "NOT EXISTS (SELECT VID FROM FILE_RECYCLE_LOG WHERE VID = :SELECT_VID2)";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(delete_sql);
    stmt.bindString(":DELETE_VID", vid);
    stmt.bindString(":SELECT_VID", vid);
    stmt.bindString(":SELECT_VID2", vid);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the tape does not exist or if it still has tape files or files in the recycle log
    if(0 == stmt.getNbAffectedRows()) {
      if(RdbmsCatalogueUtils::tapeExists(conn, vid)) {
        throw UserSpecifiedANonEmptyTape(std::string("Cannot delete tape ") + vid + " because either it contains one or more files or the files that were in it are in the file recycle log.");
      } else {
        throw UserSpecifiedANonExistentTape(std::string("Cannot delete tape ") + vid + " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<common::dataStructures::Tape> RdbmsTapeCatalogue::getTapes(const TapeSearchCriteria &searchCriteria) const {
  try {
    auto conn = m_connPool->getConn();
    return getTapes(conn, searchCriteria);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

common::dataStructures::VidToTapeMap RdbmsTapeCatalogue::getTapesByVid(const std::string& vid) const {
  try {
    const char* const sql =
    "SELECT "
      "TAPE.VID AS VID,"
      "MEDIA_TYPE.MEDIA_TYPE_NAME AS MEDIA_TYPE,"
      "TAPE.VENDOR AS VENDOR,"
      "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
      "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
      "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
      "TAPE.ENCRYPTION_KEY_NAME AS ENCRYPTION_KEY_NAME,"
      "MEDIA_TYPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
      "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
      "TAPE.LAST_FSEQ AS LAST_FSEQ,"
      "TAPE.IS_FULL AS IS_FULL,"
      "TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,"
      "TAPE.LABEL_FORMAT AS LABEL_FORMAT,"
      "TAPE.LABEL_DRIVE AS LABEL_DRIVE,"
      "TAPE.LABEL_TIME AS LABEL_TIME,"
      "TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,"
      "TAPE.LAST_READ_TIME AS LAST_READ_TIME,"
      "TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
      "TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,"
      "TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,"
      "TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,"
      "TAPE.USER_COMMENT AS USER_COMMENT,"
      "TAPE.TAPE_STATE AS TAPE_STATE,"
      "TAPE.STATE_REASON AS STATE_REASON,"
      "TAPE.STATE_UPDATE_TIME AS STATE_UPDATE_TIME,"
      "TAPE.STATE_MODIFIED_BY AS STATE_MODIFIED_BY,"
      "TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
      "TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
      "TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
      "TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
      "TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
      "TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
    "FROM "
      "TAPE "
    "INNER JOIN TAPE_POOL ON "
      "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
    "INNER JOIN LOGICAL_LIBRARY ON "
      "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID "
    "INNER JOIN MEDIA_TYPE ON "
      "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID "
    "INNER JOIN VIRTUAL_ORGANIZATION ON "
      "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
    "WHERE "
      "VID = :VID";

    common::dataStructures::VidToTapeMap vidToTapeMap;

    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    executeGetTapesByVidStmtAndCollectResults(stmt, vidToTapeMap);

    if(vidToTapeMap.size() != 1) {
      exception::Exception ex;
      ex.getMessage() << "Not all tapes were found: expected=1 actual=" << vidToTapeMap.size();
      throw ex;
    }
    return vidToTapeMap;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

common::dataStructures::VidToTapeMap RdbmsTapeCatalogue::getTapesByVid(const std::set<std::string> &vids) const {
  return getTapesByVid(vids, false);
}

common::dataStructures::VidToTapeMap RdbmsTapeCatalogue::getTapesByVid(const std::set<std::string> &vids, bool ignoreMissingVids) const {
  try {
    common::dataStructures::VidToTapeMap vidToTapeMap;

    if(vids.empty()) return vidToTapeMap;

    static const std::string selectTapesBy100VidsSql = getSelectTapesBy100VidsSql();

    auto conn = m_connPool->getConn();

    auto stmt = conn.createStmt(selectTapesBy100VidsSql);
    uint64_t vidNb = 1;

    for(const auto &vid: vids) {
      // Bind the current tape VID
      std::ostringstream paramName;
      paramName << ":V" << vidNb;
      stmt.bindString(paramName.str(), vid);

      // If the 100th tape VID has not yet been reached
      if(100 > vidNb) {
        vidNb++;
      } else { // The 100th VID has been reached
        vidNb = 1;

        // Execute the query and collect the results
        executeGetTapesByVidStmtAndCollectResults(stmt, vidToTapeMap);

        // Create a new statement
        stmt = conn.createStmt(selectTapesBy100VidsSql);
      }
    }

    // If there is a statement under construction
    if(1 != vidNb) {
      // Bind the remaining parameters with last tape VID.  This has no effect
      // on the search results but makes the statement valid.
      const std::string &lastVid = *vids.rbegin();
      while(100 >= vidNb) {
        std::ostringstream paramName;
        paramName << ":V" << vidNb;
        stmt.bindString(paramName.str(), lastVid);
        vidNb++;
      }

      // Execute the query and collect the results
      executeGetTapesByVidStmtAndCollectResults(stmt, vidToTapeMap);
    }

    if(!ignoreMissingVids && vids.size() != vidToTapeMap.size()) {
      exception::Exception ex;
      ex.getMessage() << "Not all tapes were found: expected=" << vids.size() << " actual=" << vidToTapeMap.size();
      throw ex;
    }

    return vidToTapeMap;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::map<std::string, std::string> RdbmsTapeCatalogue::getVidToLogicalLibrary(const std::set<std::string> &vids) const {
  try {
    std::map<std::string, std::string> vidToLogicalLibrary;

    if(vids.empty()) return vidToLogicalLibrary;

    static const std::string sql = getSelectVidToLogicalLibraryBy100Sql();

    auto conn = m_connPool->getConn();

    auto stmt = conn.createStmt(sql);
    uint64_t vidNb = 1;

    for(const auto &vid: vids) {
      // Bind the current tape VID
      std::ostringstream paramName;
      paramName << ":V" << vidNb;
      stmt.bindString(paramName.str(), vid);

      // If the 100th tape VID has not yet been reached
      if(100 > vidNb) {
        vidNb++;
      } else { // The 100th VID has been reached
        vidNb = 1;

        // Execute the query and collect the results
        executeGetVidToLogicalLibraryBy100StmtAndCollectResults(stmt, vidToLogicalLibrary);

        // Create a new statement
        stmt = conn.createStmt(sql);
      }
    }

    // If there is a statement under construction
    if(1 != vidNb) {
      // Bind the remaining parameters with last tape VID.  This has no effect
      // on the search results but makes the statement valid.
      const std::string &lastVid = *vids.rbegin();
      while(100 >= vidNb) {
        std::ostringstream paramName;
        paramName << ":V" << vidNb;
        stmt.bindString(paramName.str(), lastVid);
        vidNb++;
      }

      // Execute the query and collect the results
      executeGetVidToLogicalLibraryBy100StmtAndCollectResults(stmt, vidToLogicalLibrary);
    }

    if(vids.size() != vidToLogicalLibrary.size()) {
      exception::Exception ex;
      ex.getMessage() << "Not all tapes were found: expected=" << vids.size() << " actual=" <<
        vidToLogicalLibrary.size();
      throw ex;
    }

    return vidToLogicalLibrary;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
  cta::log::LogContext & lc) {
  try{
    log::TimingList tl;
    utils::Timer t;
    auto conn = m_connPool->getConn();

    TapeSearchCriteria searchCriteria;
    searchCriteria.vid = vid;
    const auto tapes = getTapes(conn, searchCriteria);
    tl.insertAndReset("getTapesTime", t);

    if (tapes.empty()) {
      throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because it does not exist");
    } else if (tapes.front().state != common::dataStructures::Tape::State::ACTIVE
      && tapes.front().state != common::dataStructures::Tape::State::DISABLED) {
      throw exception::UserError(std::string("Cannot reclaim tape ") + vid
        + " because it is not on ACTIVE or DISABLED state");
    } else if (!tapes.front().full) {
      throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because it is not FULL");
    }

    // The tape exists and is full, we can try to reclaim it
    if (this->getNbFilesOnTape(conn, vid) == 0) {
      tl.insertAndReset("getNbFilesOnTape", t);
      // There is no files on the tape, we can reclaim it : delete the files and reset the counters
      static_cast<RdbmsFileRecycleLogCatalogue*>(
        m_rdbmsCatalogue->FileRecycleLog().get())->deleteFilesFromRecycleLog(conn, vid, lc);
      tl.insertAndReset("deleteFileFromRecycleLogTime", t);
      resetTapeCounters(conn, admin, vid);
      tl.insertAndReset("resetTapeCountersTime", t);
      log::ScopedParamContainer spc(lc);
      spc.add("vid", vid);
      spc.add("host", admin.host);
      spc.add("username", admin.username);
      tl.addToLog(spc);
      lc.log(log::INFO, "In RdbmsCatalogue::reclaimTape(), tape reclaimed.");
    } else {
      throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because there is at least one tape"
            " file in the catalogue that is on the tape");
    }
  } catch (exception::UserError& ue) {
    throw;
  }
  catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::checkTapeForLabel(const std::string &vid) {
  try{
    auto conn = m_connPool->getConn();

    TapeSearchCriteria searchCriteria;
    searchCriteria.vid = vid;
    const auto tapes = getTapes(conn, searchCriteria);

    if(tapes.empty()) {
      throw exception::UserError(std::string("Cannot label tape ") + vid +
                                             " because it does not exist");
    }
    //The tape exists checks any files on it
    const uint64_t nbFilesOnTape = getNbFilesOnTape(conn, vid);
    if( 0 != nbFilesOnTape) {
      throw exception::UserError(std::string("Cannot label tape ") + vid +
                                             " because it has " +
                                             std::to_string(nbFilesOnTape) +
                                             " file(s)");
    }
  } catch (exception::UserError& ue) {
    throw;
  }
  catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

uint64_t RdbmsTapeCatalogue::getNbFilesOnTape(const std::string &vid) const {
  try {
    auto conn = m_connPool->getConn();
    return getNbFilesOnTape(conn, vid);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &mediaType) {
  try {
    auto conn = m_connPool->getConn();
    if(!RdbmsCatalogueUtils::mediaTypeExists(conn, mediaType)){
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because the media type " + mediaType + " does not exist");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "MEDIA_TYPE_ID = (SELECT MEDIA_TYPE_ID FROM MEDIA_TYPE WHERE MEDIA_TYPE.MEDIA_TYPE_NAME = :MEDIA_TYPE),"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MEDIA_TYPE", mediaType);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("mediaType", mediaType)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - mediaType");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
  const std::string &vendor) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "VENDOR = :VENDOR,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VENDOR", vendor);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("vendor", vendor)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - vendor");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &logicalLibraryName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LOGICAL_LIBRARY_ID = "
          "(SELECT LOGICAL_LIBRARY_ID FROM LOGICAL_LIBRARY WHERE LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME),"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    if(!RdbmsCatalogueUtils::logicalLibraryExists(conn,logicalLibraryName)){
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because the logical library " + logicalLibraryName + " does not exist");
    }
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because either it or logical library " +
        logicalLibraryName + " does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("logicalLibraryName", logicalLibraryName)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - logicalLibraryName");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &tapePoolName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "TAPE_POOL_ID = (SELECT TAPE_POOL_ID FROM TAPE_POOL WHERE TAPE_POOL_NAME = :TAPE_POOL_NAME),"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    if(!RdbmsCatalogueUtils::tapePoolExists(conn,tapePoolName)){
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because the tape pool " + tapePoolName + " does not exist");
    }
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because either it or tape pool " +
        tapePoolName + " does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("tapePoolName", tapePoolName)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - tapePoolName");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &encryptionKeyName) {
  try {
    std::optional<std::string> optionalEncryptionKeyName;
    if(!encryptionKeyName.empty()) {
      optionalEncryptionKeyName = encryptionKeyName;
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "ENCRYPTION_KEY_NAME = :ENCRYPTION_KEY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ENCRYPTION_KEY_NAME", optionalEncryptionKeyName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("encryptionKeyName", optionalEncryptionKeyName ? optionalEncryptionKeyName.value() : "NULL")
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - encryptionKeyName");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &verificationStatus) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "VERIFICATION_STATUS = :VERIFICATION_STATUS,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    if (verificationStatus.empty()) {
      stmt.bindString(":VERIFICATION_STATUS", std::nullopt);
    } else {
      stmt.bindString(":VERIFICATION_STATUS", verificationStatus);
    }
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("verificationStatus", verificationStatus)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - verificationStatus");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid,
  const common::dataStructures::Tape::State & state,
  const std::optional<common::dataStructures::Tape::State> & prev_state,
  const std::optional<std::string> & stateReason) {
  try {
    using namespace common::dataStructures;
    const time_t now = time(nullptr);

    const auto trimmedReason = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(stateReason, &m_log);

    std::string stateStr;
    try {
      stateStr = cta::common::dataStructures::Tape::stateToString(state);
    } catch(cta::exception::Exception & ex){
      std::string errorMsg = "The state provided in parameter (" + std::to_string(state) + ") is not known or has not been initialized existing states are:" + common::dataStructures::Tape::getAllPossibleStates();
      throw UserSpecifiedANonExistentTapeState(errorMsg);
    }

    std::string prevStateStr;
    if (prev_state.has_value()) {
      try {
        prevStateStr = cta::common::dataStructures::Tape::stateToString(prev_state.value());
      } catch (cta::exception::Exception &ex) {
        std::string errorMsg = "The previous state provided in parameter (" + std::to_string(prev_state.value()) + ") is not known or has not been initialized existing states are:" + common::dataStructures::Tape::getAllPossibleStates();
        throw UserSpecifiedANonExistentTapeState(errorMsg);
      }
    }

    //Check the reason is set for all the status except the ACTIVE one, this is the only state that allows the reason to be set to null.
    if(state != Tape::State::ACTIVE){
      if(!trimmedReason){
        throw UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive(std::string("Cannot modify the state of the tape ") + vid + " to " + stateStr + " because the reason has not been provided.");
      }
    }

    std::string sql =
      "UPDATE TAPE SET "
        "TAPE_STATE = :TAPE_STATE,"
        "STATE_REASON = :STATE_REASON,"
        "STATE_UPDATE_TIME = :STATE_UPDATE_TIME,"
        "STATE_MODIFIED_BY = :STATE_MODIFIED_BY "
      "WHERE "
        "VID = :VID";

    if (prev_state.has_value()) {
      sql += " AND TAPE_STATE = :PREV_TAPE_STATE";
    }

    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":TAPE_STATE", stateStr);
    stmt.bindString(":STATE_REASON", trimmedReason);
    stmt.bindUint64(":STATE_UPDATE_TIME", now);
    stmt.bindString(":STATE_MODIFIED_BY", RdbmsCatalogueUtils::generateTapeStateModifiedBy(admin));
    stmt.bindString(":VID",vid);
    if (prev_state.has_value()) {
      stmt.bindString(":PREV_TAPE_STATE",prevStateStr);
    }
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentTape(std::string("Cannot modify the state of the tape ") + vid
        + " because it does not exist or because a recent state change has been detected");
    }

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

bool RdbmsTapeCatalogue::tapeExists(const std::string &vid) const {
  try {
    auto conn = m_connPool->getConn();
    return RdbmsCatalogueUtils::tapeExists(conn, vid);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
  const bool fullValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FULL = :IS_FULL,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":IS_FULL", fullValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isFull", fullValue ? 1 : 0)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - isFull");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::setTapeDirty(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
  const bool dirtyValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "DIRTY = :DIRTY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":DIRTY", dirtyValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("dirty", dirtyValue ? 1 : 0)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - dirty");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::noSpaceLeftOnTape(const std::string &vid) {
  try {
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FULL = '1' "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::Exception(std::string("Tape ") + vid + " does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isFull", 1)
       .add("method", "noSpaceLeftOnTape");
    lc.log(log::INFO, "Catalogue - system modified tape - isFull");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<TapeForWriting> RdbmsTapeCatalogue::getTapesForWriting(const std::string &logicalLibraryName) const {
  try {
    std::list<TapeForWriting> tapes;
    const char *const sql =
      "SELECT "
        "TAPE.VID AS VID,"
        "MEDIA_TYPE.MEDIA_TYPE_NAME AS MEDIA_TYPE,"
        "TAPE.VENDOR AS VENDOR,"
        "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
        "MEDIA_TYPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
        "TAPE.LAST_FSEQ AS LAST_FSEQ,"
        "TAPE.LABEL_FORMAT AS LABEL_FORMAT "
      "FROM "
        "TAPE "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
      "INNER JOIN LOGICAL_LIBRARY ON "
        "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID "
      "INNER JOIN MEDIA_TYPE ON "
        "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID "
      "INNER JOIN VIRTUAL_ORGANIZATION ON "
        "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
      "WHERE "
//      "TAPE.LABEL_DRIVE IS NOT NULL AND " // Set when the tape has been labelled
//      "TAPE.LABEL_TIME IS NOT NULL AND "  // Set when the tape has been labelled
        "TAPE.TAPE_STATE = :TAPE_STATE AND "
        "TAPE.IS_FULL = '0' AND "
        "TAPE.IS_FROM_CASTOR = '0' AND "
        "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME "
      "ORDER BY TAPE.DATA_IN_BYTES DESC";

    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    stmt.bindString(":TAPE_STATE",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::ACTIVE));
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      TapeForWriting tape;
      tape.vid = rset.columnString("VID");
      tape.mediaType = rset.columnString("MEDIA_TYPE");
      tape.vendor = rset.columnString("VENDOR");
      tape.tapePool = rset.columnString("TAPE_POOL_NAME");
      tape.vo = rset.columnString("VO");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
      tape.labelFormat = common::dataStructures::Label::validateFormat(rset.columnOptionalUint8("LABEL_FORMAT"),
        "[RdbmsCatalogue::getTapesForWriting()]");
      tape.encryptionKeyName = rset.columnString("ENCRYPTION_KEY_NAME");

      tapes.push_back(tape);
    }
    return tapes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

common::dataStructures::Label::Format RdbmsTapeCatalogue::getTapeLabelFormat(const std::string& vid) const {
  try {
    const char *const sql =
      "SELECT "
        "TAPE.LABEL_FORMAT AS LABEL_FORMAT "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :VID";

    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      return common::dataStructures::Label::validateFormat(rset.columnOptionalUint8("LABEL_FORMAT"),
        "[RdbmsCatalogue::getTapeLabelFormat()]");
    } else {
      throw exception::Exception(std::string("No such tape with vid=") + vid);
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::setTapeIsFromCastorInUnitTests(const std::string &vid) {
  try {
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FROM_CASTOR = '1' "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::Exception(std::string("Tape ") + vid + " does not exist");
    }


    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isFromCastor", 1)
       .add("method", "setTapeIsFromCastorInUnitTests");
    lc.log(log::INFO, "Catalogue - system modified tape - isFromCastor");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::setTapeDirty(const std::string & vid) {
  try {
    auto conn = m_connPool->getConn();
    RdbmsCatalogueUtils::setTapeDirty(conn,vid);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::modifyTapeComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::optional<std::string> &comment) {
  try {
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", trimmedComment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }


    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("userComment", trimmedComment ? trimmedComment.value() : "")
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - userComment");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::tapeLabelled(const std::string &vid, const std::string &drive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LABEL_DRIVE = :LABEL_DRIVE,"
        "LABEL_TIME = :LABEL_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LABEL_DRIVE", drive);
    stmt.bindUint64(":LABEL_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::tapeMountedForArchive(const std::string &vid, const std::string &drive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_WRITE_DRIVE = :LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME = :LAST_WRITE_TIME, "
        "WRITE_MOUNT_COUNT = WRITE_MOUNT_COUNT + 1 "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LAST_WRITE_DRIVE", drive);
    stmt.bindUint64(":LAST_WRITE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("lastWriteDrive", drive)
       .add("lastWriteTime", now);
    lc.log(log::INFO, "Catalogue - system modified tape - mountedForArchive");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::tapeMountedForRetrieve(const std::string &vid, const std::string &drive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_READ_DRIVE = :LAST_READ_DRIVE,"
        "LAST_READ_TIME = :LAST_READ_TIME, "
        "READ_MOUNT_COUNT = READ_MOUNT_COUNT + 1 "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LAST_READ_DRIVE", drive);
    stmt.bindUint64(":LAST_READ_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("lastReadDrive", drive)
       .add("lastReadTime", now);
    lc.log(log::INFO, "Catalogue - system modified tape - mountedForRetrieve");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<common::dataStructures::Tape> RdbmsTapeCatalogue::getTapes(rdbms::Conn &conn,
  const TapeSearchCriteria &searchCriteria) const {
  if(RdbmsCatalogueUtils::isSetAndEmpty(searchCriteria.vid))
    throw exception::UserError("VID cannot be an empty string");
  if(RdbmsCatalogueUtils::isSetAndEmpty(searchCriteria.mediaType))
    throw exception::UserError("Media type cannot be an empty string");
  if(RdbmsCatalogueUtils::isSetAndEmpty(searchCriteria.vendor))
    throw exception::UserError("Vendor cannot be an empty string");
  if(RdbmsCatalogueUtils::isSetAndEmpty(searchCriteria.logicalLibrary))
    throw exception::UserError("Logical library cannot be an empty string");
  if(RdbmsCatalogueUtils::isSetAndEmpty(searchCriteria.tapePool))
    throw exception::UserError("Tape pool cannot be an empty string");
  if(RdbmsCatalogueUtils::isSetAndEmpty(searchCriteria.vo))
    throw exception::UserError("Virtual organisation cannot be an empty string");
  if(RdbmsCatalogueUtils::isSetAndEmpty(searchCriteria.diskFileIds))
    throw exception::UserError("Disk file ID list cannot be empty");

  try {
    if(searchCriteria.tapePool && !RdbmsCatalogueUtils::tapePoolExists(conn, searchCriteria.tapePool.value())) {
      UserSpecifiedANonExistentTapePool ex;
      ex.getMessage() << "Cannot list tapes because tape pool " + searchCriteria.tapePool.value() + " does not exist";
      throw ex;
    }

    std::list<common::dataStructures::Tape> tapes;
    std::string sql =
      "SELECT "
        "TAPE.VID AS VID,"
        "MEDIA_TYPE.MEDIA_TYPE_NAME AS MEDIA_TYPE,"
        "TAPE.VENDOR AS VENDOR,"
        "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
        "TAPE.ENCRYPTION_KEY_NAME AS ENCRYPTION_KEY_NAME,"
        "MEDIA_TYPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
        "TAPE.NB_MASTER_FILES AS NB_MASTER_FILES,"
        "TAPE.MASTER_DATA_IN_BYTES AS MASTER_DATA_IN_BYTES,"
        "TAPE.LAST_FSEQ AS LAST_FSEQ,"
        "TAPE.IS_FULL AS IS_FULL,"
        "TAPE.DIRTY AS DIRTY,"

        "TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,"

        "TAPE.LABEL_FORMAT AS LABEL_FORMAT,"

        "TAPE.LABEL_DRIVE AS LABEL_DRIVE,"
        "TAPE.LABEL_TIME AS LABEL_TIME,"

        "TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "TAPE.LAST_READ_TIME AS LAST_READ_TIME,"

        "TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,"

        "TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,"
        "TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,"

        "TAPE.VERIFICATION_STATUS AS VERIFICATION_STATUS,"

        "TAPE.USER_COMMENT AS USER_COMMENT,"

        "TAPE.TAPE_STATE AS TAPE_STATE,"
        "TAPE.STATE_REASON AS STATE_REASON,"
        "TAPE.STATE_UPDATE_TIME AS STATE_UPDATE_TIME,"
        "TAPE.STATE_MODIFIED_BY AS STATE_MODIFIED_BY,"

        "TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
      "INNER JOIN LOGICAL_LIBRARY ON "
        "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID "
      "INNER JOIN MEDIA_TYPE ON "
        "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID "
      "INNER JOIN VIRTUAL_ORGANIZATION ON "
        "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID";

    if(searchCriteria.vid ||
       searchCriteria.mediaType ||
       searchCriteria.vendor ||
       searchCriteria.logicalLibrary ||
       searchCriteria.tapePool ||
       searchCriteria.vo ||
       searchCriteria.capacityInBytes ||
       searchCriteria.full ||
       searchCriteria.diskFileIds ||
       searchCriteria.state ||
       searchCriteria.fromCastor) {
      sql += " WHERE";
    }

    bool addedAWhereConstraint = false;

    if(searchCriteria.vid) {
      sql += " TAPE.VID = :VID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.mediaType) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " MEDIA_TYPE.MEDIA_TYPE_NAME = :MEDIA_TYPE";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vendor) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.VENDOR = :VENDOR";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.logicalLibrary) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.tapePool) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vo) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME = :VO";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.capacityInBytes) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " MEDIA_TYPE.CAPACITY_IN_BYTES = :CAPACITY_IN_BYTES";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.full) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.IS_FULL = :IS_FULL";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileIds) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " VID IN ("
         "SELECT DISTINCT A.VID "
         "FROM "
           "TAPE_FILE A, ARCHIVE_FILE B "
         "WHERE "
           "A.ARCHIVE_FILE_ID = B.ARCHIVE_FILE_ID AND "
           "B.DISK_FILE_ID IN (:DISK_FID0)"
           //"B.DISK_FILE_ID IN (:DISK_FID0, :DISK_FID1, :DISK_FID2, :DISK_FID3, :DISK_FID4, :DISK_FID5, :DISK_FID6, :DISK_FID7, :DISK_FID8, :DISK_FID9)"
         ")";
      addedAWhereConstraint = true;
    }

    if(searchCriteria.state) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.TAPE_STATE = :TAPE_STATE";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.fromCastor) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.IS_FROM_CASTOR = :FROM_CASTOR";
      addedAWhereConstraint = true;
    }

    sql += " ORDER BY TAPE.VID";

    auto stmt = conn.createStmt(sql);

    if(searchCriteria.vid) stmt.bindString(":VID", searchCriteria.vid.value());
    if(searchCriteria.mediaType) stmt.bindString(":MEDIA_TYPE", searchCriteria.mediaType.value());
    if(searchCriteria.vendor) stmt.bindString(":VENDOR", searchCriteria.vendor.value());
    if(searchCriteria.logicalLibrary) stmt.bindString(":LOGICAL_LIBRARY_NAME", searchCriteria.logicalLibrary.value());
    if(searchCriteria.tapePool) stmt.bindString(":TAPE_POOL_NAME", searchCriteria.tapePool.value());
    if(searchCriteria.vo) stmt.bindString(":VO", searchCriteria.vo.value());
    if(searchCriteria.capacityInBytes) stmt.bindUint64(":CAPACITY_IN_BYTES", searchCriteria.capacityInBytes.value());
    if(searchCriteria.full) stmt.bindBool(":IS_FULL", searchCriteria.full.value());
    if(searchCriteria.fromCastor) stmt.bindBool(":FROM_CASTOR", searchCriteria.fromCastor.value());
    try{
      if(searchCriteria.state)
        stmt.bindString(":TAPE_STATE", cta::common::dataStructures::Tape::stateToString(searchCriteria.state.value()));
    } catch(cta::exception::Exception &ex){
      throw cta::exception::UserError(std::string("The state provided does not exist. Possible values are: ")
        + cta::common::dataStructures::Tape::getAllPossibleStates());
    }

    // Disk file ID lookup requires multiple queries
    std::vector<std::string>::const_iterator diskFileId_it;
    std::set<std::string> vidsInList;
    if(searchCriteria.diskFileIds) diskFileId_it = searchCriteria.diskFileIds.value().begin();
    int num_queries = searchCriteria.diskFileIds ? searchCriteria.diskFileIds.value().size() : 1;

    for(int i = 0; i < num_queries; ++i) {
      if(searchCriteria.diskFileIds) {
        stmt.bindString(":DISK_FID0", *diskFileId_it++);
      }

      auto rset = stmt.executeQuery();
      while (rset.next()) {
        auto vid = rset.columnString("VID");
        if(vidsInList.count(vid) == 1) continue;
        vidsInList.insert(vid);

        common::dataStructures::Tape tape;

        tape.vid = vid;
        tape.mediaType = rset.columnString("MEDIA_TYPE");
        tape.vendor = rset.columnString("VENDOR");
        tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
        tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
        tape.vo = rset.columnString("VO");
        tape.encryptionKeyName = rset.columnOptionalString("ENCRYPTION_KEY_NAME");
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

        tape.verificationStatus =  rset.columnOptionalString("VERIFICATION_STATUS");

        auto optionalComment = rset.columnOptionalString("USER_COMMENT");
        tape.comment = optionalComment ? optionalComment.value() : "";

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

        tapes.push_back(tape);
      }
    }
    if(searchCriteria.diskFileIds) {
      // When searching by diskFileId, results are not guaranteed to be in sorted order
      tapes.sort([](const common::dataStructures::Tape &a, const common::dataStructures::Tape &b) { return a.vid < b.vid; });
    }

    return tapes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::setTapeLastFSeq(rdbms::Conn &conn, const std::string &vid, const uint64_t lastFSeq) {
  try {
    threading::MutexLocker locker(m_rdbmsCatalogue->m_mutex);

    const uint64_t currentValue = getTapeLastFSeq(conn, vid);
    if(lastFSeq != currentValue + 1) {
      exception::Exception ex;
      ex.getMessage() << "The last FSeq MUST be incremented by exactly one: currentValue=" << currentValue <<
        ",nextValue=" << lastFSeq;
      throw ex;
    }
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_FSEQ = :LAST_FSEQ "
      "WHERE "
        "VID=:VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindUint64(":LAST_FSEQ", lastFSeq);
    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::deleteTapeFiles(rdbms::Conn& conn, const std::string& vid) const {
  try {
    const char * const sql =
    "DELETE FROM TAPE_FILE WHERE VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();
    RdbmsCatalogueUtils::setTapeDirty(conn,vid);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

uint64_t RdbmsTapeCatalogue::getNbFilesOnTape(rdbms::Conn& conn, const std::string& vid) const {
  try {
    const char *const sql =
    "SELECT COUNT(*) AS NB_FILES FROM TAPE_FILE "
    "WHERE VID = :VID ";

    auto stmt = conn.createStmt(sql);

    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    rset.next();
    return rset.columnUint64("NB_FILES");
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsTapeCatalogue::resetTapeCounters(rdbms::Conn& conn, const common::dataStructures::SecurityIdentity& admin,
  const std::string& vid) const {
  try {
    const time_t now = time(nullptr);
    const char * const sql =
    "UPDATE TAPE SET "
        "DATA_IN_BYTES = 0,"
        "MASTER_DATA_IN_BYTES = 0,"
        "LAST_FSEQ = 0,"
        "NB_MASTER_FILES = 0,"
        "NB_COPY_NB_1 = 0,"
        "COPY_NB_1_IN_BYTES = 0,"
        "NB_COPY_NB_GT_1 = 0,"
        "COPY_NB_GT_1_IN_BYTES = 0,"
        "IS_FULL = '0',"
        "IS_FROM_CASTOR = '0',"
        "VERIFICATION_STATUS = :VERIFICATION_STATUS,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME,"
        "DIRTY = '0' "
      "WHERE "
        "VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VERIFICATION_STATUS", std::nullopt);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();
  } catch (exception::Exception &ex){
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::optional<common::dataStructures::TapeLog> RdbmsTapeCatalogue::getTapeLogFromRset(const rdbms::Rset &rset,
  const std::string &driveColName, const std::string &timeColName) const {
  try {
    const std::optional<std::string> drive = rset.columnOptionalString(driveColName);
    const std::optional<uint64_t> time = rset.columnOptionalUint64(timeColName);

    if(!drive && !time) {
      return std::nullopt;
    }

    if(drive && !time) {
      throw exception::Exception(std::string("Database column ") + driveColName + " contains " + drive.value() +
        " but column " + timeColName + " is nullptr");
    }

    if(time && !drive) {
      throw exception::Exception(std::string("Database column ") + timeColName + " contains " +
        std::to_string(time.value()) + " but column " + driveColName + " is nullptr");
    }

    common::dataStructures::TapeLog tapeLog;
    tapeLog.drive = drive.value();
    tapeLog.time = time.value();

    return tapeLog;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::string RdbmsTapeCatalogue::getSelectTapesBy100VidsSql() const {
  std::stringstream sql;

  sql <<
    "SELECT "
      "TAPE.VID AS VID,"
      "MEDIA_TYPE.MEDIA_TYPE_NAME AS MEDIA_TYPE,"
      "TAPE.VENDOR AS VENDOR,"
      "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
      "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
      "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
      "TAPE.ENCRYPTION_KEY_NAME AS ENCRYPTION_KEY_NAME,"
      "MEDIA_TYPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
      "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
      "TAPE.LAST_FSEQ AS LAST_FSEQ,"
      "TAPE.IS_FULL AS IS_FULL,"
      "TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,"

      "TAPE.LABEL_FORMAT AS LABEL_FORMAT,"

      "TAPE.LABEL_DRIVE AS LABEL_DRIVE,"
      "TAPE.LABEL_TIME AS LABEL_TIME,"

      "TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,"
      "TAPE.LAST_READ_TIME AS LAST_READ_TIME,"

      "TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
      "TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,"

      "TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,"
      "TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,"

      "TAPE.USER_COMMENT AS USER_COMMENT,"

      "TAPE.TAPE_STATE AS TAPE_STATE,"
      "TAPE.STATE_REASON AS STATE_REASON,"
      "TAPE.STATE_UPDATE_TIME AS STATE_UPDATE_TIME,"
      "TAPE.STATE_MODIFIED_BY AS STATE_MODIFIED_BY,"

      "TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
      "TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
      "TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

      "TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
      "TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
      "TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
    "FROM "
      "TAPE "
    "INNER JOIN TAPE_POOL ON "
      "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
    "INNER JOIN LOGICAL_LIBRARY ON "
      "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID "
    "INNER JOIN MEDIA_TYPE ON "
      "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID "
    "INNER JOIN VIRTUAL_ORGANIZATION ON "
      "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
    "WHERE "
      "VID IN (:V1";

  for(uint32_t i=2; i<=100; i++) {
    sql << ",:V" << i;
  }

  sql << ")";

  return sql.str();
}

void RdbmsTapeCatalogue::executeGetTapesByVidStmtAndCollectResults(rdbms::Stmt &stmt,
  common::dataStructures::VidToTapeMap &vidToTapeMap) const {
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    common::dataStructures::Tape tape;

    tape.vid = rset.columnString("VID");
    tape.mediaType = rset.columnString("MEDIA_TYPE");
    tape.vendor = rset.columnString("VENDOR");
    tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
    tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
    tape.vo = rset.columnString("VO");
    tape.encryptionKeyName = rset.columnOptionalString("ENCRYPTION_KEY_NAME");
    tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
    tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
    tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
    tape.full = rset.columnBool("IS_FULL");
    tape.isFromCastor = rset.columnBool("IS_FROM_CASTOR");

    tape.labelFormat = common::dataStructures::Label::validateFormat(rset.columnOptionalUint8("LABEL_FORMAT"),
      "[RdbmsCatalogue::executeGetTapesByVidsStmtAndCollectResults()]");

    tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
    tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
    tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");
    tape.readMountCount = rset.columnUint64("READ_MOUNT_COUNT");
    tape.writeMountCount = rset.columnUint64("WRITE_MOUNT_COUNT");
    auto optionalComment = rset.columnOptionalString("USER_COMMENT");
    tape.comment = optionalComment ? optionalComment.value() : "";

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

    vidToTapeMap[tape.vid] = tape;
  }
}

std::string RdbmsTapeCatalogue::getSelectVidToLogicalLibraryBy100Sql() const {
  std::stringstream sql;

  sql <<
    "SELECT "
      "TAPE.VID AS VID, "
      "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME "
    "FROM "
      "TAPE "
    "INNER JOIN LOGICAL_LIBRARY ON "
      "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID "
    "WHERE "
      "VID IN (:V1";

  for(uint32_t i=2; i<=100; i++) {
    sql << ",:V" << i;
  }

  sql << ")";

  return sql.str();
}

void RdbmsTapeCatalogue::executeGetVidToLogicalLibraryBy100StmtAndCollectResults(rdbms::Stmt &stmt,
std::map<std::string, std::string> &vidToLogicalLibrary) const {
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    vidToLogicalLibrary[rset.columnString("VID")] = rset.columnString("LOGICAL_LIBRARY_NAME");
  }
}

} // namespace catalogue
} // namespace cta