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

#include <memory>
#include <optional>
#include <string>

#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsPhysicalLibraryCatalogue.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/ConstraintError.hpp"
#include "rdbms/UniqueConstraintError.hpp"
#include "rdbms/IntegrityConstraintError.hpp"

namespace cta::catalogue {

RdbmsPhysicalLibraryCatalogue::RdbmsPhysicalLibraryCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}


void RdbmsPhysicalLibraryCatalogue::createPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::PhysicalLibrary& pl) {

  auto conn = m_connPool->getConn();
  const uint64_t physicalLibraryId = getNextPhysicalLibraryId(conn);
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    INSERT INTO PHYSICAL_LIBRARY(
      PHYSICAL_LIBRARY_ID,
      PHYSICAL_LIBRARY_NAME,
      PHYSICAL_LIBRARY_MANUFACTURER,
      PHYSICAL_LIBRARY_MODEL,
      PHYSICAL_LIBRARY_TYPE,
      GUI_URL,
      WEBCAM_URL,
      PHYSICAL_LOCATION,

      NB_PHYSICAL_CARTRIDGE_SLOTS,
      NB_AVAILABLE_CARTRIDGE_SLOTS,
      NB_PHYSICAL_DRIVE_SLOTS,

      CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME, 

      USER_COMMENT) 
    VALUES(
      :PHYSICAL_LIBRARY_ID,
      :PHYSICAL_LIBRARY_NAME,
      :PHYSICAL_LIBRARY_MANUFACTURER,
      :PHYSICAL_LIBRARY_MODEL,
      :PHYSICAL_LIBRARY_TYPE,
      :GUI_URL,
      :WEBCAM_URL,
      :PHYSICAL_LOCATION,

      :NB_PHYSICAL_CARTRIDGE_SLOTS,
      :NB_AVAILABLE_CARTRIDGE_SLOTS,
      :NB_PHYSICAL_DRIVE_SLOTS,

      :CREATION_LOG_USER_NAME,
      :CREATION_LOG_HOST_NAME,
      :CREATION_LOG_TIME,

      :LAST_UPDATE_USER_NAME,
      :LAST_UPDATE_HOST_NAME,
      :LAST_UPDATE_TIME,

      :USER_COMMENT)
  )SQL";

  auto stmt = conn.createStmt(sql);

  stmt.bindUint64(":PHYSICAL_LIBRARY_ID"          , physicalLibraryId);
  stmt.bindString(":PHYSICAL_LIBRARY_NAME"        , pl.name);
  stmt.bindString(":PHYSICAL_LIBRARY_MANUFACTURER", pl.manufacturer);
  stmt.bindString(":PHYSICAL_LIBRARY_MODEL"       , pl.model);
  stmt.bindString(":PHYSICAL_LIBRARY_TYPE"        , RdbmsCatalogueUtils::nulloptIfEmptyStr(pl.type));
  stmt.bindString(":GUI_URL"                      , RdbmsCatalogueUtils::nulloptIfEmptyStr(pl.guiUrl));
  stmt.bindString(":WEBCAM_URL"                   , RdbmsCatalogueUtils::nulloptIfEmptyStr(pl.webcamUrl));
  stmt.bindString(":PHYSICAL_LOCATION"            , RdbmsCatalogueUtils::nulloptIfEmptyStr(pl.location));

  stmt.bindUint64(":NB_PHYSICAL_CARTRIDGE_SLOTS" , pl.nbPhysicalCartridgeSlots);
  stmt.bindUint64(":NB_AVAILABLE_CARTRIDGE_SLOTS", pl.nbAvailableCartridgeSlots);
  stmt.bindUint64(":NB_PHYSICAL_DRIVE_SLOTS"     , pl.nbPhysicalDriveSlots);

  stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
  stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
  stmt.bindUint64(":CREATION_LOG_TIME"     , now);

  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME"     , now);

  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(pl.comment, &m_log);
  stmt.bindString(":USER_COMMENT", trimmedComment);

  try {
    stmt.executeNonQuery();
  } catch(cta::rdbms::UniqueConstraintError& ex) {
    std::stringstream err_stream;
    err_stream << "Cannot create physical library " << pl.name << " because of unique constraint: " << ex.getViolatedConstraintName()
               << "\n" << ex.getMessageValue();
    throw exception::UserError(err_stream.str());
  } catch(cta::rdbms::ConstraintError& ex) {
    std::stringstream err_stream;
    err_stream << "Cannot create physical library " << pl.name << ": " << ex.getViolatedConstraintName()
               << "\n" << ex.getMessageValue();
    throw exception::UserError(err_stream.str());
  }
}

void RdbmsPhysicalLibraryCatalogue::deletePhysicalLibrary(const std::string& name) {
  const char* const sql = R"SQL(
    DELETE FROM PHYSICAL_LIBRARY 
    WHERE 
      PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":PHYSICAL_LIBRARY_NAME", name);

  try {
    stmt.executeNonQuery();
  } catch(cta::rdbms::IntegrityConstraintError& ex) {
    std::stringstream err_stream;
    err_stream << "Cannot delete physical library " << name << " because of integrity constraint: " << ex.getViolatedConstraintName()
               << "\n" << ex.getMessageValue();
    throw exception::UserError(err_stream.str());
  } catch(cta::rdbms::ConstraintError& ex) {
    std::stringstream err_stream;
    err_stream << "Cannot delete physical library " << name << ": " << ex.getViolatedConstraintName()
               << "\n" << ex.getMessageValue();
    throw exception::UserError(err_stream.str());
  }

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot delete physical library ") + name + " because it does not exist");
  }
}

std::list<common::dataStructures::PhysicalLibrary> RdbmsPhysicalLibraryCatalogue::getPhysicalLibraries() const {

  std::list<common::dataStructures::PhysicalLibrary> libs;
  const char* const sql = R"SQL(
    SELECT 
      PHYSICAL_LIBRARY_NAME AS PHYSICAL_LIBRARY_NAME,
      PHYSICAL_LIBRARY_MANUFACTURER AS PHYSICAL_LIBRARY_MANUFACTURER,
      PHYSICAL_LIBRARY_MODEL AS PHYSICAL_LIBRARY_MODEL,
      PHYSICAL_LIBRARY_TYPE AS PHYSICAL_LIBRARY_TYPE,
      GUI_URL AS GUI_URL,
      WEBCAM_URL AS WEBCAM_URL,
      PHYSICAL_LOCATION AS PHYSICAL_LOCATION,

      NB_PHYSICAL_CARTRIDGE_SLOTS AS NB_PHYSICAL_CARTRIDGE_SLOTS,
      NB_AVAILABLE_CARTRIDGE_SLOTS AS NB_AVAILABLE_CARTRIDGE_SLOTS,
      NB_PHYSICAL_DRIVE_SLOTS AS NB_PHYSICAL_DRIVE_SLOTS,

      CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME AS CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME AS LAST_UPDATE_TIME, 

      USER_COMMENT AS USER_COMMENT 
    FROM 
      PHYSICAL_LIBRARY 
    ORDER BY 
      PHYSICAL_LIBRARY_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    common::dataStructures::PhysicalLibrary pl;

    pl.name         = rset.columnString("PHYSICAL_LIBRARY_NAME");
    pl.manufacturer = rset.columnString("PHYSICAL_LIBRARY_MANUFACTURER");
    pl.model        = rset.columnString("PHYSICAL_LIBRARY_MODEL");
    pl.type         = rset.columnOptionalString("PHYSICAL_LIBRARY_TYPE");
    pl.guiUrl       = rset.columnOptionalString("GUI_URL");
    pl.webcamUrl    = rset.columnOptionalString("WEBCAM_URL");
    pl.location     = rset.columnOptionalString("PHYSICAL_LOCATION");

    pl.nbPhysicalCartridgeSlots  = rset.columnUint64("NB_PHYSICAL_CARTRIDGE_SLOTS");
    pl.nbAvailableCartridgeSlots = rset.columnOptionalUint16("NB_AVAILABLE_CARTRIDGE_SLOTS");
    pl.nbPhysicalDriveSlots      = rset.columnUint64("NB_PHYSICAL_DRIVE_SLOTS");

    pl.comment                   = rset.columnOptionalString("USER_COMMENT");

    pl.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    pl.creationLog.host     = rset.columnString("CREATION_LOG_HOST_NAME");
    pl.creationLog.time     = rset.columnUint64("CREATION_LOG_TIME");

    pl.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    pl.lastModificationLog.host     = rset.columnString("LAST_UPDATE_HOST_NAME");
    pl.lastModificationLog.time     = rset.columnUint64("LAST_UPDATE_TIME");

    libs.push_back(pl);
  }
  return libs;
}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::UpdatePhysicalLibrary& pl) {

  const time_t now = time(nullptr);

  std::string updateStmtStr = buildUpdateStmtStr(pl);
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(updateStmtStr);
  bindUpdateParams(stmt, pl, admin, now);

  try {
    stmt.executeNonQuery();
  } catch(cta::rdbms::UniqueConstraintError& ex) {
    std::ostringstream err_stream;
    err_stream << "Cannot update physical library " << pl.name << " because of unique constraint: " << ex.getViolatedConstraintName()
               << "\n" << ex.getMessageValue();
    throw exception::UserError(err_stream.str());
  } catch(cta::rdbms::ConstraintError& ex) {
    std::ostringstream err_stream;
    err_stream << "Cannot update physical library " << pl.name << ": " << ex.getViolatedConstraintName()
               << "\n" << ex.getMessageValue();
    throw exception::UserError(err_stream.str());
  }

  if (0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot update physical library ") + pl.name + " because it does not exist");
  }
}

std::optional<uint64_t> RdbmsPhysicalLibraryCatalogue::getPhysicalLibraryId(rdbms::Conn &conn,
  const std::string &name) const {
  const char* const sql = R"SQL(
    SELECT 
      PHYSICAL_LIBRARY_ID AS PHYSICAL_LIBRARY_ID 
    FROM 
      PHYSICAL_LIBRARY 
    WHERE 
      PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":PHYSICAL_LIBRARY_NAME", name);

  auto rset = stmt.executeQuery();
  if(!rset.next()) {
    return std::nullopt;
  }

  return rset.columnUint64("PHYSICAL_LIBRARY_ID");
}

std::string RdbmsPhysicalLibraryCatalogue::buildUpdateStmtStr(const common::dataStructures::UpdatePhysicalLibrary& pl) const {
  std::string setClause;

  if (pl.guiUrl) {
    setClause += R"SQL(GUI_URL = :GUI_URL,)SQL";
  }
  if (pl.webcamUrl) {
    setClause += R"SQL(WEBCAM_URL = :WEBCAM_URL,)SQL";
  }
  if (pl.location) {
    setClause += R"SQL(PHYSICAL_LOCATION = :PHYSICAL_LOCATION,)SQL";
  }
  if (pl.nbPhysicalCartridgeSlots) {
    setClause += R"SQL(NB_PHYSICAL_CARTRIDGE_SLOTS = :NB_PHYSICAL_CARTRIDGE_SLOTS,)SQL";
  }
  if (pl.nbAvailableCartridgeSlots) {
    setClause += R"SQL(NB_AVAILABLE_CARTRIDGE_SLOTS = :NB_AVAILABLE_CARTRIDGE_SLOTS,)SQL";
  }
  if (pl.nbPhysicalDriveSlots) {
    setClause += R"SQL(NB_PHYSICAL_DRIVE_SLOTS = :NB_PHYSICAL_DRIVE_SLOTS,)SQL";
  }
  if (pl.comment) {
    setClause += R"SQL(USER_COMMENT = :USER_COMMENT,)SQL";
  }

  if(setClause.empty()) {
    throw exception::UserError(std::string("At least one value must be updated in physical library ") + pl.name);
  }

  std::string sql = R"SQL(
    UPDATE PHYSICAL_LIBRARY SET
  )SQL";
  sql += setClause;
  sql += R"SQL(
    LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
    LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
    LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
  )SQL";
  sql += R"SQL(
    WHERE PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME
  )SQL";

  return sql;
}

void RdbmsPhysicalLibraryCatalogue::bindUpdateParams(cta::rdbms::Stmt& stmt, const common::dataStructures::UpdatePhysicalLibrary& pl, const common::dataStructures::SecurityIdentity& admin, const time_t now) const {
  if(pl.guiUrl)                    stmt.bindString(":GUI_URL", RdbmsCatalogueUtils::nulloptIfEmptyStr(pl.guiUrl.value()));
  if(pl.webcamUrl)                 stmt.bindString(":WEBCAM_URL", RdbmsCatalogueUtils::nulloptIfEmptyStr(pl.webcamUrl.value()));
  if(pl.location)                  stmt.bindString(":PHYSICAL_LOCATION", RdbmsCatalogueUtils::nulloptIfEmptyStr(pl.location.value()));
  if(pl.nbPhysicalCartridgeSlots)  stmt.bindUint64(":NB_PHYSICAL_CARTRIDGE_SLOTS", pl.nbPhysicalCartridgeSlots.value());
  if(pl.nbAvailableCartridgeSlots) stmt.bindUint64(":NB_AVAILABLE_CARTRIDGE_SLOTS", pl.nbAvailableCartridgeSlots.value());
  if(pl.nbPhysicalDriveSlots)      stmt.bindUint64(":NB_PHYSICAL_DRIVE_SLOTS", pl.nbPhysicalDriveSlots.value());
  if(pl.comment) {
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(pl.comment.value(), &m_log);
    stmt.bindString(":USER_COMMENT", trimmedComment);
  }

  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":PHYSICAL_LIBRARY_NAME", pl.name);
}

} // namespace cta::catalogue