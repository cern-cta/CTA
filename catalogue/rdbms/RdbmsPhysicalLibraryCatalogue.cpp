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
#include "rdbms/ConstraintInfo.hpp"

namespace cta {
namespace catalogue {

RdbmsPhysicalLibraryCatalogue::RdbmsPhysicalLibraryCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}


void RdbmsPhysicalLibraryCatalogue::createPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::PhysicalLibrary& pl) {
  try {
    auto conn = m_connPool->getConn();

    const uint64_t physicalLibraryId = getNextPhysicalLibraryId(conn);
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO PHYSICAL_LIBRARY("
        "PHYSICAL_LIBRARY_ID,"
        "PHYSICAL_LIBRARY_NAME,"
        "PHYSICAL_LIBRARY_MANUFACTURER,"
        "PHYSICAL_LIBRARY_MODEL,"
        "PHYSICAL_LIBRARY_TYPE,"
        "GUI_URL,"
        "WEBCAM_URL,"
        "PHYSICAL_LOCATION,"

        "NB_PHYSICAL_CARTRIDGE_SLOTS,"
        "NB_AVAILABLE_CARTRIDGE_SLOTS,"
        "NB_PHYSICAL_DRIVE_SLOTS,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME, "

        "USER_COMMENT) "
      "VALUES("
        ":PHYSICAL_LIBRARY_ID,"
        ":PHYSICAL_LIBRARY_NAME,"
        ":PHYSICAL_LIBRARY_MANUFACTURER,"
        ":PHYSICAL_LIBRARY_MODEL,"
        ":PHYSICAL_LIBRARY_TYPE,"
        ":GUI_URL,"
        ":WEBCAM_URL,"
        ":PHYSICAL_LOCATION,"

        ":NB_PHYSICAL_CARTRIDGE_SLOTS,"
        ":NB_AVAILABLE_CARTRIDGE_SLOTS,"
        ":NB_PHYSICAL_DRIVE_SLOTS,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME,"

        ":USER_COMMENT)";
    auto stmt = conn.createStmt(sql);

    auto setOptionalString = [&stmt](const std::string& sqlField, const std::optional<std::string>& optionalField) {
      if (optionalField && !optionalField.value().empty()) {
        stmt.bindString(sqlField, optionalField.value());
      } else {
        stmt.bindString(sqlField, std::nullopt);
      }
    };

    auto setOptionalUint = [&stmt](const std::string& sqlField, const std::optional<uint64_t>& optionalField) {
      if (optionalField) {
        stmt.bindUint64(sqlField, optionalField.value());
      } else {
        stmt.bindUint64(sqlField, std::nullopt);
      }
    };

    stmt.bindUint64(":PHYSICAL_LIBRARY_ID"          , physicalLibraryId);
    stmt.bindString(":PHYSICAL_LIBRARY_NAME"        , pl.name);
    stmt.bindString(":PHYSICAL_LIBRARY_MANUFACTURER", pl.manufacturer);
    stmt.bindString(":PHYSICAL_LIBRARY_MODEL"       , pl.model);
    setOptionalString(":PHYSICAL_LIBRARY_TYPE"      , pl.type);
    setOptionalString(":GUI_URL"                    , pl.guiUrl);
    setOptionalString(":WEBCAM_URL"                 , pl.webcamUrl);
    setOptionalString(":PHYSICAL_LOCATION"          , pl.location);

    stmt.bindUint64(":NB_PHYSICAL_CARTRIDGE_SLOTS" , pl.nbPhysicalCartridgeSlots);
    setOptionalUint(":NB_AVAILABLE_CARTRIDGE_SLOTS", pl.nbAvailableCartridgeSlots);
    stmt.bindUint64(":NB_PHYSICAL_DRIVE_SLOTS"     , pl.nbPhysicalDriveSlots);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME"     , now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME"     , now);

    setOptionalString(":USER_COMMENT", pl.comment);

    stmt.executeNonQuery();
  } catch(exception::UserError& ) {
    throw;
  } catch(cta::rdbms::UniqueConstraintError& ex) {
    std::stringstream err_stream;
    err_stream << "Unique constraint violation on creation of physical library " << pl.name << ": ";
    err_stream << rdbms::ConstraintInfo::constraintViolationMessage(rdbms::ConstraintInfo::CREATE, ex.getViolatedConstraintName());
    throw exception::UserError(err_stream.str());
  } catch(cta::rdbms::ConstraintError& ex) {
    std::stringstream err_stream;
    err_stream << "Constrain violation on creation of physical library " << pl.name << ": ";
    err_stream << rdbms::ConstraintInfo::constraintViolationMessage(rdbms::ConstraintInfo::CREATE, ex.getViolatedConstraintName());
    throw exception::UserError(err_stream.str());
  } catch(exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsPhysicalLibraryCatalogue::deletePhysicalLibrary(const std::string& name) {
  try {
    const char *const sql =
      "DELETE FROM PHYSICAL_LIBRARY "
      "WHERE "
        "PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":PHYSICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete physical library ") + name + " because it does not exist");
    }
  } catch(exception::UserError& ex) {
    throw;
  } catch(cta::rdbms::IntegrityConstraintError& ex) {
    std::stringstream err_stream;
    err_stream << "Integrity constraint violation on deletion of physical library " << name << ": ";
    err_stream << rdbms::ConstraintInfo::constraintViolationMessage(rdbms::ConstraintInfo::DELETE, ex.getViolatedConstraintName());
    throw exception::UserError(err_stream.str());
  } catch(cta::rdbms::ConstraintError& ex) {
    std::stringstream err_stream;
    err_stream << "Constrain violation on deletion of physical library " << name << ": ";
    err_stream << rdbms::ConstraintInfo::constraintViolationMessage(rdbms::ConstraintInfo::DELETE, ex.getViolatedConstraintName());
    throw exception::UserError(err_stream.str());
  } catch(exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<common::dataStructures::PhysicalLibrary> RdbmsPhysicalLibraryCatalogue::getPhysicalLibraries() const {
  try {
    std::list<common::dataStructures::PhysicalLibrary> libs;
    const char *const sql =
      "SELECT "
        "PHYSICAL_LIBRARY_NAME AS PHYSICAL_LIBRARY_NAME,"
        "PHYSICAL_LIBRARY_MANUFACTURER AS PHYSICAL_LIBRARY_MANUFACTURER,"
        "PHYSICAL_LIBRARY_MODEL AS PHYSICAL_LIBRARY_MODEL,"
        "PHYSICAL_LIBRARY_TYPE AS PHYSICAL_LIBRARY_TYPE,"
        "GUI_URL AS GUI_URL,"
        "WEBCAM_URL AS WEBCAM_URL,"
        "PHYSICAL_LOCATION AS PHYSICAL_LOCATION,"

        "NB_PHYSICAL_CARTRIDGE_SLOTS AS NB_PHYSICAL_CARTRIDGE_SLOTS,"
        "NB_AVAILABLE_CARTRIDGE_SLOTS AS NB_AVAILABLE_CARTRIDGE_SLOTS,"
        "NB_PHYSICAL_DRIVE_SLOTS AS NB_PHYSICAL_DRIVE_SLOTS,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME, "

        "USER_COMMENT AS USER_COMMENT "
      "FROM "
        "PHYSICAL_LIBRARY "
      "ORDER BY "
        "PHYSICAL_LIBRARY_NAME";
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
  } catch(exception::UserError& ) {
    throw;
  } catch(exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::UpdatePhysicalLibrary& pl) {
  try {
    const time_t now = time(nullptr);
    std::string setClause;

    if(pl.guiUrl)                    setClause += "GUI_URL = :GUI_URL,";
    if(pl.webcamUrl)                 setClause += "WEBCAM_URL = :WEBCAM_URL,";
    if(pl.location)                  setClause += "PHYSICAL_LOCATION = :PHYSICAL_LOCATION,";
    if(pl.nbPhysicalCartridgeSlots)  setClause += "NB_PHYSICAL_CARTRIDGE_SLOTS = :NB_PHYSICAL_CARTRIDGE_SLOTS,";
    if(pl.nbAvailableCartridgeSlots) setClause += "NB_AVAILABLE_CARTRIDGE_SLOTS = :NB_AVAILABLE_CARTRIDGE_SLOTS,";
    if(pl.nbPhysicalDriveSlots)      setClause += "NB_PHYSICAL_DRIVE_SLOTS = :NB_PHYSICAL_DRIVE_SLOTS,";
    if(pl.comment)                   setClause += "USER_COMMENT = :USER_COMMENT,";

    if(!setClause.empty()) {

      setClause += "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
                   "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
                   "LAST_UPDATE_TIME = :LAST_UPDATE_TIME ";

      std::string sql = "UPDATE PHYSICAL_LIBRARY SET ";
      sql += setClause;
      sql += "WHERE PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME";

      auto conn = m_connPool->getConn();
      auto stmt = conn.createStmt(sql);

      if(pl.guiUrl)                    stmt.bindString(":GUI_URL", pl.guiUrl.value());
      if(pl.webcamUrl)                 stmt.bindString(":WEBCAM_URL", pl.webcamUrl.value());
      if(pl.location)                  stmt.bindString(":PHYSICAL_LOCATION", pl.location.value());
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

      stmt.executeNonQuery();

      if (0 == stmt.getNbAffectedRows()) {
          throw exception::UserError(std::string("Cannot modify physical library ") + pl.name + " because it does not exist");
      }
    } else {
      throw exception::UserError("At least one value must be updated");
    }
  } catch(exception::UserError& ) {
    throw;
  } catch(cta::rdbms::UniqueConstraintError& ex) {
    std::ostringstream err_stream;
    err_stream << "Unique constraint violation on modification of physical library " << pl.name << ": ";
    err_stream << rdbms::ConstraintInfo::constraintViolationMessage(rdbms::ConstraintInfo::UPDATE, ex.getViolatedConstraintName());
    throw exception::UserError(err_stream.str());
  } catch(cta::rdbms::ConstraintError& ex) {
    std::ostringstream err_stream;
    err_stream << "Constraint violation on modification of physical library " << pl.name << ": ";
    err_stream << rdbms::ConstraintInfo::constraintViolationMessage(rdbms::ConstraintInfo::UPDATE, ex.getViolatedConstraintName());
    throw exception::UserError(err_stream.str());
  } catch(exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::optional<uint64_t> RdbmsPhysicalLibraryCatalogue::getPhysicalLibraryId(rdbms::Conn &conn,
  const std::string &name) const {
  try {
    const char *const sql =
      "SELECT "
        "PHYSICAL_LIBRARY_ID AS PHYSICAL_LIBRARY_ID "
      "FROM "
        "PHYSICAL_LIBRARY "
      "WHERE "
        "PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":PHYSICAL_LIBRARY_NAME", name);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      return std::nullopt;
    }
    return rset.columnUint64("PHYSICAL_LIBRARY_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace catalogue
} // namespace cta