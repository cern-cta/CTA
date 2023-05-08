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

namespace cta {
namespace catalogue {

RdbmsPhysicalLibraryCatalogue::RdbmsPhysicalLibraryCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}


void RdbmsPhysicalLibraryCatalogue::createPhysicalLibrary(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::PhysicalLibrary& pl) {
  try {
    auto conn = m_connPool->getConn();
    if(RdbmsCatalogueUtils::physicalLibraryExists(conn, pl.name)) {
      throw exception::UserError(std::string("Cannot create physical library ") + pl.name +
        " because a physical library with the same name already exists");
    }
    const uint64_t physicalLibraryId = getNextPhysicalLibraryId(conn);
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO PHYSICAL_LIBRARY("
        "PHYSICAL_LIBRARY_ID,"
        "PHYSICAL_LIBRARY_NAME,"
        "PHYSICAL_LIBRARY_MANUFACTURER,"
        "PHYSICAL_LIBRARY_MODEL,"

        "NB_PHYSICAL_CARTRIDGE_SLOTS,"
        "NB_PHYSICAL_DRIVE_SLOTS,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":PHYSICAL_LIBRARY_ID,"
        ":PHYSICAL_LIBRARY_NAME,"
        ":PHYSICAL_LIBRARY_MANUFACTURER,"
        ":PHYSICAL_LIBRARY_MODEL,"

        ":NB_PHYSICAL_CARTRIDGE_SLOTS,"
        ":NB_PHYSICAL_DRIVE_SLOTS,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindUint64(":PHYSICAL_LIBRARY_ID", physicalLibraryId);
    stmt.bindString(":PHYSICAL_LIBRARY_NAME", pl.name);
    stmt.bindString(":PHYSICAL_LIBRARY_MANUFACTURER", pl.manufacturer);
    stmt.bindString(":PHYSICAL_LIBRARY_MODEL", pl.model);
    stmt.bindUint64(":NB_PHYSICAL_CARTRIDGE_SLOTS", pl.nbPhysicalCartridgeSlots);
    stmt.bindUint64(":NB_PHYSICAL_DRIVE_SLOTS", pl.nbPhysicalDriveSlots);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsPhysicalLibraryCatalogue::deletePhysicalLibrary(const std::string &name) {
  try {
    const char *const sql =
      "DELETE FROM PHYSICAL_LIBRARY "
      "WHERE "
        "PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME AND "
        "NOT EXISTS ("
          "SELECT "
            "LOGICAL_LIBRARY.PHYSICAL_LIBRARY_ID "
          "FROM "
            "LOGICAL_LIBRARY "
          "WHERE "
            "LOGICAL_LIBRARY.PHYSICAL_LIBRARY_ID = PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_ID)";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":PHYSICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the physical library does not exist or if it still contains tapes
    if(0 == stmt.getNbAffectedRows()) {
      if(RdbmsCatalogueUtils::physicalLibraryExists(conn, name)) {
        throw UserSpecifiedANonEmptyLogicalLibrary(std::string("Cannot delete physical library ") + name +
          " because it contains one or more tapes");
      } else {
        throw UserSpecifiedANonExistentLogicalLibrary(std::string("Cannot delete physical library ") + name +
          " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
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
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
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

      pl.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      pl.creationLog.host     = rset.columnString("CREATION_LOG_HOST_NAME");
      pl.creationLog.time     = rset.columnUint64("CREATION_LOG_TIME");

      pl.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      pl.lastModificationLog.host     = rset.columnString("LAST_UPDATE_HOST_NAME");
      pl.lastModificationLog.time     = rset.columnUint64("LAST_UPDATE_TIME");

      libs.push_back(pl);
    }

    return libs;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  try {
    if(currentName.empty()) {
      throw UserSpecifiedAnEmptyStringPhysicalLibraryName(
        "Cannot modify physical library because the physical library name is an empty string");
    }

    if(newName.empty()) {
      throw UserSpecifiedAnEmptyStringPhysicalLibraryName(
        "Cannot modify physical library because the new name is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE PHYSICAL_LIBRARY SET "
        "PHYSICAL_LIBRARY_NAME = :NEW_PHYSICAL_LIBRARY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "PHYSICAL_LIBRARY_NAME = :CURRENT_PHYSICAL_LIBRARY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":NEW_PHYSICAL_LIBRARY_NAME", newName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":CURRENT_PHYSICAL_LIBRARY_NAME", currentName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify physical library ") + currentName
        + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryManufacturer(const common::dataStructures::SecurityIdentity &admin,
  const std::string& name, const std::string &manufacturer) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE PHYSICAL_LIBRARY SET "
        "PHYSICAL_LIBRARY_MANUFACTURER = :PHYSICAL_LIBRARY_MANUFACTURER,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":PHYSICAL_LIBRARY_MANUFACTURER", manufacturer);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":PHYSICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify physical library ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryModel(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &model) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE PHYSICAL_LIBRARY SET "
        "PHYSICAL_LIBRARY_MODEL = :PHYSICAL_LIBRARY_MODEL,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "PHYSICAL_LIBRARY_NAME = :PHYSICAL_LIBRARY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":PHYSICAL_LIBRARY_MODEL", model);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":PHYSICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify physical library ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryType(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &type) {}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryGuiUrl(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &guiUrl) {}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryWebcamUrl(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &webcamUrl) {}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryLocation(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &location) {}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryNbPhysicalCartridgeSlots(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t &nbPhysicalCartridgeSlots) {}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryNbAvailableCartridgeSlots(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t &nbAvailableCartridgeSlots) {}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryNbPhysicalDriveSlots(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t &nbPhysicalDriveSlots) {}

void RdbmsPhysicalLibraryCatalogue::modifyPhysicalLibraryComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {}

} // namespace catalogue
} // namespace cta