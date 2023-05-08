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
        ":PHYSICAL_LIBRARY_MANUFACTURER,"

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
    stmt.bindString(":NB_PHYSICAL_CARTRIDGE_SLOTS", pl.nbPhysicalCartridgeSlots);
    stmt.bindString(":NB_PHYSICAL_DRIVE_SLOTS", pl.nbPhysicalDriveSlots);

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

void deletePhysicalLibrary(const std::string &name) {
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

std::list<common::dataStructures::PhysicalLibrary> getPhysicalLibraries() const override;

void modifyPhysicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) override;

void modifyPhysicalLibraryManufacturer(const common::dataStructures::SecurityIdentity &admin,
  const std::string& name, const std::string &manufacturer) override;

void modifyPhysicalLibraryModel(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &model) override;

void modifyPhysicalLibraryType     (const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &type) override;

void modifyPhysicalLibraryGuiUrl   (const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &guiUrl) override;

void modifyPhysicalLibraryWebcamUrl(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &webcamUrl) override;

void modifyPhysicalLibraryLocation (const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &location) override;

void modifyPhysicalLibraryNbPhysicalCartridgeSlots  (const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t &nbPhysicalCartridgeSlots) override;

void modifyPhysicalLibraryNbAvailableCartridgeSlots (const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t &nbAvailableCartridgeSlots) override;

void modifyPhysicalLibraryNbPhysicalDriveSlots (const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t &nbPhysicalDriveSlots) override;

void modifyPhysicalLibraryComment (const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) override;

} // namespace catalogue
} // namespace cta