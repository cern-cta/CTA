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

#include "catalogue/rdbms/RdbmsSchemaCatalogue.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "common/Constants.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {

RdbmsSchemaCatalogue::RdbmsSchemaCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool):
  m_log(log), m_connPool(connPool) {}

SchemaVersion RdbmsSchemaCatalogue::getSchemaVersion() const {
  try {
    std::map<std::string, uint64_t> schemaVersion;
    const char *const sql =
      "SELECT "
        "CTA_CATALOGUE.SCHEMA_VERSION_MAJOR AS SCHEMA_VERSION_MAJOR,"
        "CTA_CATALOGUE.SCHEMA_VERSION_MINOR AS SCHEMA_VERSION_MINOR,"
        "CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MAJOR AS NEXT_SCHEMA_VERSION_MAJOR,"
        "CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MINOR AS NEXT_SCHEMA_VERSION_MINOR,"
        "CTA_CATALOGUE.STATUS AS STATUS "
      "FROM "
        "CTA_CATALOGUE";

    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();

    if(rset.next()) {
      SchemaVersion::Builder schemaVersionBuilder;
      schemaVersionBuilder.schemaVersionMajor(rset.columnUint64("SCHEMA_VERSION_MAJOR"))
                          .schemaVersionMinor(rset.columnUint64("SCHEMA_VERSION_MINOR"))
                          .status(rset.columnString("STATUS"));
      auto schemaVersionMajorNext = rset.columnOptionalUint64("NEXT_SCHEMA_VERSION_MAJOR");
      auto schemaVersionMinorNext = rset.columnOptionalUint64("NEXT_SCHEMA_VERSION_MINOR");
      if(schemaVersionMajorNext && schemaVersionMinorNext){
        schemaVersionBuilder.nextSchemaVersionMajor(schemaVersionMajorNext.value())
                            .nextSchemaVersionMinor(schemaVersionMinorNext.value());
      }
      return schemaVersionBuilder.build();
    } else {
      throw exception::Exception("CTA_CATALOGUE does not contain any row");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsSchemaCatalogue::verifySchemaVersion() {
  try {
    SchemaVersion schemaVersion = getSchemaVersion();
    /*if(auto [major, minor] = schemaVersion.getSchemaVersion<SchemaVersion::MajorMinor>();
            major != static_cast<uint64_t>(CTA_CATALOGUE_SCHEMA_VERSION_MAJOR)){
      std::ostringstream exceptionMsg;
      exceptionMsg << "Catalogue schema MAJOR version differ : Database schema version is "
                   << major << "." << minor
                   << ", CTA schema version is " << CTA_CATALOGUE_SCHEMA_VERSION_MAJOR << "."
                   << CTA_CATALOGUE_SCHEMA_VERSION_MINOR;
      throw WrongSchemaVersionException(exceptionMsg.str());
    }*/
    if(auto [major, minor] = schemaVersion.getSchemaVersion<SchemaVersion::MajorMinor>();
      major > static_cast<uint64_t>(CTA_CATALOGUE_SCHEMA_VERSION_MAJOR)){
      std::ostringstream exceptionMsg;
      exceptionMsg << "Catalogue schema MAJOR version differ : Database schema version "
                   << major << "." << minor << " is above CTA schema version "
                   << CTA_CATALOGUE_SCHEMA_VERSION_MAJOR << "." << CTA_CATALOGUE_SCHEMA_VERSION_MINOR;
      throw WrongSchemaVersionException(exceptionMsg.str());
    }
    if(schemaVersion.getStatus<SchemaVersion::Status>() == SchemaVersion::Status::UPGRADING){
      std::ostringstream exceptionMsg;
      exceptionMsg << "Catalogue schema is in status " + schemaVersion.getStatus<std::string>()
        + ", next schema version is " << schemaVersion.getSchemaVersionNext<std::string>();
    }
  } catch (exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// ping
//------------------------------------------------------------------------------
void RdbmsSchemaCatalogue::ping() {
  try {
    verifySchemaVersion();
  } catch (WrongSchemaVersionException &){
    throw;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> RdbmsSchemaCatalogue::getTableNames() const {
  auto conn = m_connPool->getConn();
  return conn.getTableNames();
}


} // namespace catalogue
} // namespace cta