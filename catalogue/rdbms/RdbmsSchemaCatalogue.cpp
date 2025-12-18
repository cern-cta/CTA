/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/RdbmsSchemaCatalogue.hpp"

#include "catalogue/SchemaVersion.hpp"
#include "common/Constants.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/ConnPool.hpp"

#include <iterator>
#include <set>
#include <string>
#include <vector>

namespace cta::catalogue {

RdbmsSchemaCatalogue::RdbmsSchemaCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool)
    : m_log(log),
      m_connPool(connPool) {}

SchemaVersion RdbmsSchemaCatalogue::getSchemaVersion() const {
  std::map<std::string, uint64_t, std::less<>> schemaVersion;
  const char* const sql = R"SQL(
    SELECT
      CTA_CATALOGUE.SCHEMA_VERSION_MAJOR AS SCHEMA_VERSION_MAJOR,
      CTA_CATALOGUE.SCHEMA_VERSION_MINOR AS SCHEMA_VERSION_MINOR,
      CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MAJOR AS NEXT_SCHEMA_VERSION_MAJOR,
      CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MINOR AS NEXT_SCHEMA_VERSION_MINOR,
      CTA_CATALOGUE.STATUS AS STATUS
    FROM
      CTA_CATALOGUE
  )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  if (rset.next()) {
    SchemaVersion::Builder schemaVersionBuilder;
    schemaVersionBuilder.schemaVersionMajor(rset.columnUint64("SCHEMA_VERSION_MAJOR"))
      .schemaVersionMinor(rset.columnUint64("SCHEMA_VERSION_MINOR"))
      .status(rset.columnString("STATUS"));
    {
      auto schemaVersionMajorNext = rset.columnOptionalUint64("NEXT_SCHEMA_VERSION_MAJOR");
      auto schemaVersionMinorNext = rset.columnOptionalUint64("NEXT_SCHEMA_VERSION_MINOR");
      if (schemaVersionMajorNext.has_value() && schemaVersionMinorNext.has_value()) {
        schemaVersionBuilder.nextSchemaVersionMajor(schemaVersionMajorNext.value())
          .nextSchemaVersionMinor(schemaVersionMinorNext.value());
      }
    }
    return schemaVersionBuilder.build();
  } else {
    throw exception::Exception("CTA_CATALOGUE does not contain any row");
  }
}

void RdbmsSchemaCatalogue::verifySchemaVersion() {
  const std::set<int> supported_versions {SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS_ARRAY.begin(),
                                          SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS_ARRAY.end()};
  SchemaVersion schemaVersion = getSchemaVersion();
  if (const auto [major, minor] = schemaVersion.getSchemaVersion<SchemaVersion::MajorMinor>();
      !supported_versions.contains(static_cast<int>(major))) {
    std::ostringstream exceptionMsg;
    std::ostringstream supported_versions_os;
    std::copy(supported_versions.begin(),
              supported_versions.end(),
              std::ostream_iterator<int>(supported_versions_os, ", "));
    exceptionMsg << "Catalogue schema MAJOR version not supported : Database schema version is " << major << "."
                 << minor << ", supported CTA MAJOR versions are {" << supported_versions_os.str() << "}.";
    throw WrongSchemaVersionException(exceptionMsg.str());
  }
  if (schemaVersion.getStatus<SchemaVersion::Status>() == SchemaVersion::Status::UPGRADING) {
    std::ostringstream exceptionMsg;
    exceptionMsg << "Catalogue schema is in status " + schemaVersion.getStatus<std::string>()
                      + ", next schema version is "
                 << schemaVersion.getSchemaVersionNext<std::string>();
  }
}

//------------------------------------------------------------------------------
// ping
//------------------------------------------------------------------------------
void RdbmsSchemaCatalogue::ping() {
  verifySchemaVersion();
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> RdbmsSchemaCatalogue::getTableNames() const {
  auto conn = m_connPool->getConn();
  return conn.getTableNames();
}

}  // namespace cta::catalogue
