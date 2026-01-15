/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/DropSchemaCmd.hpp"

#include "catalogue/DropSchemaCmdLineArgs.hpp"
#include "catalogue/SchemaChecker.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/ConnPool.hpp"

#include <algorithm>

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DropSchemaCmd::DropSchemaCmd(std::istream& inStream, std::ostream& outStream, std::ostream& errStream)
    : CmdLineTool(inStream, outStream, errStream) {}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int DropSchemaCmd::exceptionThrowingMain(const int argc, char* const* const argv) {
  const DropSchemaCmdLineArgs cmdLineArgs(argc, argv);

  if (cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const rdbms::Login dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t maxNbConns = 1;
  rdbms::ConnPool connPool(dbLogin, maxNbConns);
  auto conn = connPool.getConn();

  // Abort if the schema is already dropped
  if (conn.getTableNames().empty() && conn.getSequenceNames().empty()) {
    m_out << "Database contains no tables and no sequences." << std::endl
          << "Assuming the schema has already been dropped." << std::endl;
    return 0;
  }

  if (isProductionSet(conn)) {
    throw cta::exception::Exception(
      "Cannot drop a production database. If you still wish to proceed then please modify the database manually to "
      "remove its production status before trying again.");
  }

  if (userConfirmsDropOfSchema(dbLogin)) {
    m_out << "DROPPING the schema of the CTA calalogue database" << std::endl;
    dropCatalogueSchema(dbLogin.dbType, conn);
  } else {
    m_out << "Aborting" << std::endl;
  }

  return 0;
}

//------------------------------------------------------------------------------
// userConfirmsDropOfSchema
//------------------------------------------------------------------------------
bool DropSchemaCmd::userConfirmsDropOfSchema(const rdbms::Login& dbLogin) {
  m_out << "WARNING" << std::endl;

  m_out << "You are about to drop ALL tables and sequences from the following database:" << std::endl;
  m_out << "    Database: " << dbLogin.connectionString << std::endl;
  m_out << "Are you sure you want to continue?" << std::endl;

  std::string userResponse;
  while (userResponse != "yes" && userResponse != "no") {
    m_out << R"(Please type either "yes" or "no" > )";
    std::getline(m_in, userResponse);
  }

  return userResponse == "yes";
}

//------------------------------------------------------------------------------
// dropCatalogueSchema
//------------------------------------------------------------------------------
void DropSchemaCmd::dropCatalogueSchema(const rdbms::Login::DbType& dbType, rdbms::Conn& conn) {
  switch (dbType) {
    case rdbms::Login::DBTYPE_IN_MEMORY:
      throw exception::Exception("Dropping the schema of an in_memory database is not supported");
    case rdbms::Login::DBTYPE_SQLITE:
      throw exception::Exception("Dropping the schema of an sqlite database is not supported");
    case rdbms::Login::DBTYPE_NONE:
      throw exception::Exception("Cannot delete the schema of catalogue database without a database type");
    default:
      dropDatabaseSequences(conn);
      dropDatabaseTables(conn);
  }
}

//------------------------------------------------------------------------------
// dropSingleTable
//------------------------------------------------------------------------------
bool DropSchemaCmd::dropSingleTable(rdbms::Conn& conn, const std::string& tableName) {
  try {
    conn.executeNonQuery(std::string("DROP TABLE ") + tableName);
    m_out << "Dropped table " << tableName << std::endl;
    return true;
  } catch (exception::Exception&) {
    return false;
  }
}

//------------------------------------------------------------------------------
// dropDatabaseTables
//------------------------------------------------------------------------------
void DropSchemaCmd::dropDatabaseTables(rdbms::Conn& conn) {
  bool droppedAtLeastOneTable = true;
  while (droppedAtLeastOneTable) {
    droppedAtLeastOneTable = false;
    auto tables = conn.getTableNames();
    std::erase_if(tables, [](const std::string& tableName) {
      return tableName == "CTA_CATALOGUE";
    });  // Remove CTA_CATALOGUE to drop it at the end
    for (const auto& table : tables) {
      droppedAtLeastOneTable |= dropSingleTable(conn, table);
    }
  }

  // Drop CTA_CATALOGUE table
  auto tables = conn.getTableNames();
  if (tables.size() > 1) {
    throw exception::Exception("Failed to delete all tables, except CTA_CATALOGUE.");
  }
  dropSingleTable(conn, "CTA_CATALOGUE");

  tables = conn.getTableNames();
  if (!tables.empty()) {
    throw exception::Exception("Failed to delete all tables.  Maybe there is a circular dependency.");
  }
}

//------------------------------------------------------------------------------
// dropDatabaseSequences
//------------------------------------------------------------------------------
void DropSchemaCmd::dropDatabaseSequences(rdbms::Conn& conn) {
  auto sequences = conn.getSequenceNames();
  for (const auto& sequence : sequences) {
    conn.executeNonQuery(std::string("DROP SEQUENCE ") + sequence);
    m_out << "Dropped sequence " << sequence << std::endl;
  }
}

//------------------------------------------------------------------------------
// isProductionSet
//------------------------------------------------------------------------------
bool DropSchemaCmd::isProductionSet(cta::rdbms::Conn& conn) {
  // Check if the CTA_CATALOGUE table exists
  if (const auto tables = conn.getTableNames();
      std::find(tables.begin(), tables.end(), "CTA_CATALOGUE") == tables.end()) {
    return false;
  }

  const char* const sql = R"SQL(
    SELECT CTA_CATALOGUE.IS_PRODUCTION AS IS_PRODUCTION FROM CTA_CATALOGUE
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (rset.next()) {
    return rset.columnBool("IS_PRODUCTION");
  } else {
    return false;  // The table is empty
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void DropSchemaCmd::printUsage(std::ostream& os) {
  DropSchemaCmdLineArgs::printUsage(os);
}

}  // namespace cta::catalogue
