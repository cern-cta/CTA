/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/DropSchemaCmd.hpp"
#include "catalogue/DropSchemaCmdLineArgs.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/ConnPool.hpp"

#include <algorithm>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DropSchemaCmd::DropSchemaCmd(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream):
  CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
DropSchemaCmd::~DropSchemaCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int DropSchemaCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const DropSchemaCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const rdbms::Login dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t maxNbConns = 1;
  rdbms::ConnPool connPool(dbLogin, maxNbConns);
  auto conn = connPool.getConn();

  // Abort if the schema is already dropped
  if(conn.getTableNames().empty() && conn.getSequenceNames().empty()) {
    m_out << "Database contains no tables and no sequences." << std::endl <<
      "Assuming the schema has already been dropped." << std::endl;
    return 0;
  }

  if(userConfirmsDropOfSchema(dbLogin)) {
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
bool DropSchemaCmd::userConfirmsDropOfSchema(const rdbms::Login &dbLogin) {
  m_out << "WARNING" << std::endl;
  m_out << "You are about to drop the schema of the CTA calalogue database" << std::endl;
  m_out << "    Database name: " << dbLogin.database << std::endl;
  m_out << "Are you sure you want to continue?" << std::endl;

  std::string userResponse;
  while(userResponse != "yes" && userResponse != "no") {
    m_out << "Please type either \"yes\" or \"no\" > ";
    std::getline(m_in, userResponse);
  }

  return userResponse == "yes";
}

//------------------------------------------------------------------------------
// dropCatalogueSchema
//------------------------------------------------------------------------------
void DropSchemaCmd::dropCatalogueSchema(const rdbms::Login::DbType &dbType, rdbms::Conn &conn) {
  try {
    switch(dbType) {
    case rdbms::Login::DBTYPE_IN_MEMORY:
    case rdbms::Login::DBTYPE_SQLITE:
      dropSqliteCatalogueSchema(conn);
      break;
    case rdbms::Login::DBTYPE_MYSQL:
      dropMysqlCatalogueSchema(conn);
      break;
    case rdbms::Login::DBTYPE_POSTGRESQL:
      dropPostgresCatalogueSchema(conn);
      break;
    case rdbms::Login::DBTYPE_ORACLE:
      dropOracleCatalogueSchema(conn);
      break;
    case rdbms::Login::DBTYPE_NONE:
      throw exception::Exception("Cannot delete the schema of  catalogue database without a database type");
    default:
      {
        exception::Exception ex;
        ex.getMessage() << "Unknown database type: value=" << dbType;
        throw ex;
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// dropSqliteCatalogueSchema
//------------------------------------------------------------------------------
void DropSchemaCmd::dropSqliteCatalogueSchema(rdbms::Conn &conn) {
  try {
    std::list<std::string> tablesInDb = conn.getTableNames();
    std::list<std::string> tablesToDrop = {
      "CTA_CATALOGUE",
      "ARCHIVE_ROUTE",
      "TAPE_FILE",
      "TEMP_TAPE_FILE",
      "DATABASECHANGELOGLOCK", /* Liquibase specific table */
      "DATABASECHANGELOG", /* Liquibase specific table */
      "TAPE_FILE_RECYCLE_BIN",
      "ARCHIVE_FILE_RECYCLE_BIN",
      "ARCHIVE_FILE",
      "ARCHIVE_FILE_ID",
      "TAPE",
      "MEDIA_TYPE",
      "MEDIA_TYPE_ID",
      "REQUESTER_MOUNT_RULE",
      "REQUESTER_GROUP_MOUNT_RULE",
      "ADMIN_USER",
      "STORAGE_CLASS",
      "STORAGE_CLASS_ID",
      "TAPE_POOL",
      "TAPE_POOL_ID",
      "VIRTUAL_ORGANIZATION",
      "VIRTUAL_ORGANIZATION_ID",
      "LOGICAL_LIBRARY",
      "LOGICAL_LIBRARY_ID",
      "MOUNT_POLICY",
      "ACTIVITIES_WEIGHTS",
      "USAGESTATS",
      "EXPERIMENTS",
      "DISK_SYSTEM"
    };
    dropDatabaseTables(conn, tablesToDrop);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// dropMysqlCatalogueSchema
//------------------------------------------------------------------------------
void DropSchemaCmd::dropMysqlCatalogueSchema(rdbms::Conn &conn) {
  try {
    std::list<std::string> tablesInDb = conn.getTableNames();
    std::list<std::string> tablesToDrop = {
      "CTA_CATALOGUE",
      "ARCHIVE_ROUTE",
      "TAPE_FILE",
      "TEMP_TAPE_FILE",
      "DATABASECHANGELOGLOCK", /* Liquibase specific table */
      "DATABASECHANGELOG", /* Liquibase specific table */
      "FILE_RECYCLE_BIN",
      "TAPE_FILE_RECYCLE_BIN",
      "ARCHIVE_FILE_RECYCLE_BIN",
      "ARCHIVE_FILE",
      "ARCHIVE_FILE_ID",
      "TAPE",
      "MEDIA_TYPE",
      "MEDIA_TYPE_ID",
      "REQUESTER_MOUNT_RULE",
      "REQUESTER_GROUP_MOUNT_RULE",
      "ADMIN_USER",
      "STORAGE_CLASS",
      "STORAGE_CLASS_ID",
      "TAPE_POOL",
      "TAPE_POOL_ID",
      "VIRTUAL_ORGANIZATION",
      "VIRTUAL_ORGANIZATION_ID",
      "LOGICAL_LIBRARY",
      "LOGICAL_LIBRARY_ID",
      "MOUNT_POLICY",
      "ACTIVITIES_WEIGHTS",
      "USAGESTATS",
      "EXPERIMENTS",
      "DISK_SYSTEM"
    };
    dropDatabaseTables(conn, tablesToDrop);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}


//------------------------------------------------------------------------------
// dropDatabaseTables
//------------------------------------------------------------------------------
void DropSchemaCmd::dropDatabaseTables(rdbms::Conn &conn, const std::list<std::string> &tablesToDrop) {
  try {
    std::list<std::string> tablesInDb = conn.getTableNames();
    for(auto tableToDrop : tablesToDrop) {
      const bool tableToDropIsInDb = tablesInDb.end() != std::find(tablesInDb.begin(), tablesInDb.end(), tableToDrop);
      if(tableToDropIsInDb) {
        conn.executeNonQuery(std::string("DROP TABLE ") + tableToDrop);
        m_out << "Dropped table " << tableToDrop << std::endl;
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// dropOracleCatalogueSchema
//------------------------------------------------------------------------------
void DropSchemaCmd::dropOracleCatalogueSchema(rdbms::Conn &conn) {
  try {
    std::list<std::string> tablesInDb = conn.getTableNames();
    std::list<std::string> tablesToDrop = {
      "CTA_CATALOGUE",
      "ARCHIVE_ROUTE",
      "TAPE_FILE",
      "FILE_RECYCLE_BIN",
      "ARCHIVE_FILE",
      "TAPE_FILE_RECYCLE_BIN",
      "ARCHIVE_FILE_RECYCLE_BIN",
      "TAPE",
      "MEDIA_TYPE",
      "TEMP_TAPE_FILE_BATCH",
      "TEMP_TAPE_FILE_INSERTION_BATCH",
      "TEMP_TAPE_FILE",
      "DATABASECHANGELOGLOCK", /* Liquibase specific table */
      "DATABASECHANGELOG", /* Liquibase specific table */
      "TEMP_REMOVE_CASTOR_METADATA",
      "REQUESTER_MOUNT_RULE",
      "REQUESTER_GROUP_MOUNT_RULE",
      "ADMIN_USER",
      "ADMIN_HOST",
      "STORAGE_CLASS",
      "TAPE_POOL",
      "VIRTUAL_ORGANIZATION",
      "LOGICAL_LIBRARY",
      "MOUNT_POLICY",
      "ACTIVITIES_WEIGHTS",
      "USAGESTATS",
      "EXPERIMENTS",
      "DISK_SYSTEM"
    };

    dropDatabaseTables(conn, tablesToDrop);

    std::list<std::string> sequencesToDrop = {
      "ARCHIVE_FILE_ID_SEQ",
      "LOGICAL_LIBRARY_ID_SEQ",
      "MEDIA_TYPE_ID_SEQ",
      "STORAGE_CLASS_ID_SEQ",
      "TAPE_POOL_ID_SEQ",
      "VIRTUAL_ORGANIZATION_ID_SEQ"
    };
    dropDatabaseSequences(conn, sequencesToDrop);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// dropPostgresCatalogueSchema
//------------------------------------------------------------------------------
void DropSchemaCmd::dropPostgresCatalogueSchema(rdbms::Conn &conn) {
  try {
    std::list<std::string> tablesInDb = conn.getTableNames();
    std::list<std::string> tablesToDrop = {
      "CTA_CATALOGUE",
      "ARCHIVE_ROUTE",
      "TAPE_FILE",
      "TEMP_TAPE_FILE",
      "DATABASECHANGELOGLOCK", /* Liquibase specific table */
      "DATABASECHANGELOG", /* Liquibase specific table */
      "FILE_RECYCLE_BIN",
      "ARCHIVE_FILE",
      "TAPE_FILE_RECYCLE_BIN",
      "ARCHIVE_FILE_RECYCLE_BIN",
      "TAPE",
      "MEDIA_TYPE",
      "REQUESTER_MOUNT_RULE",
      "REQUESTER_GROUP_MOUNT_RULE",
      "ADMIN_USER",
      "ADMIN_HOST",
      "STORAGE_CLASS",
      "TAPE_POOL",
      "VIRTUAL_ORGANIZATION",
      "LOGICAL_LIBRARY",
      "MOUNT_POLICY",
      "ACTIVITIES_WEIGHTS",
      "USAGESTATS",
      "EXPERIMENTS",
      "DISK_SYSTEM"
    };

    dropDatabaseTables(conn, tablesToDrop);

    std::list<std::string> sequencesToDrop = {
      "ARCHIVE_FILE_ID_SEQ",
      "LOGICAL_LIBRARY_ID_SEQ",
      "MEDIA_TYPE_ID_SEQ",
      "STORAGE_CLASS_ID_SEQ",
      "TAPE_POOL_ID_SEQ",
      "VIRTUAL_ORGANIZATION_ID_SEQ"
    };
    dropDatabaseSequences(conn, sequencesToDrop);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// dropDatabaseSequences
//------------------------------------------------------------------------------
void DropSchemaCmd::dropDatabaseSequences(rdbms::Conn &conn, const std::list<std::string> &sequencesToDrop) {
  try {
    std::list<std::string> sequencesInDb = conn.getSequenceNames();
    for(auto sequenceToDrop : sequencesToDrop) {
      const bool sequenceToDropIsInDb = sequencesInDb.end() != std::find(sequencesInDb.begin(), sequencesInDb.end(),
        sequenceToDrop);
      if(sequenceToDropIsInDb) {
        conn.executeNonQuery(std::string("DROP SEQUENCE ") + sequenceToDrop);
        m_out << "Dropped sequence " << sequenceToDrop << std::endl;
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void DropSchemaCmd::printUsage(std::ostream &os) {
  DropSchemaCmdLineArgs::printUsage(os);
}

} // namespace catalogue
} // namespace cta
