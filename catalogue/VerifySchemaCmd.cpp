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

#include "catalogue/VerifySchemaCmd.hpp"
#include "catalogue/VerifySchemaCmdLineArgs.hpp"
#include "catalogue/MysqlCatalogueSchema.hpp"
#include "catalogue/OracleCatalogueSchema.hpp"
#include "catalogue/PostgresCatalogueSchema.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "common/log/DummyLogger.hpp"
#include "Catalogue.hpp"

#include <algorithm>
#include <map>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifySchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
VerifySchemaCmd::~VerifySchemaCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int VerifySchemaCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const VerifySchemaCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const auto login = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t maxNbConns = 1;
  rdbms::ConnPool connPool(login, maxNbConns);
  auto conn = connPool.getConn();;

  const bool ctaCatalogueTableExists = tableExists("CTA_CATALOGUE", conn);

  if(!ctaCatalogueTableExists) {
    std::cerr << "Cannot verify the database schema because the CTA_CATALOGUE table does not exists" << std::endl;
    return 1;
  }
    
  std::unique_ptr<CatalogueSchema> schema;
  switch(login.dbType) {
  case rdbms::Login::DBTYPE_IN_MEMORY:
  case rdbms::Login::DBTYPE_SQLITE:
    schema.reset(new SqliteCatalogueSchema);
    break;
  case rdbms::Login::DBTYPE_POSTGRESQL:
    schema.reset(new PostgresCatalogueSchema);
    break;
  case rdbms::Login::DBTYPE_MYSQL:
    schema.reset(new MysqlCatalogueSchema);
    break;
  case rdbms::Login::DBTYPE_ORACLE:
    schema.reset(new OracleCatalogueSchema);
    break;
  case rdbms::Login::DBTYPE_NONE:
    throw exception::Exception("Cannot verify a catalogue without a database type");
  default:
    {
      exception::Exception ex;
      ex.getMessage() << "Unknown database type: value=" << login.dbType;
      throw ex;
    }
  }

  if (nullptr == schema) {
    exception::Exception ex;
    ex.getMessage() << "The catalogue schema uninitialized";
      throw ex;
  }
  
  std::cerr << "Checking schema version..." << std::endl;
  log::DummyLogger dummyLog("dummy", "dummy");
  const auto catalogueFactory = CatalogueFactoryFactory::create(dummyLog, login, maxNbConns, maxNbConns);
  const auto catalogue = catalogueFactory->create();  
  const auto schemaDbVersion = catalogue->getSchemaVersion();
  const auto schemaVersion = schema->getSchemaVersion(); 
  const VerifyStatus verifySchemaStatus = verifySchemaVersion(schemaVersion, schemaDbVersion);

  std::cerr << "Checking table names..." << std::endl;
  const auto schemaTableNames = schema->getSchemaTableNames();
  const auto dbTableNames = conn.getTableNames();
  const VerifyStatus verifyTablesStatus = verifyTableNames(schemaTableNames, dbTableNames);
  
  std::cerr << "Checking columns in tables..." << std::endl;
  const VerifyStatus verifyColumnsStatus = verifyColumns(schemaTableNames, dbTableNames, *schema, conn);

  std::cerr << "Checking index names..." << std::endl;
  const auto schemaIndexNames = schema->getSchemaIndexNames();
  const auto dbIndexNames = conn.getIndexNames();
  const VerifyStatus verifyIndexesStatus = verifyIndexNames(schemaIndexNames, dbIndexNames);

  std::cerr << "Checking sequence names..." << std::endl;
  const auto schemaSequenceNames = schema->getSchemaSequenceNames();
  const auto dbSequenceNames = conn.getSequenceNames();
  const VerifyStatus verifySequencesStatus = verifySequenceNames(schemaSequenceNames, dbSequenceNames);

  std::cerr << "Checking trigger names..." << std::endl;
  const auto schemaTriggerNames = schema->getSchemaTriggerNames();
  const auto dbTriggerNames = conn.getTriggerNames();
  const VerifyStatus verifyTriggersStatus = verifyTriggerNames(schemaTriggerNames, dbTriggerNames);
  
  if (verifySchemaStatus    == VerifyStatus::ERROR ||
      verifyTablesStatus    == VerifyStatus::ERROR || 
      verifyIndexesStatus   == VerifyStatus::ERROR ||
      verifySequencesStatus == VerifyStatus::ERROR || 
      verifyTriggersStatus  == VerifyStatus::ERROR ||
      verifyColumnsStatus   == VerifyStatus::ERROR ) {
    return 1;
  }
  return 0;
}

//------------------------------------------------------------------------------
// tableExists
//------------------------------------------------------------------------------
bool VerifySchemaCmd::tableExists(const std::string tableName, rdbms::Conn &conn) const {
  const auto names = conn.getTableNames();
  for(auto &name : names) {
    if(tableName == name) {
      return true;
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void VerifySchemaCmd::printUsage(std::ostream &os) {
  VerifySchemaCmdLineArgs::printUsage(os);
}

//------------------------------------------------------------------------------
// verifySchemaVersion
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifyStatus VerifySchemaCmd::verifySchemaVersion(const std::map<std::string, uint64_t> &schemaVersion, 
  const std::map<std::string, uint64_t> &schemaDbVersion) const {
  try {
    VerifyStatus status = VerifyStatus::UNKNOWN;
    for (const auto &schemaInfo : schemaVersion) {
      if (schemaDbVersion.count(schemaInfo.first)) {
        if (schemaInfo.second != schemaDbVersion.at(schemaInfo.first)) {
          std::cerr << "  ERROR: the schema version mismatch: SCHEMA[" 
                    << schemaInfo.first << "] = "  << schemaInfo.second 
                    << ", DB[" << schemaInfo.first << "] = " 
                    << schemaDbVersion.at(schemaInfo.first) << std::endl;
          status = VerifyStatus::ERROR;
        }
      } else {
        std::cerr << "  ERROR: the schema version value for " << schemaInfo.first 
                  <<"  not found in the Catalogue DB" << std::endl;
        status = VerifyStatus::ERROR;
      }
    }
    if (status != VerifyStatus::INFO && status != VerifyStatus::ERROR) {
      std::cerr << "  OK" << std::endl;
      status = VerifyStatus::OK;
    }
    return status;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// verifyColumns
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifyStatus VerifySchemaCmd::verifyColumns(const std::list<std::string> &schemaTableNames,
  const std::list<std::string> &dbTableNames, const CatalogueSchema &schema,
  const rdbms::Conn &conn) const {
  try {
    VerifyStatus status = VerifyStatus::UNKNOWN;
    for(auto &tableName : schemaTableNames) {
      const bool schemaTableIsInDb = dbTableNames.end() != std::find(dbTableNames.begin(), dbTableNames.end(), tableName);
      if (schemaTableIsInDb) {
        const auto schemaColumns = schema.getSchemaColumns(tableName);
        const auto dbColumns = conn.getColumns(tableName);
        // first check database columns are present the schema catalogue
        for (const auto &dbColumn : dbColumns) {
          if (!schemaColumns.count(dbColumn.first)) {
            std::cerr << "  ERROR: the DB column " << dbColumn.first
                      <<" not found in the catalogue schema" << std::endl;
            status = VerifyStatus::ERROR;
          }
        }
        // second check schema columns against the database catalogue
        for (const auto &schemaColumn : schemaColumns) {
            if (dbColumns.count(schemaColumn.first)) {
            if (schemaColumn.second != dbColumns.at(schemaColumn.first)) {
              std::cerr << "  ERROR: type mismatch for the column: DB[" 
                        << schemaColumn.first << "] = "  << dbColumns.at(schemaColumn.first) 
                        << ", SCHEMA[" << schemaColumn.first << "] = " 
                        << schemaColumn.second << std::endl;
              status = VerifyStatus::ERROR;
            }
          } else {
            std::cerr << "  ERROR: the schema column " << schemaColumn.first 
                      <<" not found in the DB" << std::endl;
            status = VerifyStatus::ERROR;
          }
        }
      } else {
        std::cerr << "  ERROR: the schema table " << tableName 
                  <<" not found in the DB" << std::endl;
        status = VerifyStatus::ERROR; 
      }
    }
    if (status != VerifyStatus::INFO && status != VerifyStatus::ERROR) {
      std::cerr << "  OK" << std::endl;
      status = VerifyStatus::OK;
    }
    return status;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// verifyTableNames
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifyStatus VerifySchemaCmd::verifyTableNames(const std::list<std::string> &schemaTableNames, 
  const std::list<std::string> &dbTableNames) const {
  try {
    VerifyStatus status = VerifyStatus::UNKNOWN;
    for(auto &tableName : schemaTableNames) {
      const bool schemaTableIsNotInDb = dbTableNames.end() == std::find(dbTableNames.begin(), dbTableNames.end(), tableName);
      if (schemaTableIsNotInDb) {
        std::cerr << "  ERROR: the schema table " << tableName << " not found in the DB" << std::endl;
        status = VerifyStatus::ERROR;
      }
    }
    for(auto &tableName : dbTableNames) {
      const bool dbTableIsNotInSchema = schemaTableNames.end() == std::find(schemaTableNames.begin(), schemaTableNames.end(), tableName);
      if (dbTableIsNotInSchema) {
        std::cerr << "  INFO: the database table " << tableName << " not found in the schema" << std::endl;
        if ( VerifyStatus::ERROR != status) {
          status = VerifyStatus::INFO;
        }
      }
    }
    if (status != VerifyStatus::INFO && status != VerifyStatus::ERROR) {
      std::cerr << "  OK" << std::endl;
      status = VerifyStatus::OK;
    }
    return status;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}
  
//------------------------------------------------------------------------------
// verifyIndexNames
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifyStatus VerifySchemaCmd::verifyIndexNames(const std::list<std::string> &schemaIndexNames, 
  const std::list<std::string> &dbIndexNames) const {
  try {
    VerifyStatus status = VerifyStatus::UNKNOWN;
    for(auto &indexName : schemaIndexNames) {
      const bool schemaIndexIsNotInDb = dbIndexNames.end() == std::find(dbIndexNames.begin(), dbIndexNames.end(), indexName);
      if (schemaIndexIsNotInDb) {
        std::cerr << "  ERROR: the schema index " << indexName << " not found in the DB" << std::endl;
        status = VerifyStatus::ERROR;
      }
    }
    for(auto &indexName : dbIndexNames) {
      const bool dbIndexIsNotInSchema = schemaIndexNames.end() == std::find(schemaIndexNames.begin(), schemaIndexNames.end(), indexName);
      if (dbIndexIsNotInSchema) {
        std::cerr << "  INFO: the database index " << indexName << " not found in the schema" << std::endl;
        if ( VerifyStatus::ERROR != status) {
          status = VerifyStatus::INFO;
        }
      }
    }
    if (status != VerifyStatus::INFO && status != VerifyStatus::ERROR) {
      std::cerr << "  OK" << std::endl;
      status = VerifyStatus::OK;
    }
    return status;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// verifySequenceNames
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifyStatus VerifySchemaCmd::verifySequenceNames(const std::list<std::string> &schemaSequenceNames, 
  const std::list<std::string> &dbSequenceNames) const {
  try {
    VerifyStatus status = VerifyStatus::UNKNOWN;
    for(auto &sequenceName : schemaSequenceNames) {
      const bool schemaSequenceIsNotInDb = dbSequenceNames.end() == std::find(dbSequenceNames.begin(), dbSequenceNames.end(), sequenceName);
      if (schemaSequenceIsNotInDb) {
        std::cerr << "  ERROR: the schema sequence " << sequenceName << " not found in the DB" << std::endl;
        status = VerifyStatus::ERROR;
      }
    }
    for(auto &sequenceName : dbSequenceNames) {
      const bool dbSequenceIsNotInSchema = schemaSequenceNames.end() == std::find(schemaSequenceNames.begin(), schemaSequenceNames.end(), sequenceName);
      if (dbSequenceIsNotInSchema) {
        std::cerr << "  INFO: the database sequence " << sequenceName << " not found in the schema" << std::endl;
        if ( VerifyStatus::ERROR != status) {
          status = VerifyStatus::INFO;
        }
      }
    }
    if (status != VerifyStatus::INFO && status != VerifyStatus::ERROR) {
      std::cerr << "  OK" << std::endl;
      status = VerifyStatus::OK;
    }
    return status;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// verifyTriggerNames
//------------------------------------------------------------------------------
VerifySchemaCmd::VerifyStatus VerifySchemaCmd::verifyTriggerNames(const std::list<std::string> &schemaTriggerNames, 
  const std::list<std::string> &dbTriggerNames) const {
  try {
    VerifyStatus status = VerifyStatus::UNKNOWN;
    for(auto &triggerName : schemaTriggerNames) {
      const bool schemaTriggerIsNotInDb = dbTriggerNames.end() == std::find(dbTriggerNames.begin(), dbTriggerNames.end(), triggerName);
      if (schemaTriggerIsNotInDb) {
        std::cerr << "  ERROR: the schema trigger " << triggerName << " not found in the DB" << std::endl;
        status = VerifyStatus::ERROR;
      }
    }
    for(auto &triggerName : dbTriggerNames) {
      const bool dbTriggerIsNotInSchema = schemaTriggerNames.end() == std::find(schemaTriggerNames.begin(), schemaTriggerNames.end(), triggerName);
      if (dbTriggerIsNotInSchema) {
        std::cerr << "  INFO: the database trigger " << triggerName << " not found in the schema" << std::endl;
        if ( VerifyStatus::ERROR != status) {
          status = VerifyStatus::INFO;
        }
      }
    }
    if (status != VerifyStatus::INFO && status != VerifyStatus::ERROR) {
      std::cerr << "  OK" << std::endl;
      status = VerifyStatus::OK;
    }
    return status;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
