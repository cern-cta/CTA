/******************************************************************************
 *                      OraCnvSvc.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: OraCnvSvc.cpp,v $ $Revision: 1.47 $ $Release$ $Date: 2009/07/23 12:21:56 $ $Author: waldron $
 *
 * The conversion service to Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/IConverter.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/IObject.hpp"
#include "castor/Services.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/db/DbParamsSvc.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/db/newora/OraStatement.hpp"
#include "castor/exception/BadVersion.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include <sstream>
#include <iomanip>
#include <sys/types.h>
#include <linux/unistd.h>

// Local Files
#include "castor/db/newora/OraCnvSvc.hpp"

//------------------------------------------------------------------------------
// External C function used for getting configuration from castor.conf file
//------------------------------------------------------------------------------
extern "C" {
  char* getconfent_fromfile (const char *, const char *, const char *, int);
}

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraCnvSvc>* s_factoryOraCnvSvc =
  new castor::SvcFactory<castor::db::ora::OraCnvSvc>();

//------------------------------------------------------------------------------
// OraCnvSvc
//------------------------------------------------------------------------------
castor::db::ora::OraCnvSvc::OraCnvSvc(const std::string name) :
  castor::db::DbCnvSvc(name),
  m_user(""),
  m_passwd(""),
  m_dbName(""),
  m_environment(0),
  m_connection(0) {}

//------------------------------------------------------------------------------
// ~OraCnvSvc
//------------------------------------------------------------------------------
castor::db::ora::OraCnvSvc::~OraCnvSvc() throw() {
  dropConnection();
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraCnvSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraCnvSvc::ID() {
  return castor::SVC_ORACNV;
}

//------------------------------------------------------------------------------
// getPhysRepType
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraCnvSvc::getPhysRepType() const {
  return castor::REP_ORACLE;
}

//------------------------------------------------------------------------------
// getConnection
//------------------------------------------------------------------------------
oracle::occi::Connection* castor::db::ora::OraCnvSvc::getConnection()
  throw (castor::exception::Exception) {
  // Quick answer if connection available
  if (0 != m_connection) return m_connection;

  // No connection available, try to build one
  // get the parameters service to resolve the schema version and the config file
  // when different from the default/hardcoded ones
  castor::IService* psvc =
    castor::BaseObject::sharedServices()->service("DbParamsSvc", 0);
  castor::db::DbParamsSvc* params = dynamic_cast<castor::db::DbParamsSvc*>(psvc);
  if (params == 0) {
    // We didn't find it, locally allocate a default instance of the service
    params = new castor::db::DbParamsSvc("DbParamsSvc");
    psvc = 0;
  }
  
  // If the CASTOR_INSTANCE environment variable exists, append it the name
  // of the configuration option to lookup in the config file.
  std::string nameVal = name();
  const char *instance = getenv("CASTOR_INSTANCE");
  if (instance != NULL) {
    nameVal += "_";
    nameVal += instance;
  }
  
    if ("" == m_user || "" == m_dbName) {
      // get the config file name. Defaults to ORASTAGERCONFIG
      std::string confFile = params->getDbAccessConfFile();
      if (confFile == "") {
        confFile = std::string(ORASTAGERCONFIGFILE);
      }
      // get the new values
      char* cuser = getconfent_fromfile(confFile.c_str(), nameVal.c_str(), "user", 0);
      if (cuser == 0) {
        castor::exception::InvalidArgument e;
        e.getMessage() << "Failed to connect to database. Missing " << nameVal
                       << "/user configuration option from " << confFile.c_str()
                       << ".";
        if (serrno == SENOCONFIG) {
          e.getMessage() << " The file could not be opened.";
        }
        throw e;
      } else {
        m_user = std::string(cuser);
      }
  
      char* cpasswd = getconfent_fromfile(confFile.c_str(), nameVal.c_str(), "passwd", 0);
      if (cpasswd == 0) {
        castor::exception::InvalidArgument e;
        e.getMessage() << "Failed to connect to database. Missing " << nameVal
                       << "/passwd configuration option from " << confFile.c_str()
                       << ".";
        if (serrno == SENOCONFIG) {
          e.getMessage() << " The file could not be opened.";
        }
        throw e;
      } else {
        m_passwd = std::string(cpasswd);
      }
  
      char* cdbName = getconfent_fromfile(confFile.c_str(), nameVal.c_str(), "dbName", 0);
      if (cdbName == 0) {
        castor::exception::InvalidArgument e;
        e.getMessage() << "Failed to connect to database. Missing " << nameVal
                       << "/dbName configuration option from " << confFile.c_str()
                       << ".";
        if (serrno == SENOCONFIG) {
          e.getMessage() << " The file could not be opened.";
        }
        throw e;
      } else {
        m_dbName = std::string(cdbName);
      }
  
      if ("" == m_user || "" == m_dbName) {
        // If still empty, try to avoid connecting with empty string, since
        // ORACLE would core dump !
        castor::exception::InvalidArgument e;
        e.getMessage() << "Empty user name or db name, cannot connect to database.";
        throw e;
      }
    }

  // Setup Oracle connection
  try {
    if (0 == m_environment) {
      m_environment = oracle::occi::Environment::createEnvironment
        (oracle::occi::Environment::THREADED_MUTEXED);
    }
    m_connection =
      m_environment->createConnection(m_user, m_passwd, m_dbName);
  } catch (oracle::occi::SQLException &orae) {
    castor::exception::SQLError e;
    e.getMessage() << orae.what();
    throw e;    
  }    

  // get the schema version. No hardcoded default here, but DbParamsSvc contains
  // the Castor hardcoded value for it.
  std::string codeVersion = params->getSchemaVersion();
  std::string dbVersion = "";
  oracle::occi::Statement* stmt = 0;
  try {
    oracle::occi::Statement* stmt = m_connection->createStatement
      ("SELECT schemaVersion FROM CastorVersion");
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      dbVersion = rset->getString(1);
    }
    m_connection->terminateStatement(stmt);
    if (codeVersion != dbVersion) {
      dropConnection();
      castor::exception::BadVersion e;
      e.getMessage() << "Version mismatch between the database and the software : \""
                     << dbVersion << "\" versus \""
                     << codeVersion << "\"";
      throw e;
    }

    // Uncomment this to enable tracing of the DB
    //stmt = m_connection->createStatement
    //  ("alter session set events '10046 trace name context forever, level 8'");
    //stmt->executeUpdate();
    //m_connection->terminateStatement(stmt);
    //m_connection->commit();

    // for logging/debugging purposes, we set an identifier for this session
    std::ostringstream ss;
    ss << "BEGIN DBMS_APPLICATION_INFO.SET_CLIENT_INFO('CASTOR pid="
       << getpid() << " tid=" << syscall(__NR_gettid) << "'); END;";    // gettid() is not defined???
    stmt = m_connection->createStatement(ss.str());
    stmt->executeUpdate();
    m_connection->terminateStatement(stmt);
  }
  catch (oracle::occi::SQLException &orae) {
    // No CastorVersion table ?? This means bad version
    dropConnection();
    castor::exception::SQLError e;
    e.getMessage() << "Not able to find the version of castor in the database"
                   << " Original error was " << orae.what();
    if (0 != stmt) m_connection->terminateStatement(stmt);
    throw e;
  }
  
  if(psvc == 0) {
    // Delete the locally allocated instance of the params service
    delete params;
  }

  // "Created new Oracle connection"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, DLF_BASE_ORACLELIB + 24);
  return m_connection;
}

//------------------------------------------------------------------------------
// dropConnection
//------------------------------------------------------------------------------
void castor::db::ora::OraCnvSvc::dropConnection() throw() {
  // inherited cleanup
  castor::db::DbCnvSvc::dropConnection();
  // drop the connection
  try {
    if (0 != m_connection && 0 != m_environment) {
      //oracle::occi::Statement* stmt = m_connection->createStatement
      //  ("alter session set events '10046 trace name context off'");
      //stmt->executeUpdate();
      //m_connection->terminateStatement(stmt);
      //m_connection->commit();
      m_environment->terminateConnection(m_connection);
    }
    if (0 != m_environment) {
      oracle::occi::Environment::terminateEnvironment(m_environment);
    }
    // "Oracle connection dropped"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, DLF_BASE_ORACLELIB + 25);
  } catch (oracle::occi::SQLException e) {
    // "Failed to drop the Oracle connection"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.what())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                            DLF_BASE_ORACLELIB + 26, 1, params);
  } catch (...) {};
  // reset all whatever the state is
  m_connection  = 0;
  m_environment = 0;
  // also reset the connection string so that we reload parameters next time
  m_user   = "";
  m_passwd = "";
  m_dbName = "";
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void castor::db::ora::OraCnvSvc::commit()
  throw (castor::exception::Exception) {
  try {
    if (0 != m_connection) {
      m_connection->commit();
    }
  } catch (oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Error while committing :"
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void castor::db::ora::OraCnvSvc::rollback()
  throw (castor::exception::Exception) {
  try {
    if (0 != m_connection) {
      m_connection->rollback();
    }
  } catch (oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Error while rollbacking :"
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createStatement - the real one!
//------------------------------------------------------------------------------
castor::db::IDbStatement* castor::db::ora::OraCnvSvc::createStatement(const std::string& stmt)
  throw (castor::exception::Exception) {
  try {
    oracle::occi::Statement* statement = getConnection()->createStatement(stmt);
    return new castor::db::ora::OraStatement(statement, this);
  } catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Error creating statement, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
  return 0;
}


//------------------------------------------------------------------------------
// createOraStatement - for Oracle specific statements
//------------------------------------------------------------------------------
oracle::occi::Statement* castor::db::ora::OraCnvSvc::createOraStatement(const std::string& stmt)
  throw (castor::exception::Exception) {
  try {
    oracle::occi::Statement* oraStmt = getConnection()->createStatement(stmt);
    return oraStmt;
  } catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Error creating statement, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
  return 0;
}

//------------------------------------------------------------------------------
// terminateStatement
//------------------------------------------------------------------------------
void castor::db::ora::OraCnvSvc::terminateStatement(oracle::occi::Statement* oraStmt)
  throw (castor::exception::Exception) {
  try {
    if(0 == m_connection) {
      castor::exception::SQLError ex;
      ex.getMessage() << "Error closing statement, Oracle connection not initialized";
      throw ex;
    }
    m_connection->terminateStatement(oraStmt);
  } catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Error closing statement, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//-------------------------------------------------------------------------------
//  handleException
//-------------------------------------------------------------------------------
void castor::db::ora::OraCnvSvc::handleException(std::exception& e) {
  int errcode = ((oracle::occi::SQLException&)e).getErrorCode();
  if (errcode == 28    || errcode == 3113 || errcode == 3114  ||
      errcode == 32102 || errcode == 3135 || errcode == 12170 ||
      errcode == 12541 || errcode == 1012 || errcode == 1003  ||
      errcode == 12571 || errcode == 1033 || errcode == 1089  ||
      errcode == 12537 || (errcode >= 25401 && errcode <= 25409)) {
    // here we lost the connection due to an Oracle restart or network glitch
    // and this is the current list of errors acknowledged as a lost connection.
    // Notes:
    // - error #1003 'no statement parsed' means a SQL procedure
    // got invalid. The SQL code has still to be revalidated by hand, but
    // this way the process doesn't need to be restarted afterwards.
    // - error #12537 'TNS:connection closed' means that the Oracle backend dropped
    // the connection, usually because it's too overloaded. So it may come in bursts
    // but when fixed on the server side, the daemon will be able to reconnect.
    // - error #32102 'invalid OCI handle' seems to happen after an uncaught
    // Oracle side error, and a priori should act as a catch-all case.
    dropConnection();  // reset values and drop the connection
  }
}
