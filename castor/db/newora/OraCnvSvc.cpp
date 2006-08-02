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
 * @(#)$RCSfile: OraCnvSvc.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2006/08/02 16:45:48 $ $Author: itglp $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/IConverter.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/IObject.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/db/DbBaseObj.hpp"
#include "castor/db/newora/OraStatement.hpp"
#include "castor/exception/BadVersion.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include <iomanip>

// Local Files
#include "castor/db/newora/OraCnvSvc.hpp"

// -----------------------------------------------------------------------
// External C function used for getting configuration from shift.conf file
// -----------------------------------------------------------------------
extern "C" {
  char* getconfent_fromfile (const char *, const char *, const char *, int);
}

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraCnvSvc> s_factoryOraCnvSvc;
const castor::IFactory<castor::IService>& OraCnvSvcFactory = s_factoryOraCnvSvc;

// -----------------------------------------------------------------------
// OraCnvSvc
// -----------------------------------------------------------------------
castor::db::ora::OraCnvSvc::OraCnvSvc(const std::string name) :
  castor::db::DbCnvSvc(name),
  m_user(""),
  m_passwd(""),
  m_dbName(""),
  m_environment(0),
  m_connection(0) {
  std::string full_name = name;
  if(char* p = getenv("CASTOR_INSTANCE")) full_name = full_name + "_" + p;
  char* cuser = getconfent_fromfile(ORASTAGERCONFIGFILE, full_name.c_str(), "user", 0);
  if (0 != cuser) m_user = std::string(cuser);
  char* cpasswd = getconfent_fromfile(ORASTAGERCONFIGFILE, full_name.c_str(), "passwd", 0);
  if (0 != cpasswd) m_passwd = std::string(cpasswd);
  char* cdbName = getconfent_fromfile(ORASTAGERCONFIGFILE, full_name.c_str(), "dbName", 0);
  if (0 != cdbName) m_dbName = std::string(cdbName);
}

// -----------------------------------------------------------------------
// ~OraCnvSvc
// -----------------------------------------------------------------------
castor::db::ora::OraCnvSvc::~OraCnvSvc() throw() {
  dropConnection();
  if (0 != m_environment) {
    try {
      oracle::occi::Environment::terminateEnvironment(m_environment);
    } catch (...) { }
  }
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraCnvSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraCnvSvc::ID() {
  return castor::SVC_ORACNV;
}

// -----------------------------------------------------------------------
// getPhysRepType
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraCnvSvc::getPhysRepType() const {
  return castor::REP_ORACLE;
}

// -----------------------------------------------------------------------
// getConnection
// -----------------------------------------------------------------------
oracle::occi::Connection* castor::db::ora::OraCnvSvc::getConnection()
  throw (oracle::occi::SQLException,
         castor::exception::Exception) {
  // Quick answer if connection available
  if (0 != m_connection) return m_connection;
  // Else try to build one
  if (0 == m_environment) {
    m_environment = oracle::occi::Environment::createEnvironment
      (oracle::occi::Environment::THREADED_MUTEXED);
  }
  if (0 == m_connection) {
    if ("" == m_user || "" == m_dbName) {
      // Try to avoid connecting with empty string, since
      // ORACLE would core dump !
      castor::exception::InvalidArgument e;
      e.getMessage() << "Empty user name or db name, cannot connect to database.";
      throw e;
    }
    m_connection =
      m_environment->createConnection(m_user, m_passwd, m_dbName);
    clog() << DEBUG << "Created new Oracle connection" << std::endl;

    std::string codeVersion = "2_0_3_0";
    std::string DBVersion = "";
    oracle::occi::Statement* stmt = 0;
    try {
      oracle::occi::Statement* stmt = m_connection->createStatement
        ("SELECT version FROM CastorVersion");
      oracle::occi::ResultSet *rset = stmt->executeQuery();
      if (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
        DBVersion = rset->getString(1);
      }   
      m_connection->terminateStatement(stmt);
      if (codeVersion != DBVersion) {
        castor::exception::BadVersion e;
        e.getMessage() << "Version mismatch between the database and the code : \""
                       << DBVersion << "\" versus \""
                       << codeVersion << "\"";
        throw e;
      }
    } catch (oracle::occi::SQLException e) {
      // No CastorVersion table ?? This means bad version
      if (0 != stmt) m_connection->terminateStatement(stmt);
      castor::exception::BadVersion ex;
      ex.getMessage() << "Not able to find the version of castor in the database"
                      << "\n Original error was " << e.what();
      throw ex;
    }
    
    // Uncomment this to unable tracing of the DB
    //stmt = m_connection->createStatement
    //  ("alter session set events '10046 trace name context forever, level 8'");
    //stmt->executeUpdate();
    //m_connection->terminateStatement(stmt);
    //m_connection->commit();
  }
  return m_connection;
}

// -----------------------------------------------------------------------
// dropConnection
// -----------------------------------------------------------------------
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
  } catch (oracle::occi::SQLException e) {};
  // reset all whatever the state is
  m_connection = 0;
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

// -----------------------------------------------------------------------
// createStatement - the real one!
// -----------------------------------------------------------------------
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


// -----------------------------------------------------------------------
// createOraStatement - for Oracle specific statements
// -----------------------------------------------------------------------
oracle::occi::Statement* castor::db::ora::OraCnvSvc::createOraStatement(const std::string& stmt)
  throw (castor::exception::Exception) {
  try {
     // XXX this is exposing the OCCI API - should it disappear?
     oracle::occi::Statement* statement = getConnection()->createStatement(stmt);
     return statement;     
  } catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Error creating statement, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
  return 0;
}

// -----------------------------------------------------------------------
// closeStatement
// -----------------------------------------------------------------------
void castor::db::ora::OraCnvSvc::closeStatement(castor::db::IDbStatement* stmt)
  throw (castor::exception::Exception) {
  try {
      castor::db::ora::OraStatement* oraStmt = dynamic_cast<castor::db::ora::OraStatement*>(stmt);
      if(oraStmt == 0) return;
      m_connection->terminateStatement(oraStmt->getStatementImpl());
      // the stmt object is still alive; this method is called only inside ~OraStatement
      // so there's no need to delete it.
  } catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Error closing statement, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

// -------------------------------------------------------------------------
//  handleException
// -------------------------------------------------------------------------
void castor::db::ora::OraCnvSvc::handleException(std::exception& e) {
	try {
    // Always try to rollback
    rollback();
    
    std::exception* pe = &e;
    if (3114 == ((oracle::occi::SQLException*)pe)->getErrorCode() || 28 == ((oracle::occi::SQLException*)pe)->getErrorCode()) {
      // We've obviously lost the ORACLE connection here
      dropConnection(); // reset values and drop the connection
    }
  }
  catch (castor::exception::Exception e) {
    // rollback failed, let's drop the connection for security
	  dropConnection(); // instead of reset .... 
  }	
}

