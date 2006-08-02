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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
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
#include "castor/db/ora/OraBaseObj.hpp"
#include "castor/exception/BadVersion.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include <iomanip>
#include <stdlib.h>                            // For getenv()

#include "castor/IAddress.hpp"
#include "occi.h"

// Local Files
#include "OraCnvSvc.hpp"

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

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for type retrieval
const std::string castor::db::ora::OraCnvSvc::s_getTypeStatementString =
  "SELECT type FROM Id2Type WHERE id = :1";

// -----------------------------------------------------------------------
// OraCnvSvc
// -----------------------------------------------------------------------
castor::db::ora::OraCnvSvc::OraCnvSvc(const std::string name) :
  BaseCnvSvc(name),
  m_user(""),
  m_passwd(""),
  m_dbName(""),
  m_environment(0),
  m_connection(0),
  m_getTypeStatement(0) {
  std::string full_name = name;
  if(char* p = getenv("CASTOR_INSTANCE")) full_name = full_name + "_" + p;
  char* cuser = getconfent_fromfile(ORASTAGERCONFIGFILE, full_name.c_str(), "user", 0);
  if (0 != cuser) m_user = std::string(cuser);
  char* cpasswd = getconfent_fromfile(ORASTAGERCONFIGFILE, full_name.c_str(), "passwd", 0);
  if (0 != cpasswd) m_passwd = std::string(cpasswd);
  char* cdbName = getconfent_fromfile(ORASTAGERCONFIGFILE, full_name.c_str(), "dbName", 0);
  if (0 != cdbName) m_dbName = std::string(cdbName);
  // Add alias for DiskCopyForRecall on DiskCopy
  addAlias(58, 5);
  // Add alias for TapeCopyForMigration on TapeCopy
  addAlias(59, 30);  
}

// -----------------------------------------------------------------------
// ~OraCnvSvc
// -----------------------------------------------------------------------
castor::db::ora::OraCnvSvc::~OraCnvSvc() throw() {
  reset();
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


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraCnvSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    m_connection->terminateStatement(m_getTypeStatement); // (deleteStatement is not available)
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_getTypeStatement= 0;
}


// -----------------------------------------------------------------------
// repType
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraCnvSvc::repType() const {
  return REPTYPE();
}

// -----------------------------------------------------------------------
// REPTYPE
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraCnvSvc::REPTYPE() {
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
void castor::db::ora::OraCnvSvc::dropConnection () throw() {
  // make all registered converters aware
  for (std::set<castor::db::ora::OraBaseObj*>::const_iterator it =
         m_registeredCnvs.begin();
       it != m_registeredCnvs.end();
       it++) {
    (*it)->reset();
  }
  // drop the connection
  try {
    if (0 != m_connection) {
      if (0 != m_getTypeStatement)
        m_connection->terminateStatement(m_getTypeStatement);
      if (0 != m_environment) {
        //oracle::occi::Statement* stmt = m_connection->createStatement
        //  ("alter session set events '10046 trace name context off'");
        //stmt->executeUpdate();
        //m_connection->terminateStatement(stmt);
        //m_connection->commit();
        m_environment->terminateConnection(m_connection);
      }
    }
  } catch (oracle::occi::SQLException e) {};
  // reset all whatever the state is
  m_connection = 0;
  m_getTypeStatement = 0;
}

// -----------------------------------------------------------------------
// createObj
// -----------------------------------------------------------------------
castor::IObject* castor::db::ora::OraCnvSvc::createObj (castor::IAddress* address)
  throw (castor::exception::Exception) {
  // If the address has no type, find it out
  if (OBJ_INVALID == address->objType()) {
    castor::BaseAddress* ad =
      dynamic_cast<castor::BaseAddress*>(address);
    unsigned int type = getTypeFromId(ad->target());
    if (0 == type) return 0;
    ad->setObjType(type);
  }
  // call method of parent object
  return this->BaseCnvSvc::createObj(address);
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
    castor::exception::Internal ex;
    ex.getMessage() << "Error while commiting :"
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
    castor::exception::Internal ex;
    ex.getMessage() << "Error while rollbacking :"
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

// -----------------------------------------------------------------------
// getTypeFromId
// -----------------------------------------------------------------------
const unsigned int
castor::db::ora::OraCnvSvc::getTypeFromId(const u_signed64 id)
  throw (castor::exception::Exception) {
  // a null id has a null type
  if (0 == id) return 0;
  oracle::occi::ResultSet *rset;
  try {
    // Check whether the statement is ok
    if (0 == m_getTypeStatement) {
      m_getTypeStatement = getConnection()->createStatement();
      m_getTypeStatement->setSQL(s_getTypeStatementString);
    }
    // Execute it
    m_getTypeStatement->setInt(1, id);
    rset = m_getTypeStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_getTypeStatement->closeResultSet(rset);
      castor::exception::NoEntry ex;
      ex.getMessage() << "No type found for id : " << id;
      throw ex;
    }
    const unsigned int res = rset->getInt(1);
    m_getTypeStatement->closeResultSet(rset);
    return res;
  } catch (oracle::occi::SQLException e) {
      try {
      m_getTypeStatement->closeResultSet(rset);
     } catch (oracle::occi::SQLException e2) {}
      handleException(e);
    castor::exception::InvalidArgument ex; // XXX fix it depending on ORACLE error
    ex.getMessage() << "Error in getting type from id."
                    << std::endl << e.what();
    throw ex;
  }
  // never reached
  return OBJ_INVALID;
}

// -----------------------------------------------------------------------
// getObjFromId
// -----------------------------------------------------------------------
castor::IObject* castor::db::ora::OraCnvSvc::getObjFromId
(u_signed64 id)
  throw (castor::exception::Exception) {
  castor::BaseAddress clientAd;
  clientAd.setTarget(id);
  clientAd.setCnvSvcName("DbCnvSvc");
  clientAd.setCnvSvcType(repType());
  return createObj(&clientAd);
}


// -----------------------------------------------------------------------
// handleException
// -----------------------------------------------------------------------

void  castor::db::ora::OraCnvSvc::handleException(oracle::occi::SQLException e) {
   try {
      		// Always try to rollback
      	 rollback();
      		
         if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
        // We've obviously lost the ORACLE connection here
           dropConnection(); // reset values and drop the connection
     	  }
		
    	} catch (castor::exception::Exception e) {
      	// rollback failed, let's drop the connection for security
	  dropConnection(); // instead of reset .... 
   	 } 
}


