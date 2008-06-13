/******************************************************************************
 *                      OraCommonSvc.cpp
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
 * @(#)$RCSfile: OraCommonSvc.cpp,v $ $Revision: 1.35 $ $Release$ $Date: 2008/06/13 14:43:15 $ $Author: sponcec3 $
 *
 * Implementation of the ICommonSvc for Oracle - CDBC version
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/FileClass.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/db/newora/OraCommonSvc.hpp"
#include "castor/db/newora/OraCnvSvc.hpp"
#include "castor/db/newora/OraStatement.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Busy.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/BaseAddress.hpp"
#include "occi.h"
#include <Cuuid.h>
#include <string>
#include <sstream>
#include <vector>
#include <Cns_api.h>
#include <vmgr_api.h>
#include <Ctape_api.h>
#include <serrno.h>

#define NS_SEGMENT_NOTOK (' ')

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraCommonSvc>* s_factoryOraCommonSvc =
  new castor::SvcFactory<castor::db::ora::OraCommonSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for requestToDo
const std::string castor::db::ora::OraCommonSvc::s_requestToDoStatementString =
  "BEGIN requestToDo(:1, :2); END;";

/// SQL statement for selectTape
const std::string castor::db::ora::OraCommonSvc::s_selectTapeStatementString =
  "SELECT id FROM Tape WHERE vid = :1 AND side = :2 AND tpmode = :3 FOR UPDATE";

/// SQL statement for selectSvcClass
const std::string castor::db::ora::OraCommonSvc::s_selectSvcClassStatementString =
  "SELECT id, nbDrives, defaultFileSize, maxReplicaNb, replicationPolicy, gcEnabled, migratorPolicy, recallerPolicy, hasDiskOnlyBehavior, streamPolicy, gcPolicy FROM SvcClass WHERE name = :1";

/// SQL statement for selectFileClass
const std::string castor::db::ora::OraCommonSvc::s_selectFileClassStatementString =
  "SELECT id, nbCopies FROM FileClass WHERE name = :1";

/// SQL statement for selectFileSystem
const std::string castor::db::ora::OraCommonSvc::s_selectFileSystemStatementString =
  "SELECT d.id, d.status, d.adminStatus, d.readRate, d.writeRate, d.nbReadStreams, d.nbWriteStreams, d.nbReadWriteStreams, d.nbMigratorStreams, d.nbRecallerStreams, f.id, f.free, f.minFreeSpace, f.minAllowedFreeSpace, f.maxFreeSpace, f.totalSize, f.readRate, f.writeRate, f.nbReadStreams, f.nbWriteStreams, f.nbReadWriteStreams, f.nbMigratorStreams, f.nbRecallerStreams, f.status, f.adminStatus FROM FileSystem f, DiskServer d WHERE d.name = :1 AND f.mountPoint = :2 AND f.diskserver = d.id";


//------------------------------------------------------------------------------
// OraCommonSvc
//------------------------------------------------------------------------------
castor::db::ora::OraCommonSvc::OraCommonSvc(const std::string name) :
  BaseSvc(name), DbBaseObj(0),
  m_requestToDoStatement(0),
  m_selectTapeStatement(0),
  m_selectSvcClassStatement(0),
  m_selectFileClassStatement(0),
  m_selectFileSystemStatement(0) {
}

//------------------------------------------------------------------------------
// ~OraCommonSvc
//------------------------------------------------------------------------------
castor::db::ora::OraCommonSvc::~OraCommonSvc() throw() {
  reset();
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraCommonSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraCommonSvc::ID() {
  return castor::SVC_ORACOMMONSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraCommonSvc::reset() throw() {
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    if (m_requestToDoStatement) deleteStatement(m_requestToDoStatement);
    if (m_selectTapeStatement) deleteStatement(m_selectTapeStatement);
    if (m_selectSvcClassStatement) deleteStatement(m_selectSvcClassStatement);
    if (m_selectFileClassStatement) deleteStatement(m_selectFileClassStatement);
    if (m_selectFileSystemStatement) deleteStatement(m_selectFileSystemStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_requestToDoStatement = 0;
  m_selectTapeStatement = 0;
  m_selectSvcClassStatement = 0;
  m_selectFileClassStatement = 0;
  m_selectFileSystemStatement = 0;
}

//------------------------------------------------------------------------------
// requestToDo
//------------------------------------------------------------------------------
castor::stager::Request*
castor::db::ora::OraCommonSvc::requestToDo(std::string service)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_requestToDoStatement) {
      m_requestToDoStatement =
        createStatement(s_requestToDoStatementString);
      m_requestToDoStatement->registerOutParam
        (2, oracle::occi::OCCIDOUBLE);
      m_requestToDoStatement->setAutoCommit(true);
    }
    // execute the statement
    m_requestToDoStatement->setString(1, service);
    m_requestToDoStatement->executeUpdate();
    // see whether we've found something
    u_signed64 id = (u_signed64)m_requestToDoStatement->getDouble(2);
    if (0 == id) {
      // Found no Request to handle
      return 0;
    }
    // Create result
    IObject* obj = cnvSvc()->getObjFromId(id);
    if (0 == obj) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "requestToDo : could not retrieve object for id "
        << id;
      throw ex;
    }
    castor::stager::Request* result =
      dynamic_cast<castor::stager::Request*>(obj);
    if (0 == result) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "requestToDo : object retrieved for id "
        << id << " was a "
        << castor::ObjectsIdStrings[obj->type()]
        << " while a Request was expected.";
      delete obj;
      throw ex;
    }
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in requestToDo."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// selectTape
//------------------------------------------------------------------------------
castor::stager::Tape*
castor::db::ora::OraCommonSvc::selectTape(const std::string vid,
                                          const int side,
                                          const int tpmode)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectTapeStatement) {
    m_selectTapeStatement = createStatement(s_selectTapeStatementString);
  }
  
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectTapeStatement->setString(1, vid);
    m_selectTapeStatement->setInt(2, side);
    m_selectTapeStatement->setInt(3, tpmode);
    oracle::occi::ResultSet *rset = m_selectTapeStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectTapeStatement->closeResultSet(rset);
      // we found nothing, so let's create the tape

      castor::stager::Tape* tape = new castor::stager::Tape();
      tape->setVid(vid);
      tape->setSide(side);
      tape->setTpmode(tpmode);
      tape->setStatus(castor::stager::TAPE_UNUSED);
      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      try {
        cnvSvc()->createRep(&ad, tape, false);
        return tape;
      } catch (castor::exception::Exception e) {
        delete tape;
        // XXX Change createRep in CodeGenerator to forward the oracle errorcode 
        if ( e.getMessage().str().find("ORA-00001", 0) != std::string::npos ) {
          // if violation of unique constraint, ie means that
          // some other thread was quicker than us on the insertion
          // So let's select what was inserted
       
          // set again the parameters

          rset = m_selectTapeStatement->executeQuery();
          if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
            // Still nothing ! Here it's a real error
            m_selectTapeStatement->closeResultSet(rset);
            castor::exception::Internal ex;
            ex.getMessage()
              << "Unable to select tape while inserting "
              << "violated unique constraint :"
              << std::endl << e.getMessage().str();
            throw ex;
          }
        } else {
          m_selectTapeStatement->closeResultSet(rset);
          // Else, "standard" error, throw exception
          castor::exception::Internal ex;
          ex.getMessage()
            << "Exception while inserting new tape in the DB :"
            << std::endl << e.getMessage().str();
          throw ex;
        }
      }
    }
    // If we reach this point, then we selected successfully
    // a tape and it's id is in rset
    id = rset->getInt(1);
    m_selectTapeStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape by vid, side and tpmode :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // Now get the tape from its id
  try {
    castor::BaseAddress ad;
    ad.setTarget(id);
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    castor::IObject* obj = cnvSvc()->createObj(&ad);
    castor::stager::Tape* tape =
      dynamic_cast<castor::stager::Tape*> (obj);
    if (0 == tape) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << id;
      delete obj;
      throw e;
    }
    return tape;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point
}

//------------------------------------------------------------------------------
// selectSvcClass
//------------------------------------------------------------------------------
castor::stager::SvcClass*
castor::db::ora::OraCommonSvc::selectSvcClass
(const std::string name)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectSvcClassStatement) {
    m_selectSvcClassStatement =
      createStatement(s_selectSvcClassStatementString);
  }
  // Execute statement and get result
  try {
    m_selectSvcClassStatement->setString(1, name);
    oracle::occi::ResultSet *rset = m_selectSvcClassStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectSvcClassStatement->closeResultSet(rset);
      return 0;
    }
    // Found the SvcClass, so create it in memory
    castor::stager::SvcClass* result =
      new castor::stager::SvcClass();
    result->setId((u_signed64)rset->getDouble(1));
    result->setNbDrives(rset->getInt(2));
    result->setDefaultFileSize((u_signed64)rset->getDouble(3));
    result->setMaxReplicaNb(rset->getInt(4));
    result->setReplicationPolicy(rset->getString(5));
    result->setGcEnabled(rset->getInt(6));
    result->setMigratorPolicy(rset->getString(7));
    result->setRecallerPolicy(rset->getString(8));
    result->setHasDiskOnlyBehavior(rset->getInt(9));
    result->setStreamPolicy(rset->getString(10));
    result->setGcPolicy(rset->getString(11));
    result->setName(name);
    m_selectSvcClassStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select SvcClass by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// selectFileClass
//------------------------------------------------------------------------------
castor::stager::FileClass*
castor::db::ora::OraCommonSvc::selectFileClass
(const std::string name)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectFileClassStatement) {
    m_selectFileClassStatement =
      createStatement(s_selectFileClassStatementString);
  }
  // Execute statement and get result
  try {
    m_selectFileClassStatement->setString(1, name);
    oracle::occi::ResultSet *rset = m_selectFileClassStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectFileClassStatement->closeResultSet(rset);
      return 0;
    }
    // Found the FileClass, so create it in memory
    castor::stager::FileClass* result =
      new castor::stager::FileClass();
    result->setId((u_signed64)rset->getDouble(1));
    result->setNbCopies(rset->getInt(2));
    result->setName(name);
    m_selectFileClassStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select FileClass by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// selectFileSystem
//------------------------------------------------------------------------------
castor::stager::FileSystem*
castor::db::ora::OraCommonSvc::selectFileSystem
(const std::string mountPoint,
 const std::string diskServer)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectFileSystemStatement) {
    m_selectFileSystemStatement =
      createStatement(s_selectFileSystemStatementString);
  }
  // Execute statement and get result
  try {
    m_selectFileSystemStatement->setString(1, diskServer);
    m_selectFileSystemStatement->setString(2, mountPoint);
    oracle::occi::ResultSet *rset = m_selectFileSystemStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectFileSystemStatement->closeResultSet(rset);
      return 0;
    }
    // Found the FileSystem and the DiskServer,
    // create them in memory
    castor::stager::DiskServer* ds =
      new castor::stager::DiskServer();
    ds->setId((u_signed64)rset->getDouble(1));
    ds->setStatus
      ((enum castor::stager::DiskServerStatusCode)rset->getInt(2));
    ds->setAdminStatus
      ((enum castor::monitoring::AdminStatusCodes)rset->getInt(3));
    ds->setName(diskServer);
    ds->setReadRate((u_signed64)rset->getDouble(4));
    ds->setWriteRate((u_signed64)rset->getDouble(5));
    ds->setNbReadStreams(rset->getInt(6));
    ds->setNbWriteStreams(rset->getInt(7));
    ds->setNbReadWriteStreams(rset->getInt(8));
    ds->setNbMigratorStreams(rset->getInt(9));
    ds->setNbRecallerStreams(rset->getInt(10));
    castor::stager::FileSystem* result =
      new castor::stager::FileSystem();
    result->setId((u_signed64)rset->getDouble(11));
    result->setFree((u_signed64)rset->getDouble(12));
    result->setMinFreeSpace(rset->getFloat(13));
    result->setMinAllowedFreeSpace(rset->getFloat(14));
    result->setMaxFreeSpace(rset->getFloat(15));
    result->setTotalSize((u_signed64)rset->getDouble(16));
    result->setReadRate((u_signed64)rset->getDouble(17));
    result->setWriteRate((u_signed64)rset->getDouble(18));
    result->setNbReadStreams(rset->getInt(19));
    result->setNbWriteStreams(rset->getInt(20));
    result->setNbReadWriteStreams(rset->getInt(21));
    result->setNbMigratorStreams(rset->getInt(22));
    result->setNbRecallerStreams(rset->getInt(23));
    result->setStatus
      ((enum castor::stager::FileSystemStatusCodes)rset->getInt(24));
    result->setAdminStatus
      ((enum castor::monitoring::AdminStatusCodes)rset->getInt(25));
    result->setMountPoint(mountPoint);
    result->setDiskserver(ds);
    ds->addFileSystems(result);
    m_selectFileSystemStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select FileSystem by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void castor::db::ora::OraCommonSvc::commit() {
  try {
    cnvSvc()->commit();
  } catch (castor::exception::Exception) {
    // commit failed, let's rollback
    rollback();
  }
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void castor::db::ora::OraCommonSvc::rollback() {
  try {
    cnvSvc()->rollback();
  } catch (castor::exception::Exception) {
    // rollback failed, let's reset everything for security
    reset();
  }
}

//------------------------------------------------------------------------------
//  handleException
//------------------------------------------------------------------------------
void castor::db::ora::OraCommonSvc::handleException
(oracle::occi::SQLException& e) {
  dynamic_cast<castor::db::ora::OraCnvSvc*>(cnvSvc())->handleException(e);
}

//------------------------------------------------------------------------------
// createStatement - for Oracle specific statements
//------------------------------------------------------------------------------
oracle::occi::Statement*
castor::db::ora::OraCommonSvc::createStatement (const std::string &stmtString)
  throw (castor::exception::Exception) {
    return dynamic_cast<castor::db::ora::OraCnvSvc*>(cnvSvc())->createOraStatement(stmtString);
}

//------------------------------------------------------------------------------
// deleteStatement - for Oracle specific statements
//------------------------------------------------------------------------------
void castor::db::ora::OraCommonSvc::deleteStatement(oracle::occi::Statement* stmt)
  throw (castor::exception::Exception) {
  castor::db::ora::OraStatement* oraStmt =
    new castor::db::ora::OraStatement(stmt, dynamic_cast<castor::db::ora::OraCnvSvc*>(cnvSvc()));
  delete oraStmt;
}
