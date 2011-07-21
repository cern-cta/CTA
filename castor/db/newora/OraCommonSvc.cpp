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
 * @(#)$RCSfile: OraCommonSvc.cpp,v $ $Revision: 1.42 $ $Release$ $Date: 2009/03/26 10:59:47 $ $Author: itglp $
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
"BEGIN requestToDo(:1, :2, :3); END;";

/// SQL statement for selectTape
const std::string castor::db::ora::OraCommonSvc::s_selectTapeStatementString =
  "SELECT id FROM Tape WHERE vid = :1 AND side = :2 AND tpmode = :3";

/// SQL statement for selectSvcClass
const std::string castor::db::ora::OraCommonSvc::s_selectSvcClassStatementString =
  "SELECT id, nbDrives, defaultFileSize, maxReplicaNb, migratorPolicy, recallerPolicy, disk1Behavior, failJobsWhenNoSpace, streamPolicy, gcPolicy, replicateOnClose FROM SvcClass WHERE name = :1";

/// SQL statement for selectFileClass
const std::string castor::db::ora::OraCommonSvc::s_selectFileClassStatementString =
  "SELECT id, nbCopies FROM FileClass WHERE name = :1";


//------------------------------------------------------------------------------
// OraCommonSvc
//------------------------------------------------------------------------------
castor::db::ora::OraCommonSvc::OraCommonSvc(const std::string name,
                                            castor::ICnvSvc* conversionService) :
  BaseSvc(name), DbBaseObj(conversionService),
  m_requestToDoStatement(0),
  m_selectTapeStatement(0),
  m_selectSvcClassStatement(0),
  m_selectFileClassStatement(0) {
  registerToCnvSvc(this);  // equivalent to conversionService->registerDepSvc(this);
}

//------------------------------------------------------------------------------
// ~OraCommonSvc
//------------------------------------------------------------------------------
castor::db::ora::OraCommonSvc::~OraCommonSvc() throw() {
  unregisterFromCnvSvc(this);
  reset();
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraCommonSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraCommonSvc::ID() {
  return castor::SVC_ORACOMMONSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraCommonSvc::reset() throw() {
  // Call upper level reset
  this->castor::BaseSvc::reset();
  this->castor::db::DbBaseObj::reset();
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    if (m_requestToDoStatement) deleteStatement(m_requestToDoStatement);
    if (m_selectTapeStatement) deleteStatement(m_selectTapeStatement);
    if (m_selectSvcClassStatement) deleteStatement(m_selectSvcClassStatement);
    if (m_selectFileClassStatement) deleteStatement(m_selectFileClassStatement);
  } catch (castor::exception::Exception& ignored) {};
  // Now reset all pointers to 0
  m_requestToDoStatement = 0;
  m_selectTapeStatement = 0;
  m_selectSvcClassStatement = 0;
  m_selectFileClassStatement = 0;
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
      m_requestToDoStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
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
    unsigned type = m_requestToDoStatement->getInt(3);
    // Create result
    IObject* obj = cnvSvc()->getObjFromId(id, type);
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
  u_signed64 id;
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
      } catch (castor::exception::Exception& e) {
        delete tape;
        // XXX Change createRep in CodeGenerator to forward the oracle errorcode
        if ( e.getMessage().str().find("ORA-00001", 0) != std::string::npos ) {
          // if violation of unique constraint, ie means that
          // some other thread was quicker than us on the insertion
          // So let's select what was inserted
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
    id = (u_signed64)rset->getDouble(1);
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
    ad.setObjType(castor::OBJ_Tape);
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
    result->setMigratorPolicy(rset->getString(5));
    result->setRecallerPolicy(rset->getString(6));
    result->setDisk1Behavior(rset->getInt(7));
    result->setFailJobsWhenNoSpace(rset->getInt(8));
    result->setStreamPolicy(rset->getString(9));
    result->setGcPolicy(rset->getString(10));
    result->setReplicateOnClose(rset->getInt(11));
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
  dynamic_cast<castor::db::ora::OraCnvSvc*>(cnvSvc())->terminateStatement(stmt);
}
