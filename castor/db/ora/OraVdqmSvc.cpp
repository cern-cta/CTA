/******************************************************************************
 *                      OraVdqmSvc.cpp
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
 * @(#)RCSfile: OraVdqmSvc.cpp  Revision: 1.0  Release Date: May 31, 2005  Author: mbraeger 
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/db/newora/OraCnvSvc.hpp"
#include "castor/db/newora/OraStatement.hpp"
#include "castor/db/ora/OraVdqmSvc.hpp"
#include "castor/vdqm/ClientIdentification.hpp"
#include "castor/vdqm/VdqmTape.hpp"
#include "castor/vdqm/TapeAccessSpecification.hpp"
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeDriveCompatibility.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/newVdqm.h"
#include "h/vdqm_constants.h"

#include "occi.h"
#include <Cuuid.h>
#include <net.h>
#include <string.h>


// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraVdqmSvc>* s_factoryOraVdqmSvc =
  new castor::SvcFactory<castor::db::ora::OraVdqmSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for selectTape
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeStatementString =
  "SELECT id FROM VdqmTape WHERE vid = :1 AND side = :2 AND tpmode = :3 FOR UPDATE";

/// SQL statement for function getTapeServer
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeServerStatementString =
  "SELECT id FROM TapeServer WHERE serverName = :1 FOR UPDATE";

/// SQL statement for function checkTapeRequest
const std::string castor::db::ora::OraVdqmSvc::s_checkTapeRequestStatement1String =
  "SELECT id FROM ClientIdentification WHERE machine = :1 AND userName = :2 AND port = :3 AND euid = :4 AND egid = :5 and magic = :6";

/// SQL statement for function checkTapeRequest
const std::string castor::db::ora::OraVdqmSvc::s_checkTapeRequestStatement2String =
  "SELECT id FROM TapeRequest WHERE tapeAccessSpecification = :1 AND tape = :2 AND requestedSrv = :3 AND client = :4";

/// SQL statement for function getQueuePosition
const std::string castor::db::ora::OraVdqmSvc::s_getQueuePositionStatementString =
  "SELECT count(*) FROM TapeRequest tr1, TapeRequest tr2 WHERE tr1.id = :1 AND tr1.deviceGroupName = tr2.deviceGroupName AND tr2.modificationTime <= tr1.modificationTime";
  
/// SQL statement for function deleteAllTapeDrvsFromSrv
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeDriveStatementString =
  "SELECT id FROM TapeDrive WHERE driveName = :1 AND tapeServer = :2 FOR UPDATE";
  
/// SQL statement for function existTapeDriveWithTapeInUse
const std::string castor::db::ora::OraVdqmSvc::s_existTapeDriveWithTapeInUseStatementString =
  "SELECT td.id FROM TapeDrive td, TapeRequest tr, VdqmTape WHERE td.runningTapeReq = tr.id AND tr.tape = VdqmTape.id AND VdqmTape.vid = :1";
  
/// SQL statement for function existTapeDriveWithTapeMounted
const std::string castor::db::ora::OraVdqmSvc::s_existTapeDriveWithTapeMountedStatementString =
  "SELECT td.id FROM TapeDrive td, VdqmTape WHERE td.tape = VdqmTape.id AND VdqmTape.vid = :1";
  
/// SQL statement for function selectTapeByVid
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeByVidStatementString =
  "SELECT id FROM VdqmTape WHERE vid = :1";      
  
/// SQL statement for function selectTapeReqForMountedTape
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeReqForMountedTapeStatementString =
  "SELECT id FROM TapeRequest WHERE tapeDrive = 0 AND tape = :1 AND (requestedSrv = :2 OR requestedSrv = 0) FOR UPDATE";
  
/// SQL statement for function selectTapeAccessSpecification
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeAccessSpecificationStatementString =
  "SELECT id FROM TapeAccessSpecification WHERE accessMode = :1 AND density = :2 AND tapeModel = :3";
  
/// SQL statement for function selectDeviceGroupName
const std::string castor::db::ora::OraVdqmSvc::s_selectDeviceGroupNameStatementString =
  "SELECT id FROM DeviceGroupName WHERE dgName = :1";
  
/*
/// SQL statement for function selectTapeRequestQueue
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeRequestQueueStatementString =
  "BEGIN selectTapeRequestQueue(:1, :2, :3); END;";
*/
  
/// SQL statement for function selectTapeRequestQueue
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeRequestQueueStatementString =
  "SELECT"
  "  ID,"
  "  DRIVENAME,"
  "  TAPEDRIVEID,"
  "  PRIORITY,"
  "  CLIENTPORT,"
  "  CLIENTEUID,"
  "  CLIENTEGID,"
  "  ACCESSMODE,"
  "  MODIFICATIONTIME,"
  "  CLIENTMACHINE,"
  "  VID,"
  "  TAPESERVER,"
  "  DGNAME,"
  "  CLIENTUSERNAME "
  "FROM"
  "  TAPEREQUESTSSHOWQUEUES_VIEW "
  "WHERE"
  "      (:1 IS NULL OR :2 = DGNAME)"
  "  AND (:3 IS NULL OR :4 = TAPESERVER) "
  "ORDER BY"
  "  MODIFICATIONTIME";

/// SQL statement for function selectTapeDriveQueue
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeDriveQueueStatementString =
  "SELECT"
  "  STATUS, ID, RUNNINGTAPEREQ, JOBID, MODIFICATIONTIME, RESETTIME, USECOUNT,"
  "  ERRCOUNT, TRANSFERREDMB, TAPEACCESSMODE, TOTALMB, SERVERNAME, VID,"
  "  DRIVENAME, DGNAME, DRIVEMODEL "
  "FROM"
  "  TAPEDRIVESHOWQUEUES_VIEW "
  "WHERE"
  "      (:1 IS NULL OR :2 = DGNAME)"
  "  AND (:3 IS NULL OR :4 = SERVERNAME) "
  "ORDER BY"
  "  DRIVENAME ASC";
  
/// SQL statement for function selectDeviceGroupName
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeRequestStatementString =
  "SELECT id FROM TapeRequest WHERE CAST(id AS INT) = :1";
  
/// SQL statement for function selectCompatibilitiesForDriveModel
const std::string castor::db::ora::OraVdqmSvc::s_selectCompatibilitiesForDriveModelStatementString =
  "SELECT id FROM TapeDriveCompatibility WHERE tapeDriveModel = :1";
  
/// SQL statement for function selectTapeAccessSpecifications
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeAccessSpecificationsStatementString =
  "SELECT id FROM TapeAccessSpecification WHERE tapeModel = :1 ORDER BY accessMode DESC";  

/**
 * This is the main select statement to dedicate a tape to a tape drive.
 * It respects the old and of course the new way to select a tape for a 
 * tape drive.
 * The old way is to look, if the tapeDrive and the tapeRequest have the same
 * dgn.
 * The new way is to look if the TapeAccessSpecification can be served by a 
 * specific tapeDrive. The tape Request are then orderd by the priorityLevel (for 
 * the new way) and by the modification time.
 */  
/// SQL statement for function allocateDrive
const std::string castor::db::ora::OraVdqmSvc::s_allocateDriveStatementString =
  "BEGIN allocateDrive(:1); END;";

/// SQL statement for function reuseTapeAllocation
const std::string castor::db::ora::OraVdqmSvc::s_reuseTapeAllocationStatementString =
  "BEGIN reuseTapeAllocation(:1, :2, :3); END;";

/// SQL statement for function requestToSubmit
const std::string castor::db::ora::OraVdqmSvc::s_requestToSubmitStatementString
  = "BEGIN requestToSubmit(:1); END;";


// -----------------------------------------------------------------------
// OraVdqmSvc
// -----------------------------------------------------------------------
castor::db::ora::OraVdqmSvc::OraVdqmSvc(const std::string name) :
  BaseSvc(name), DbBaseObj(0),
  m_selectTapeStatement(0),
  m_selectTapeServerStatement(0),
  m_checkTapeRequestStatement1(0),
  m_checkTapeRequestStatement2(0),
  m_getQueuePositionStatement(0),
  m_selectTapeDriveStatement(0),
  m_existTapeDriveWithTapeInUseStatement(0),
  m_existTapeDriveWithTapeMountedStatement(0),
  m_selectTapeByVidStatement(0),
  m_selectTapeReqForMountedTapeStatement(0),
  m_selectTapeAccessSpecificationStatement(0),
  m_selectDeviceGroupNameStatement(0),
  m_selectTapeRequestQueueStatement(0),
  m_selectTapeDriveQueueStatement(0),
  m_allocateDriveStatement(0),
  m_reuseTapeAllocationStatement(0),
  m_requestToSubmitStatement(0),
  m_selectCompatibilitiesForDriveModelStatement(0),
  m_selectTapeAccessSpecificationsStatement(0),
  m_selectTapeRequestStatement(0) {
}


// -----------------------------------------------------------------------
// ~OraVdqmSvc
// -----------------------------------------------------------------------
castor::db::ora::OraVdqmSvc::~OraVdqmSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraVdqmSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraVdqmSvc::ID() {
  return castor::SVC_ORAVDQMSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    if (m_selectTapeStatement) deleteStatement(m_selectTapeStatement);
    if (m_selectTapeServerStatement) deleteStatement(m_selectTapeServerStatement);
    if (m_checkTapeRequestStatement1) deleteStatement(m_checkTapeRequestStatement1);
    if (m_checkTapeRequestStatement2) deleteStatement(m_checkTapeRequestStatement2);
    if (m_getQueuePositionStatement) deleteStatement(m_getQueuePositionStatement);
    if (m_selectTapeDriveStatement) deleteStatement(m_selectTapeDriveStatement);
    if (m_existTapeDriveWithTapeInUseStatement) deleteStatement(m_existTapeDriveWithTapeInUseStatement);
    if (m_existTapeDriveWithTapeMountedStatement) deleteStatement(m_existTapeDriveWithTapeMountedStatement);
    if (m_selectTapeByVidStatement) deleteStatement(m_selectTapeByVidStatement);
    if (m_selectTapeReqForMountedTapeStatement) deleteStatement(m_selectTapeReqForMountedTapeStatement);
    if (m_selectTapeAccessSpecificationStatement) deleteStatement(m_selectTapeAccessSpecificationStatement);
    if (m_selectDeviceGroupNameStatement) deleteStatement(m_selectDeviceGroupNameStatement);
    if (m_selectTapeRequestQueueStatement) deleteStatement(m_selectTapeRequestQueueStatement);
    if (m_selectTapeRequestStatement) deleteStatement(m_selectTapeRequestStatement);
    if (m_selectTapeDriveQueueStatement) deleteStatement(m_selectTapeDriveQueueStatement);
    if (m_allocateDriveStatement) deleteStatement(m_allocateDriveStatement);
    if (m_reuseTapeAllocationStatement) deleteStatement(m_reuseTapeAllocationStatement);
    if (m_requestToSubmitStatement) deleteStatement(m_requestToSubmitStatement);
    if (m_selectCompatibilitiesForDriveModelStatement) deleteStatement(m_selectCompatibilitiesForDriveModelStatement);
    if (m_selectTapeAccessSpecificationsStatement) deleteStatement(m_selectTapeAccessSpecificationsStatement);
  } catch (oracle::occi::SQLException e) {};
  
  // Now reset all pointers to 0
  m_selectTapeStatement = 0;
  m_selectTapeServerStatement = 0;
  m_checkTapeRequestStatement1 = 0;
  m_checkTapeRequestStatement2 = 0;
  m_getQueuePositionStatement = 0;
  m_selectTapeDriveStatement = 0;
  m_existTapeDriveWithTapeInUseStatement = 0;
  m_existTapeDriveWithTapeMountedStatement = 0;
  m_selectTapeByVidStatement = 0;
  m_selectTapeReqForMountedTapeStatement = 0;
  m_selectTapeAccessSpecificationStatement = 0;
  m_selectDeviceGroupNameStatement = 0;
  m_selectTapeRequestQueueStatement = 0;
  m_selectTapeDriveQueueStatement = 0;
  m_selectTapeRequestStatement = 0;
  m_allocateDriveStatement = 0;
  m_reuseTapeAllocationStatement = 0;
  m_requestToSubmitStatement = 0;
  m_selectCompatibilitiesForDriveModelStatement = 0;
  m_selectTapeAccessSpecificationsStatement = 0;
}


// -----------------------------------------------------------------------
// selectTape
// -----------------------------------------------------------------------
castor::vdqm::VdqmTape*
castor::db::ora::OraVdqmSvc::selectTape(const std::string vid,
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

      castor::vdqm::VdqmTape* tape = new castor::vdqm::VdqmTape();
      tape->setVid(vid);
      tape->setSide(side);
      tape->setTpmode(tpmode);
      tape->setStatus(castor::vdqm::TAPE_UNUSED);
      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      try {
        cnvSvc()->createRep(&ad, tape, false);
        return tape;
      } catch (castor::exception::Exception e) {
        delete tape;
        // XXX  Change createRep in CodeGenerator to forward the oracle errorcode 
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
    castor::vdqm::VdqmTape* tape =
      dynamic_cast<castor::vdqm::VdqmTape*> (obj);
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

// -----------------------------------------------------------------------
// selectTapeServer
// -----------------------------------------------------------------------
castor::vdqm::TapeServer* 
  castor::db::ora::OraVdqmSvc::selectTapeServer(
  const std::string serverName,
  bool withTapeDrives)
  throw (castor::exception::Exception) {
  
  //Check, if the parameter isn't empty 
  if ( std::strlen(serverName.c_str()) == 0 )
    return NULL;
    
  // Check whether the statements are ok
  if (0 == m_selectTapeServerStatement) {
    m_selectTapeServerStatement = createStatement(s_selectTapeServerStatementString);
  }
  // Execute statement and get result
  u_signed64 id;
  try {
    m_selectTapeServerStatement->setString(1, serverName);
    oracle::occi::ResultSet *rset = m_selectTapeServerStatement->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectTapeServerStatement->closeResultSet(rset);
      
      // we found nothing, so let's create the tape sever
      castor::vdqm::TapeServer* tapeServer = new castor::vdqm::TapeServer();
      tapeServer->setServerName(serverName);
      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      try {
        cnvSvc()->createRep(&ad, tapeServer, false);
        return tapeServer;
      } catch (oracle::occi::SQLException e) {
        delete tapeServer;
        if (1 == e.getErrorCode()) {
          // if violation of unique constraint, ie means that
          // some other thread was quicker than us on the insertion
          // So let's select what was inserted
          rset = m_selectTapeServerStatement->executeQuery();
          if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
            // Still nothing ! Here it's a real error
            m_selectTapeServerStatement->closeResultSet(rset);
            castor::exception::Internal ex;
            ex.getMessage()
              << "Unable to select tapeServer while inserting "
              << "violated unique constraint :"
              << std::endl << e.getMessage();
            throw ex;
          }
        }
        m_selectTapeServerStatement->closeResultSet(rset);
        // Else, "standard" error, throw exception
        castor::exception::Internal ex;
        ex.getMessage()
          << "Exception while inserting new tapeServer in the DB :"
          << std::endl << e.getMessage();
        throw ex;
      }
    }
    
    // If we reach this point, then we selected successfully
    // a tapeServer and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    m_selectTapeServerStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tapeServer by vid, side and tpmode :"
      << std::endl << e.getMessage();
    throw ex;
  }
  
  // Now get the tapeServer from its id
  try {
    castor::BaseAddress ad;
    ad.setTarget(id);
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    castor::IObject* obj = cnvSvc()->createObj(&ad);
    
    castor::vdqm::TapeServer* tapeServer =
      dynamic_cast<castor::vdqm::TapeServer*> (obj);
    if (0 == tapeServer) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << id;
      delete obj;
      throw e;
    }
    
    if ( withTapeDrives ) {
      cnvSvc()->fillObj(&ad, obj, castor::OBJ_TapeDrive);
          
      /**
       * For existing TapeDrives, we want also the corresponding TapeRequest
       */
      for (std::vector<castor::vdqm::TapeDrive*>::iterator it = tapeServer->tapeDrives().begin();
           it != tapeServer->tapeDrives().end();
           it++) {
        if ((*it) != NULL) {
          cnvSvc()->fillObj(&ad, (*it), castor::OBJ_TapeRequest);
        }
      }
    }
   
    
    //reset Pointer
    obj = 0;
    
    return tapeServer;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tapeServer for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point
}


// -----------------------------------------------------------------------
// checkTapeRequest
// -----------------------------------------------------------------------
bool castor::db::ora::OraVdqmSvc::checkTapeRequest(
  const castor::vdqm::TapeRequest *newTapeRequest) 
  throw (castor::exception::Exception) {
    
  try {
    // Check whether the statements are ok
    if (0 == m_checkTapeRequestStatement1) {
      m_checkTapeRequestStatement1 =
        createStatement(s_checkTapeRequestStatement1String);
    }
    
    if (0 == m_checkTapeRequestStatement2) {
      m_checkTapeRequestStatement2 =
        createStatement(s_checkTapeRequestStatement2String);
    }
    
    castor::vdqm::ClientIdentification *client = newTapeRequest->client();
    
    m_checkTapeRequestStatement1->setString(1, client->machine());
    m_checkTapeRequestStatement1->setString(2, client->userName());
    m_checkTapeRequestStatement1->setInt(3, client->port());
    m_checkTapeRequestStatement1->setInt(4, client->euid());
    m_checkTapeRequestStatement1->setInt(5, client->egid());
    m_checkTapeRequestStatement1->setInt(6, client->magic());
    client = 0;
    
    // execute the statement
    oracle::occi::ResultSet *rset = m_checkTapeRequestStatement1->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found
      m_checkTapeRequestStatement1->closeResultSet(rset);
      return true;
    }
    
    /**
     * For every found ClientIdentification has to be checked whether
     * a tape request can be found, with the matching values. If yes, 
     * we return false, because we don't want to queue twice the same
     * request!
     */
    
    m_checkTapeRequestStatement2->setDouble
      (1, newTapeRequest->tapeAccessSpecification()->id());
    m_checkTapeRequestStatement2->setDouble
      (2, newTapeRequest->tape()->id());
    m_checkTapeRequestStatement2->setDouble
      (3, newTapeRequest->requestedSrv() == 0 ? 0 : newTapeRequest->requestedSrv()->id());
    
    oracle::occi::ResultSet *rset2 = NULL;
    u_signed64 clientId = 0;
    do {
      // If we reach this point, then we selected successfully
      // a ClientIdentification and it's id is in rset
      clientId = (u_signed64)rset->getDouble(1);
      
      m_checkTapeRequestStatement2->setDouble(4, clientId);
      rset2 = m_checkTapeRequestStatement2->executeQuery();
      if (oracle::occi::ResultSet::END_OF_FETCH == rset2->next()) {
        // Nothing found
        m_checkTapeRequestStatement2->closeResultSet(rset2);
      }
      else {
        // We found the same request in the database!
        m_checkTapeRequestStatement1->closeResultSet(rset);
        m_checkTapeRequestStatement2->closeResultSet(rset2);
        return false;
      } 
    } while (oracle::occi::ResultSet::END_OF_FETCH != rset->next());
    
    /**
     * If we are here, the request doesn't yet exist.
     */
    m_checkTapeRequestStatement1->closeResultSet(rset);
    return true;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in checkTapeRequest."
      << std::endl << e.what();
    throw ex;
  }
  // We should never reach this point.
    
  return true;
}


// -----------------------------------------------------------------------
// getQueuePosition
// -----------------------------------------------------------------------
int castor::db::ora::OraVdqmSvc::getQueuePosition(
  const castor::vdqm::TapeRequest *tapeRequest) 
  throw (castor::exception::Exception) {
    
  try {
    // Check whether the statements are ok
    if (0 == m_getQueuePositionStatement) {
      m_getQueuePositionStatement =
        createStatement(s_getQueuePositionStatementString);
    }
    
    // execute the statement
    m_getQueuePositionStatement->setDouble(1, tapeRequest->id());
    oracle::occi::ResultSet *rset = m_getQueuePositionStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return -1
      //Normally, the statement should always find something!
      m_getQueuePositionStatement->closeResultSet(rset);
      return -1;
    }
    
    // Return the TapeRequest queue position
    int queuePosition = (int)rset->getInt(1);
    m_getQueuePositionStatement->closeResultSet(rset);
    
    // XXX: Maybe in future the return value should be double!
    return queuePosition;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getQueuePosition."
      << std::endl << e.what();
    throw ex;
  }
  // We should never reach this point.
}


// -----------------------------------------------------------------------
// selectTapeDrive
// -----------------------------------------------------------------------
castor::vdqm::TapeDrive* 
  castor::db::ora::OraVdqmSvc::selectTapeDrive(
  const newVdqmDrvReq_t* driveRequest,
  castor::vdqm::TapeServer* tapeServer)
  throw (castor::exception::Exception) {
    
  // Check whether the statements are ok
  if (0 == m_selectTapeDriveStatement) {
    m_selectTapeDriveStatement = createStatement(s_selectTapeDriveStatementString);
  }
  // Execute statement and get result
  u_signed64 id;
  try {
    m_selectTapeDriveStatement->setString(1, driveRequest->drive);
    m_selectTapeDriveStatement->setDouble(2, tapeServer->id());
    oracle::occi::ResultSet *rset = m_selectTapeDriveStatement->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectTapeDriveStatement->closeResultSet(rset);
      
      // we found nothing, so return NULL
      return NULL;
    }
    
    // If we reach this point, then we selected successfully
    // a tapeDrive and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    m_selectTapeDriveStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tapeDrive by driveName and tapeServer id: "
      << std::endl << e.getMessage();
    throw ex;
  }
  
  // Now get the tapeDrive from its id
  try {
    castor::BaseAddress ad;
    ad.setTarget(id);
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    castor::IObject* obj = cnvSvc()->createObj(&ad);
    
    castor::vdqm::TapeDrive* tapeDrive =
      dynamic_cast<castor::vdqm::TapeDrive*> (obj);
    if (0 == tapeDrive) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << id;
      delete obj;
      throw e;
    }
    
    //Now we get the foreign related objects

    cnvSvc()->fillObj(&ad, obj, castor::OBJ_TapeRequest);
    cnvSvc()->fillObj(&ad, obj, castor::OBJ_VdqmTape);  
    cnvSvc()->fillObj(&ad, obj, castor::OBJ_DeviceGroupName);  
    
    tapeDrive->setTapeServer(tapeServer);

    /**
     * If there is already an assigned tapeRequest, we want also its objects
     */
    castor::vdqm::TapeRequest* tapeRequest = tapeDrive->runningTapeReq();
    if (tapeRequest != NULL) {
        cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_ClientIdentification);
        cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_VdqmTape);
        cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_DeviceGroupName);
        cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_TapeAccessSpecification);        
        cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_TapeServer);
    }
     
    //Reset pointer
    obj = 0;
    tapeRequest = 0;
    
    return tapeDrive;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tapeDrive for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point
}


// -----------------------------------------------------------------------
// existTapeDriveWithTapeInUse
// -----------------------------------------------------------------------
bool
  castor::db::ora::OraVdqmSvc::existTapeDriveWithTapeInUse(
  const std::string volid)
  throw (castor::exception::Exception) {
  
  // Execute statement and get result
  u_signed64 id;
    
  // Check whether the statements are ok
  if (0 == m_existTapeDriveWithTapeInUseStatement) {
    m_existTapeDriveWithTapeInUseStatement = 
      createStatement(s_existTapeDriveWithTapeInUseStatementString);
  }

  try {
    m_existTapeDriveWithTapeInUseStatement->setString(1, volid);
    oracle::occi::ResultSet *rset = m_existTapeDriveWithTapeInUseStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_existTapeDriveWithTapeInUseStatement->closeResultSet(rset);
      // we found nothing, so let's return false
      return false;
    }
    // If we reach this point, then we selected successfully
    // a tape drive and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    m_existTapeDriveWithTapeInUseStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape by vid: "
      << std::endl << e.getMessage();
    throw ex;
  }  
  
  return true;
}


// -----------------------------------------------------------------------
// existTapeDriveWithTapeMounted
// -----------------------------------------------------------------------
bool
  castor::db::ora::OraVdqmSvc::existTapeDriveWithTapeMounted(
  const std::string volid)
  throw (castor::exception::Exception) {

  // Execute statement and get result
  u_signed64 id;
    
  // Check whether the statements are ok
  if (0 == m_existTapeDriveWithTapeMountedStatement) {
    m_existTapeDriveWithTapeMountedStatement = 
      createStatement(s_existTapeDriveWithTapeMountedStatementString);
  }

  try {
    m_existTapeDriveWithTapeMountedStatement->setString(1, volid);
    oracle::occi::ResultSet *rset = m_existTapeDriveWithTapeMountedStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_existTapeDriveWithTapeMountedStatement->closeResultSet(rset);
      // we found nothing, so let's return false
      return false;
    }
    // If we reach this point, then we selected successfully
    // a tape drive and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    m_existTapeDriveWithTapeMountedStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape by vid: "
      << std::endl << e.getMessage();
    throw ex;
  }
   
   return true;    
}


// -----------------------------------------------------------------------
// selectTapeByVid
// -----------------------------------------------------------------------
castor::vdqm::VdqmTape* 
  castor::db::ora::OraVdqmSvc::selectTapeByVid(
  const std::string volid)
  throw (castor::exception::Exception) {
    
  // Check whether the statements are ok
  if (0 == m_selectTapeByVidStatement) {
    m_selectTapeByVidStatement = createStatement(s_selectTapeByVidStatementString);
  }
  // Execute statement and get result
  u_signed64 id;
  try {
    m_selectTapeByVidStatement->setString(1, volid);
    oracle::occi::ResultSet *rset = m_selectTapeByVidStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectTapeByVidStatement->closeResultSet(rset);
      // we found nothing, so let's return NULL
      return NULL;
    }
    // If we reach this point, then we selected successfully
    // a tape and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    m_selectTapeByVidStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape by vid: "
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
    castor::vdqm::VdqmTape* tape =
      dynamic_cast<castor::vdqm::VdqmTape*> (obj);
    if (0 == tape) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << id;
      delete obj;
      throw e;
    }
    return tape;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point  
}


// -----------------------------------------------------------------------
// selectTapeReqForMountedTape
// -----------------------------------------------------------------------
castor::vdqm::TapeRequest* 
  castor::db::ora::OraVdqmSvc::selectTapeReqForMountedTape(
  const castor::vdqm::TapeDrive* tapeDrive)
  throw (castor::exception::Exception) {
  
  // Check whether the statements are ok
  if (0 == m_selectTapeReqForMountedTapeStatement) {
    m_selectTapeReqForMountedTapeStatement = 
      createStatement(s_selectTapeReqForMountedTapeStatementString);
  }
  // Execute statement and get result
  u_signed64 id;
  try {
    m_selectTapeReqForMountedTapeStatement->setDouble
      (1, tapeDrive->tape() == 0 ? 0 : tapeDrive->tape()->id());
    m_selectTapeReqForMountedTapeStatement->setDouble
      (2, tapeDrive->tapeServer()->id());
    oracle::occi::ResultSet *rset =
      m_selectTapeReqForMountedTapeStatement->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectTapeReqForMountedTapeStatement->closeResultSet(rset);
      
      // we found nothing, so return NULL
      return NULL;
    }
    
    // If we reach this point, then we selected successfully
    // a tapeDrive and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    m_selectTapeReqForMountedTapeStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tapeDrive by vid, side and tpmode :"
      << std::endl << e.getMessage();
    throw ex;
  }
  
  // Now get the tapeRequest from its id
  try {
    castor::BaseAddress ad;
    ad.setTarget(id);
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    castor::IObject* obj = cnvSvc()->createObj(&ad);
    
    castor::vdqm::TapeRequest* tapeRequest =
      dynamic_cast<castor::vdqm::TapeRequest*> (obj);
    if (0 == tapeRequest) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << id;
      delete obj;
      throw e;
    }
    
    //Now we get the foreign related objects
    cnvSvc()->fillObj(&ad, obj, castor::OBJ_ClientIdentification);
    cnvSvc()->fillObj(&ad, obj, castor::OBJ_TapeServer);
    cnvSvc()->fillObj(&ad, obj, castor::OBJ_VdqmTape);
    cnvSvc()->fillObj(&ad, obj, castor::OBJ_DeviceGroupName);
    cnvSvc()->fillObj(&ad, obj, castor::OBJ_TapeAccessSpecification);    

    //Reset pointer
    obj = 0;
    
    return tapeRequest;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tapeRequest for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point  
}


// -----------------------------------------------------------------------
// selectTapeAccessSpecification
// -----------------------------------------------------------------------
castor::vdqm::TapeAccessSpecification* 
  castor::db::ora::OraVdqmSvc::selectTapeAccessSpecification
  (const int accessMode,
   const std::string density,
   const std::string tapeModel)
  throw (castor::exception::Exception) {
    
  // Check whether the statements are ok
  if (0 == m_selectTapeAccessSpecificationStatement) {
    m_selectTapeAccessSpecificationStatement = 
      createStatement(s_selectTapeAccessSpecificationStatementString);
  }
  // Execute statement and get result
  u_signed64 id;
  try {
    m_selectTapeAccessSpecificationStatement->setInt(1, accessMode);
    m_selectTapeAccessSpecificationStatement->setString(2, density);
    m_selectTapeAccessSpecificationStatement->setString(3, tapeModel);    
    oracle::occi::ResultSet *rset = m_selectTapeAccessSpecificationStatement->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectTapeAccessSpecificationStatement->closeResultSet(rset);
      // we found nothing, so let's return the NULL pointer  
      return NULL;
    }
    
    // If we reach this point, then we selected successfully
    // a dgName and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    m_selectTapeAccessSpecificationStatement->closeResultSet(rset);
  
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select TapeAccessSpecification by accessMode, density, tapeModel:"
      << std::endl << e.getMessage();
    throw ex;
  }
  // Now get the DeviceGroupName from its id
  try {
    castor::BaseAddress ad;
    ad.setTarget(id);
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    castor::IObject* obj = cnvSvc()->createObj(&ad);
    castor::vdqm::TapeAccessSpecification* tapeAccessSpec =
      dynamic_cast<castor::vdqm::TapeAccessSpecification*> (obj);
    if (0 == tapeAccessSpec) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << id;
      delete obj;
      throw e;
    }
    return tapeAccessSpec;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select TapeAccessSpecification for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point   
}


// -----------------------------------------------------------------------
// selectDeviceGroupName
// -----------------------------------------------------------------------
castor::vdqm::DeviceGroupName* 
  castor::db::ora::OraVdqmSvc::selectDeviceGroupName
  (const std::string dgName)
  throw (castor::exception::Exception) {

  // Check whether the statements are ok
  if (0 == m_selectDeviceGroupNameStatement) {
    m_selectDeviceGroupNameStatement = createStatement(s_selectDeviceGroupNameStatementString);
  }
  // Execute statement and get result
  u_signed64 id;
  try {
    m_selectDeviceGroupNameStatement->setString(1, dgName);
    oracle::occi::ResultSet *rset = m_selectDeviceGroupNameStatement->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectDeviceGroupNameStatement->closeResultSet(rset);
      // we found nothing, so let's return NULL
      
      return NULL;
    }
    // If we reach this point, then we selected successfully
    // a DeviceGroupName and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    m_selectDeviceGroupNameStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select DeviceGroupName by dgName :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // Now get the DeviceGroupName from its id
  try {
    castor::BaseAddress ad;
    ad.setTarget(id);
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    castor::IObject* obj = cnvSvc()->createObj(&ad);
    castor::vdqm::DeviceGroupName* deviceGroupName =
      dynamic_cast<castor::vdqm::DeviceGroupName*> (obj);
    if (0 == deviceGroupName) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << id;
      delete obj;
      throw e;
    }
    return deviceGroupName;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select DeviceGroupName for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point   
}


// -----------------------------------------------------------------------
// selectTapeRequestQueue
// -----------------------------------------------------------------------
std::vector<newVdqmVolReq_t>*
  castor::db::ora::OraVdqmSvc::selectTapeRequestQueue(const std::string dgn, 
  const std::string requestedSrv) throw (castor::exception::Exception) {

  // Check whether the statements are ok
  if (0 == m_selectTapeRequestQueueStatement) {
    m_selectTapeRequestQueueStatement =
      createStatement(s_selectTapeRequestQueueStatementString);
  }

  // Set the query statements parameters
  try {
    m_selectTapeRequestQueueStatement->setString(1, dgn);
    m_selectTapeRequestQueueStatement->setString(2, dgn);
    m_selectTapeRequestQueueStatement->setString(3, requestedSrv);
    m_selectTapeRequestQueueStatement->setString(4, requestedSrv);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Failed to set the parameters of selectTapeRequestQueueStatement:"
      << std::endl << e.getMessage();

    throw ex;
  }

  // Execute statement and get result
  try
  {
    oracle::occi::ResultSet *rs =
      m_selectTapeRequestQueueStatement->executeQuery();

    // The result of the search in the database.
    std::vector<newVdqmVolReq_t>* volReqs = new std::vector<newVdqmVolReq_t>;

    newVdqmVolReq_t volReq;

    while(rs->next())
    {
      volReq.VolReqID = rs->getInt(1);

      strncpy(volReq.drive, rs->getString(2).c_str(), sizeof(volReq.drive));
      // Null-terminate in case source string is longer than destination
      volReq.drive[sizeof(volReq.drive) - 1] = '\0';

      volReq.DrvReqID    = rs->getInt(3);
      volReq.priority    = rs->getInt(4);
      volReq.client_port = rs->getInt(5);
      volReq.clientUID   = rs->getInt(6);
      volReq.clientGID   = rs->getInt(7);
      volReq.mode        = rs->getInt(8);
      volReq.recvtime    = rs->getInt(9);

      strncpy(volReq.client_host, rs->getString(10).c_str(),
        sizeof(volReq.client_host));
      // Null-terminate in case source string is longer than destination
      volReq.client_host[sizeof(volReq.client_host) - 1] = '\0';

      strncpy(volReq.volid, rs->getString(11).c_str(), sizeof(volReq.volid));
      // Null-terminate in case source string is longer than destination
      volReq.volid[sizeof(volReq.volid) - 1] = '\0';

      strncpy(volReq.server, rs->getString(12).c_str(), sizeof(volReq.server));
      // Null-terminate in case source string is longer than destination
      volReq.server[sizeof(volReq.server) - 1] = '\0';

      strncpy(volReq.dgn, rs->getString(13).c_str(), sizeof(volReq.dgn));
      // Null-terminate in case source string is longer than destination
      volReq.dgn[sizeof(volReq.dgn) - 1] = '\0';

      strncpy(volReq.client_name, rs->getString(14).c_str(),
        sizeof(volReq.client_name));
      // Null-terminate in case source string is longer than destination
      volReq.client_name[sizeof(volReq.client_name) - 1] = '\0';

      volReqs->push_back(volReq);
    }

    return volReqs;

  } catch(oracle::occi::SQLException &e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Failed to query tape drive queue:"
      << std::endl << e.getMessage();

    throw ex;
  }
}


// -----------------------------------------------------------------------
// selectTapeDriveQueue
// -----------------------------------------------------------------------
std::vector<newVdqmDrvReq_t>*
  castor::db::ora::OraVdqmSvc::selectTapeDriveQueue(const std::string dgn,
  const std::string requestedSrv) throw (castor::exception::Exception) {

  // Check whether the statements are ok
  if (0 == m_selectTapeDriveQueueStatement) {
    m_selectTapeDriveQueueStatement = 
      createStatement(s_selectTapeDriveQueueStatementString);
  }

  // Set the query statements parameters
  try {
    m_selectTapeDriveQueueStatement->setString(1, dgn);
    m_selectTapeDriveQueueStatement->setString(2, dgn);
    m_selectTapeDriveQueueStatement->setString(3, requestedSrv);
    m_selectTapeDriveQueueStatement->setString(4, requestedSrv);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Failed to set the parameters of selectTapeDriveQueueStatement:"
      << std::endl << e.getMessage();

    throw ex;
  }
 
  // Execute statement and get result
  try
  {
    oracle::occi::ResultSet *rs =
      m_selectTapeDriveQueueStatement->executeQuery();

    // The result of the search in the database.
    std::vector<newVdqmDrvReq_t>* drvReqs = new std::vector<newVdqmDrvReq_t>;

    newVdqmDrvReq_t drvReq;

    while(rs->next())
    {
      drvReq.status    =
        translateNewStatus((castor::vdqm::TapeDriveStatusCodes)rs->getInt(1));
      drvReq.DrvReqID  = rs->getInt(2);
      drvReq.VolReqID  = rs->getInt(3);
      drvReq.jobID     = rs->getInt(4);
      drvReq.recvtime  = rs->getInt(5);
      drvReq.resettime = rs->getInt(6);
      drvReq.usecount  = rs->getInt(7);
      drvReq.errcount  = rs->getInt(8);
      drvReq.MBtransf  = rs->getInt(9);
      drvReq.mode      = rs->getInt(10);
      drvReq.TotalMB   = (u_signed64)rs->getDouble(11);

      strncpy(drvReq.reqhost, rs->getString(12).c_str(),sizeof(drvReq.reqhost));
      // Null-terminate in case source string is longer than destination
      drvReq.reqhost[sizeof(drvReq.reqhost) - 1] = '\0';

      strncpy(drvReq.volid, rs->getString(13).c_str(), sizeof(drvReq.volid));
      // Null-terminate in case source string is longer than destination
      drvReq.volid[sizeof(drvReq.volid) - 1] = '\0';

      strncpy(drvReq.server, drvReq.reqhost, sizeof(drvReq.server));
      // Null-terminate in case source string is longer than destination
      drvReq.server[sizeof(drvReq.server) - 1] = '\0';

      strncpy(drvReq.drive, rs->getString(14).c_str(), sizeof(drvReq.drive));
      // Null-terminate in case source string is longer than destination
      drvReq.drive[sizeof(drvReq.drive) - 1] = '\0';

      strncpy(drvReq.dgn, rs->getString(15).c_str(), sizeof(drvReq.dgn));
      // Null-terminate in case source string is longer than destination
      drvReq.dgn[sizeof(drvReq.dgn) - 1] = '\0';

      // TODO: Here, we can give infomation about occured errors, which will
      // be stored in the future in ErrorHistory. At the moment we leave it 
      // empty, because we don't care about the old dedicate string.
      drvReq.errorHistory[0] = '\0';

      strncpy(drvReq.tapeDriveModel, rs->getString(16).c_str(),
        sizeof(drvReq.tapeDriveModel));
      // Null-terminate in case source string is longer than destination
      drvReq.tapeDriveModel[sizeof(drvReq.tapeDriveModel) - 1] = '\0';

      drvReqs->push_back(drvReq);
    }

    return drvReqs;

  } catch(oracle::occi::SQLException &e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Failed to query tape drive queue:"
      << std::endl << e.getMessage();

    throw ex;

  } catch(castor::exception::Exception &e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Failed to query tape drive queue:"
      << std::endl << e.getMessage().str();

    throw ex;
  }
}


//------------------------------------------------------------------------------
// translateNewStatus
//------------------------------------------------------------------------------
int castor::db::ora::OraVdqmSvc::translateNewStatus(
  castor::vdqm::TapeDriveStatusCodes newStatusCode)
  throw (castor::exception::Exception) {

  int oldStatus = 0;

  switch (newStatusCode) {
    case castor::vdqm::UNIT_UP:
        oldStatus = VDQM_UNIT_UP | VDQM_UNIT_FREE;
        break;
    case castor::vdqm::UNIT_STARTING:
        oldStatus = VDQM_UNIT_UP | VDQM_UNIT_BUSY;
        break;
    case castor::vdqm::UNIT_ASSIGNED:
        oldStatus = VDQM_UNIT_UP | VDQM_UNIT_ASSIGN | VDQM_UNIT_BUSY;
        break;
    case castor::vdqm::VOL_MOUNTED:
        oldStatus = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_ASSIGN;
        break;
    case castor::vdqm::FORCED_UNMOUNT:
        oldStatus = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE |
                     VDQM_FORCE_UNMOUNT | VDQM_UNIT_UNKNOWN;
        break;
    case castor::vdqm::UNIT_DOWN:
        oldStatus = VDQM_UNIT_DOWN;
        break;
    case castor::vdqm::WAIT_FOR_UNMOUNT:
        oldStatus = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE |
                     VDQM_UNIT_UNKNOWN;
        break;
    case castor::vdqm::STATUS_UNKNOWN:
        oldStatus = VDQM_UNIT_UNKNOWN;
        break;
    default:
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "The tapeDrive is in a wrong status" << std::endl;
        throw ex;
  }

  return oldStatus;
}


// -----------------------------------------------------------------------
// selectTapeRequest
// -----------------------------------------------------------------------
castor::vdqm::TapeRequest* 
  castor::db::ora::OraVdqmSvc::selectTapeRequest(
  const int VolReqID)
  throw (castor::exception::Exception) {
    
   // Check whether the statements are ok
  if (0 == m_selectTapeRequestStatement) {
    m_selectTapeRequestStatement = createStatement(s_selectTapeRequestStatementString);
  }
  // Execute statement and get result
  u_signed64 id;
  try {
    m_selectTapeRequestStatement->setInt(1, VolReqID);
    oracle::occi::ResultSet *rset = m_selectTapeRequestStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectTapeRequestStatement->closeResultSet(rset);
      // we found nothing, so let's return NULL
      return NULL;
    }
    // If we reach this point, then we selected successfully
    // a tape and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    m_selectTapeRequestStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape request by VolReqID: "
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
    castor::vdqm::TapeRequest* tapeRequest =
      dynamic_cast<castor::vdqm::TapeRequest*> (obj);
    if (0 == tapeRequest) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << id;
      delete obj;
      obj = 0;
      throw e;
    }
    
    // Get the foreign related object
    cnvSvc()->fillObj(&ad, obj, castor::OBJ_ClientIdentification);
    cnvSvc()->fillObj(&ad, obj, castor::OBJ_TapeDrive);
    obj = 0;
    
    return tapeRequest;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape request for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point
}


// -----------------------------------------------------------------------
// allocateDrive
// -----------------------------------------------------------------------
bool castor::db::ora::OraVdqmSvc::allocateDrive()
  throw (castor::exception::Exception) {

  int aDriveWasAllocated = 0; // 0 = FALSE and 1 = TRUE
  

  // Check whether the statements are ok
  if (0 == m_allocateDriveStatement) {
    m_allocateDriveStatement =
      createStatement(s_allocateDriveStatementString);
    
    m_allocateDriveStatement->registerOutParam
        (1, oracle::occi::OCCIINT);

    m_allocateDriveStatement->setAutoCommit(true);
  }
  
  // Execute statement and get result
  try {
    m_allocateDriveStatement->executeUpdate();
    
    aDriveWasAllocated = m_allocateDriveStatement->getInt(1);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to find a TapeRequest for a free TapeDrive: "
      << std::endl << e.getMessage();
    throw ex;
  }
  
  return aDriveWasAllocated == 1;
}


// -----------------------------------------------------------------------
// reuseTapeAllocation
// -----------------------------------------------------------------------
u_signed64 castor::db::ora::OraVdqmSvc::reuseTapeAllocation(
  const castor::vdqm::VdqmTape *tape, const castor::vdqm::TapeDrive *drive)
  throw (castor::exception::Exception) {

  u_signed64 requestId = 0;


  // Check whether the statements are ok
  if (0 == m_reuseTapeAllocationStatement) {
    m_reuseTapeAllocationStatement =
      createStatement(s_reuseTapeAllocationStatementString);
    
    m_reuseTapeAllocationStatement->registerOutParam
        (3, oracle::occi::OCCIDOUBLE);

    m_reuseTapeAllocationStatement->setAutoCommit(false);
  }

  m_reuseTapeAllocationStatement->setDouble(1, tape->id());
  m_reuseTapeAllocationStatement->setDouble(2, drive->id());
  
  // Execute statement and get result
  try {
    m_reuseTapeAllocationStatement->executeUpdate();
    
    requestId = (u_signed64)m_reuseTapeAllocationStatement->getDouble(3);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Failed to try to reuse tape allocation: "
      << std::endl << e.getMessage();
    throw ex;
  }
  
  return requestId;
}


// -----------------------------------------------------------------------
// requestToSubmit
// -----------------------------------------------------------------------
castor::vdqm::TapeRequest *castor::db::ora::OraVdqmSvc::requestToSubmit()
  throw (castor::exception::Exception) {

  u_signed64                idTapeRequest = 0;
  castor::vdqm::TapeRequest *tapeRequest  = NULL;


  // Check whether the statements are ok
  if (0 == m_requestToSubmitStatement) {
    m_requestToSubmitStatement =
      createStatement(s_requestToSubmitStatementString);

    m_requestToSubmitStatement->registerOutParam
        (1, oracle::occi::OCCIDOUBLE);

    m_requestToSubmitStatement->setAutoCommit(true);
  }


  // Execute statement and get result
  try {
    m_requestToSubmitStatement->executeUpdate();

    idTapeRequest = (u_signed64)m_requestToSubmitStatement->getDouble(1);

    if (idTapeRequest == 0 ) {
      // We found nothing, so return NULL
      return NULL;
    }
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to find a TapeRequest for a free TapeDrive: "
      << std::endl << e.getMessage();
    throw ex;
  }

  // Needed to get the create objects from the database IDs
  castor::BaseAddress ad;
  ad.setTarget(idTapeRequest);
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);

  // Create the tape request object
  castor::IObject* obj = cnvSvc()->createObj(&ad);
  tapeRequest = dynamic_cast<castor::vdqm::TapeRequest*> (obj);
  if (0 == tapeRequest) {
    castor::exception::Internal e;
    e.getMessage() << "createObj returned unexpected type "
                     << obj->type() << " for id " << idTapeRequest;
    delete obj;
    throw e;
  }

  // Create the foreign related objects of the tape request
  cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_ClientIdentification);
  if(tapeRequest->client() == NULL) {
    castor::exception::Internal ie;

    ie.getMessage()
      << "Tape request is not linked to a set of client identification data";

    delete tapeRequest;
    throw ie;
  }
  cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_TapeDrive);
  if(tapeRequest->tapeDrive() == NULL) {
    castor::exception::Internal ie;

    ie.getMessage() << "Tape request is not linked to a tape drive";

    delete tapeRequest;
      throw ie;
  }

  // Get the foreign related objects of the tape drive of the tape request
  cnvSvc()->fillObj(&ad, tapeRequest->tapeDrive(),
    castor::OBJ_DeviceGroupName);
  if(tapeRequest->tapeDrive()->deviceGroupName() == NULL) {
    castor::exception::Internal ie;

    ie.getMessage()
      << "Tape drive of tape request is not linked to a device group name";

    delete tapeRequest;
    throw ie;
  }
  cnvSvc()->fillObj(&ad, tapeRequest->tapeDrive(), castor::OBJ_TapeServer);
  if(tapeRequest->tapeDrive()->tapeServer() == NULL) {
    castor::exception::Internal ie;

    ie.getMessage()
      << "Tape drive of tape request is not linked to a tape server";

    delete tapeRequest;
    throw ie;
  }

  return tapeRequest;
}


// -----------------------------------------------------------------------
// selectCompatibilitiesForDriveModel
// -----------------------------------------------------------------------
std::vector<castor::vdqm::TapeDriveCompatibility*>* 
  castor::db::ora::OraVdqmSvc::selectCompatibilitiesForDriveModel(
  const std::string tapeDriveModel)
  throw (castor::exception::Exception) {
  
  //The result from the select statement
  oracle::occi::ResultSet *rset;
  // Execute statement and get result
  u_signed64 id;
  
  // Check whether the statements are ok
  if (0 == m_selectCompatibilitiesForDriveModelStatement) {
    m_selectCompatibilitiesForDriveModelStatement = createStatement(
      s_selectCompatibilitiesForDriveModelStatementString);
  }
 
  try {
    m_selectCompatibilitiesForDriveModelStatement->setString(1, tapeDriveModel);
    rset = m_selectCompatibilitiesForDriveModelStatement->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectCompatibilitiesForDriveModelStatement->closeResultSet(rset);
      // we found nothing, so let's return NULL
      
      return NULL;
    }
    
    // If we reach this point, then we selected successfully
    // a TapeDriveCompatibility object and it's id is in rset
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select TapeDriveCompatibility by tapeDriveModel :"
      << std::endl << e.getMessage();
    throw ex;
  }
  
  // Now get the TapeDriveCompatibility from its id
  std::vector<castor::vdqm::TapeDriveCompatibility*>* result;
  try {
    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    
     // create result
    result = new std::vector<castor::vdqm::TapeDriveCompatibility*>; 
    castor::vdqm::TapeDriveCompatibility* driveCompatibility = NULL;
    
    do {
      id = (u_signed64)rset->getDouble(1);
      ad.setTarget(id);
      castor::IObject* obj = cnvSvc()->createObj(&ad);
      driveCompatibility =
        dynamic_cast<castor::vdqm::TapeDriveCompatibility*> (obj);
      
      if (0 == driveCompatibility) {
        castor::exception::Internal e;
        e.getMessage() << "createObj return unexpected type "
                       << obj->type() << " for id " << id;
        delete obj;
        obj = 0;
        throw e;
      }
      
      result->push_back(driveCompatibility);
    } while (oracle::occi::ResultSet::END_OF_FETCH != rset->next());
    
    m_selectCompatibilitiesForDriveModelStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select TapeDriveCompatibility for id " << id  << " :"
      << std::endl << e.getMessage();
      
    for (unsigned int i = 0; i < result->size(); i++) {
      delete (*result)[i];
    }
    result->clear();
    delete result;
    result = 0;
      
    throw ex;
  }
  // We should never reach this point 
}


// -----------------------------------------------------------------------
// selectTapeAccessSpecifications
// -----------------------------------------------------------------------
std::vector<castor::vdqm::TapeAccessSpecification*>*
  castor::db::ora::OraVdqmSvc::selectTapeAccessSpecifications(
  const std::string tapeModel)
  throw (castor::exception::Exception) {
  
  //The result from the select statement
  oracle::occi::ResultSet *rset;
  // Execute statement and get result
  u_signed64 id;
  
  // Check whether the statements are ok
  if (0 == m_selectTapeAccessSpecificationsStatement) {
    m_selectTapeAccessSpecificationsStatement = createStatement(
      s_selectTapeAccessSpecificationsStatementString);
  }
 
  try {
    m_selectTapeAccessSpecificationsStatement->setString(1, tapeModel);
    rset = m_selectTapeAccessSpecificationsStatement->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectTapeAccessSpecificationsStatement->closeResultSet(rset);
      // we found nothing, so let's return NULL
      
      return NULL;
    }
    
    // If we reach this point, then we selected successfully
    // a TapeAccessSpecification object and it's id is in rset
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select TapeAccessSpecification by tapeModel :"
      << std::endl << e.getMessage();
    throw ex;
  }
  
  // Now get the TapeAccessSpecification from its id
  std::vector<castor::vdqm::TapeAccessSpecification*>* result;
  try {
    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    
     // create result
    result = new std::vector<castor::vdqm::TapeAccessSpecification*>; 
    castor::vdqm::TapeAccessSpecification* tapeAccessSpec = NULL;
    
    do {
      id = (u_signed64)rset->getDouble(1);
      ad.setTarget(id);
      castor::IObject* obj = cnvSvc()->createObj(&ad);
      tapeAccessSpec = 
        dynamic_cast<castor::vdqm::TapeAccessSpecification*> (obj);
      
      if (0 == tapeAccessSpec) {
        castor::exception::Internal e;
        e.getMessage() << "createObj return unexpected type "
                       << obj->type() << " for id " << id;
        delete obj;
        obj = 0;
        throw e;
      }
      
      result->push_back(tapeAccessSpec);
    } while (oracle::occi::ResultSet::END_OF_FETCH != rset->next());
    
    m_selectTapeAccessSpecificationsStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select TapeAccessSpecification for id " << id  << " :"
      << std::endl << e.getMessage();
      
    for (unsigned int i = 0; i < result->size(); i++) {
      delete (*result)[i];
    }
    result->clear();
    delete result;
    result = 0;
      
    throw ex;
  }
  // We should never reach this point
}

// -------------------------------------------------------------------------
//  handleException
// -------------------------------------------------------------------------
void
castor::db::ora::OraVdqmSvc::handleException(oracle::occi::SQLException& e) {
  dynamic_cast<castor::db::ora::OraCnvSvc*>(cnvSvc())->handleException(e);
}

// -----------------------------------------------------------------------
// createStatement - for Oracle specific statements
// -----------------------------------------------------------------------
oracle::occi::Statement*
castor::db::ora::OraVdqmSvc::createStatement (const std::string &stmtString)
  throw (castor::exception::Exception) {
    return dynamic_cast<castor::db::ora::OraCnvSvc*>(cnvSvc())->createOraStatement(stmtString);
}

// -----------------------------------------------------------------------
// deleteStatement - for Oracle specific statements
// -----------------------------------------------------------------------
void
castor::db::ora::OraVdqmSvc::deleteStatement(oracle::occi::Statement* stmt)
  throw (castor::exception::Exception) {
    castor::db::ora::OraStatement* oraStmt =
        new castor::db::ora::OraStatement(stmt, dynamic_cast<castor::db::ora::OraCnvSvc*>(cnvSvc()));
    delete oraStmt;
}
