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
#include "h/vdqm_constants.h"

#include "occi.h"
#include <Cuuid.h>
#include <errno.h>
#include <net.h>
#include <string.h>
#include <memory>


// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraVdqmSvc>* s_factoryOraVdqmSvc =
  new castor::SvcFactory<castor::db::ora::OraVdqmSvc>();

// -----------------------------------------------------------------------
// Static map of SQL statement strings
// -----------------------------------------------------------------------
castor::db::ora::OraVdqmSvc::StatementStringMap
  castor::db::ora::OraVdqmSvc::s_statementStrings;


// -----------------------------------------------------------------------
// StatementStringMap::StatementStringMap
// -----------------------------------------------------------------------
castor::db::ora::OraVdqmSvc::StatementStringMap::StatementStringMap() {
  addStmtStr(SELECT_TAPE_SQL_STMT,
    "SELECT id FROM VdqmTape WHERE vid = :1 FOR UPDATE");
  addStmtStr(SELECT_TAPE_SERVER_SQL_STMT,
    "SELECT id FROM TapeServer WHERE serverName = :1 FOR UPDATE");
  addStmtStr(CHECK_TAPE_REQUEST1_SQL_STMT,
    "SELECT"
    "  id "
    "FROM"
    "  ClientIdentification "
    "WHERE"
    "      ClientIdentification.machine = :1"
    "  AND userName = :2"
    "  AND port = :3"
    "  AND euid = :4"
    "  AND egid = :5"
    "  AND magic = :6");
  addStmtStr(CHECK_TAPE_REQUEST2_SQL_STMT,
    "SELECT"
    "  id "
    "FROM"
    "  TapeRequest "
    "WHERE"
    "      TapeRequest.tapeAccessSpecification = :1"
    "  AND TapeRequest.tape = :2"
    "  AND TapeRequest.requestedSrv = :3"
    "  AND TapeRequest.client = :4");
  addStmtStr(GET_QUEUE_POSITION_SQL_STMT,
    "SELECT"
    "  count(*) "
    "FROM"
    "  TapeRequest tr1,"
    "  TapeRequest tr2 "
    "WHERE"
    "      tr1.id = :1 "
    "  AND tr1.deviceGroupName = tr2.deviceGroupName"
    "  AND tr2.modificationTime <= tr1.modificationTime");
  addStmtStr(SET_VOL_PRIORITY_SQL_STMT,
    "BEGIN castorVdqm.setVolPriority(:1, :2, :3, :4, :5, :6, :7); END;");
  addStmtStr(DELETE_VOL_PRIORITY_SQL_STMT,
    "BEGIN castorVdqm.deleteVolPriority(:1, :2, :3, :4, :5, :6, :7, :8); END;");
  addStmtStr(DELETE_OLD_VOL_PRIORITIES_SQL_STMT,
    "BEGIN castorVdqm.deleteOldVolPriorities(:1, :2); END;");
  addStmtStr(GET_ALL_VOL_PRIORITIES_SQL_STMT,
    "SELECT"
    "  priority,"
    "  clientUID,"
    "  clientGID,"
    "  clientHost,"
    "  vid,"
    "  tpMode,"
    "  lifespanType,"
    "  id,"
    "  creationTime,"
    "  modificationTime "
    "FROM"
    "  VolumePriority "
    "ORDER BY"
    "  vid, tpMode, lifespanType");
  addStmtStr(GET_EFFECTIVE_VOL_PRIORITIES_SQL_STMT,
    "SELECT"
    "  priority,"
    "  clientUID,"
    "  clientGID,"
    "  clientHost,"
    "  vid,"
    "  tpMode,"
    "  lifespanType,"
    "  id,"
    "  creationTime,"
    "  modificationTime "
    "FROM"
    "  EffectiveVolumePriority_VIEW "
    "ORDER BY"
    "  vid, tpMode");
  addStmtStr(GET_VOL_PRIORITIES_SQL_STMT,
    "SELECT"
    "  priority,"
    "  clientUID,"
    "  clientGID,"
    "  clientHost,"
    "  vid,"
    "  tpMode,"
    "  lifespanType,"
    "  id,"
    "  creationTime,"
    "  modificationTime "
    "FROM"
    "  VolumePriority "
    "WHERE"
    "  lifespanType = :1 "
    "ORDER BY"
    "  vid, tpMode, lifespanType");
  addStmtStr(SELECT_TAPE_DRIVE_SQL_STMT,
    "SELECT"
    "  id "
    "FROM"
    "  TapeDrive "
    "WHERE"
    "      driveName = :1"
    "  AND tapeServer = :2 "
    "FOR UPDATE");
  addStmtStr(DEDICATE_DRIVE_SQL_STMT,
    "BEGIN castorVdqm.dedicateDrive(:1, :2, :3, :4); END;");
  addStmtStr(DELETE_DRIVE_SQL_STMT,
    "BEGIN castorVdqm.deleteDrive(:1, :2, :3, :4); END;");
  addStmtStr(WRITE_RTCPD_JOB_SUBMISSION_SQL_STMT,
    "BEGIN castorVdqm.writeRTPCDJobSubmission(:1, :2, :3); END;");
  addStmtStr(WRITE_FAILED_RTCPD_JOB_SUBMISSION_SQL_STMT,
    "BEGIN castorVdqm.writeFailedRTPCDJobSubmission(:1, :2, :3); END;");
  addStmtStr(EXIST_TAPE_DRIVE_WITH_TAPE_IN_USE_SQL_STMT,
    "SELECT"
    "  td.id "
    "FROM"
    "  TapeDrive td,"
    "  TapeRequest tr,"
    "  VdqmTape "
    "WHERE"
    "      td.runningTapeReq = tr.id"
    "  AND tr.tape = VdqmTape.id"
    "  AND VdqmTape.vid = :1");
  addStmtStr(EXIST_TAPE_DRIVE_WITH_TAPE_MOUNTED_SQL_STMT,
    "SELECT"
    "  td.id "
    "FROM"
    "  TapeDrive td,"
    "  VdqmTape "
    "WHERE"
    "      td.tape = VdqmTape.id"
    "  AND VdqmTape.vid = :1");
  addStmtStr(SELECT_TAPE_BY_VID_SQL_STMT,
    "SELECT id FROM VdqmTape WHERE vid = :1");
  addStmtStr(SELECT_TAPE_REQ_FOR_MOUNTED_TAPE_SQL_STMT,
    "SELECT"
    "  id "
    "FROM"
    "  TapeRequest "
    "WHERE"
    "      tapeDrive = 0"
    "  AND tape = :1"
    "  AND (requestedSrv = :2"
    "   OR requestedSrv = 0) "
    "FOR UPDATE");
  addStmtStr(SELECT_TAPE_ACCESS_SPECIFICATION_SQL_STMT,
    "SELECT"
    "  id "
    "FROM"
    "  TapeAccessSpecification "
    "WHERE"
    "      accessMode = :1"
    "  AND density = :2"
    "  AND tapeModel = :3");
  addStmtStr(SELECT_DEVICE_GROUP_NAME_SQL_STMT,
    "SELECT id FROM DeviceGroupName WHERE dgName = :1");
  addStmtStr(SELECT_VOL_REQS_DGN_CREATION_TIME_ORDER_SQL_STMT,
    "SELECT"
    "  id,"
    "  driveName,"
    "  tapeDriveId,"
    "  priority,"
    "  clientPort,"
    "  clientEuid,"
    "  clientEgid,"
    "  accessMode,"
    "  creationTime,"
    "  clientMachine,"
    "  vid,"
    "  tapeServer,"
    "  dgName,"
    "  clientUsername "
    "FROM"
    "  tapeRequestShowqueues_VIEW "
    "WHERE"
    "      (:1 IS NULL OR :2 = dgName)"
    "  AND (:3 IS NULL OR :4 = tapeServer) "
    "ORDER BY"
    "  dgName ASC,"
    "  creationTime ASC");
  addStmtStr(SELECT_VOL_REQS_PRIORITY_ORDER_SQL_STMT,
    "SELECT"
    "  id,"
    "  driveName,"
    "  tapeDriveId,"
    "  priority,"
    "  clientPort,"
    "  clientEuid,"
    "  clientEgid,"
    "  accessMode,"
    "  creationTime,"
    "  clientMachine,"
    "  vid,"
    "  tapeServer,"
    "  dgName,"
    "  clientUsername,"
    "  volumePriority,"
    "  remoteCopyType "
    "FROM"
    "  tapeRequestShowqueues_VIEW "
    "WHERE"
    "      (:1 IS NULL OR :2 = dgName)"
    "  AND (:3 IS NULL OR :4 = tapeServer) "
    "ORDER BY"
    "  accessMode DESC,"
    "  volumePriority DESC,"
    "  creationTime ASC");
  addStmtStr(SELECT_TAPE_DRIVE_QUEUE_SQL_STMT,
    "SELECT"
    "  status,"
    "  id,"
    "  runningTapeReq,"
    "  jobId,"
    "  modificationTime,"
    "  resetTime,"
    "  useCount,"
    "  errCount,"
    "  transferredMB,"
    "  tapeAccessMode,"
    "  totalMB,"
    "  serverName,"
    "  vid,"
    "  driveName,"
    "  DGName,"
    "  dedicate "
    "FROM"
    "  TapeDriveShowqueues_VIEW "
    "WHERE"
    "      (:1 IS NULL OR :2 = DGName)"
    "  AND (:3 IS NULL OR :4 = serverName)");
  addStmtStr(SELECT_TAPE_REQUEST_SQL_STMT,
    "SELECT id FROM TapeRequest WHERE CAST(id AS INT) = :1");
  addStmtStr(SELECT_TAPE_REQUEST_FOR_UPDATE_SQL_STMT,
    "SELECT id FROM TapeRequest WHERE CAST(id AS INT) = :1 FOR UPDATE");
  addStmtStr(SELECT_COMPATIBILITIES_FOR_DRIVE_MODEL_SQL_STMT,
    "SELECT id FROM TapeDriveCompatibility WHERE tapeDriveModel = :1");
  addStmtStr(SELECT_TAPE_ACCESS_SPECIFICATIONS_SQL_STMT,
    "SELECT"
    "  id "
    "FROM"
    "  TapeAccessSpecification "
    "WHERE"
    "  tapeModel = :1 "
    "ORDER BY"
    "  accessMode DESC");
  addStmtStr(ALLOCATE_DRIVE_SQL_STMT,
    "BEGIN castorVdqm.allocateDrive(:1, :2, :3, :4, :5); END;");
  addStmtStr(REUSE_DRIVE_ALLOCATION_SQL_STMT,
    "BEGIN castorVdqm.reuseDriveAllocation(:1, :2, :3, :4, :5); END;");
  addStmtStr(REQUEST_TO_SUBMIT_SQL_STMT,
    "BEGIN castorVdqm.getRequestToSubmit(:1); END;");
  addStmtStr(REQUEST_SUBMITTED_SQL_STMT,
    "BEGIN castorVdqm.requestSubmitted(:1, :2, :3, :4, :5, :6, :7, :8, :9, "
    ":10, :11, :12, :13); END;");
  addStmtStr(RESET_DRIVE_AND_REQUEST_SQL_STMT,
    "BEGIN castorVdqm.resetDriveAndRequest(:1, :2, :3, :4, :5, :6, :7, :8, "
    ":9, :10, :11, :12); END;");
}


// -----------------------------------------------------------------------
// StatementStringMap::addStmtStr
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::StatementStringMap::addStmtStr(
  const StatementId stmtId, const char *stmt) {
  insert(std::make_pair(stmtId, stmt));
}


// -----------------------------------------------------------------------
// OraVdqmSvc
// -----------------------------------------------------------------------
castor::db::ora::OraVdqmSvc::OraVdqmSvc(const std::string name) :
  BaseSvc(name), DbBaseObj(0) {
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
unsigned int castor::db::ora::OraVdqmSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
unsigned int castor::db::ora::OraVdqmSvc::ID() {
  return castor::SVC_ORAVDQMSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::reset() throw() {
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  for(std::map<int, oracle::occi::Statement *>::iterator itor =
    m_statements.begin(); itor != m_statements.end(); itor++) {
    try {
      deleteStatement(itor->second);
    } catch(oracle::occi::SQLException e) {
      // Do nothing
    }
  }
  // Reset the stored statements
  m_statements.clear();
  // Call upper level reset
  this->castor::db::DbBaseObj::reset();
}


// -----------------------------------------------------------------------
// commit
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::commit() {
  DbBaseObj::commit();
}


// -----------------------------------------------------------------------
// rollback
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::rollback() {
  DbBaseObj::rollback();
}


// -----------------------------------------------------------------------
// selectOrCreateTape
// -----------------------------------------------------------------------
castor::vdqm::VdqmTape*
castor::db::ora::OraVdqmSvc::selectOrCreateTape(const std::string vid)
  throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  u_signed64 id;
  try {
    stmt->setString(1, vid);
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      // we found nothing, so let's create the tape

      castor::vdqm::VdqmTape* tape = new castor::vdqm::VdqmTape();
      tape->setVid(vid);
      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      try {
        cnvSvc()->createRep(&ad, tape, false);
        return tape;
      } catch (castor::exception::Exception& e) {
        delete tape;
        // XXX  Change createRep in CodeGenerator to forward the oracle
        // errorcode 
        if ( e.getMessage().str().find("ORA-00001", 0) != std::string::npos ) {
          // if violation of unique constraint, ie means that
          // some other thread was quicker than us on the insertion
          // So let's select what was inserted
       
          // set again the parameters

          rset = stmt->executeQuery();
          if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
            // Still nothing ! Here it's a real error
            stmt->closeResultSet(rset);
            castor::exception::Internal ex;
            ex.getMessage()
              << "Unable to select tape while inserting "
              << "violated unique constraint :"
              << std::endl << e.getMessage().str();
            throw ex;
          }
        } else {
	  // Else, "standard" error, throw exception
	  castor::exception::Internal ex;
	  ex.getMessage()
	    << "Exception while inserting new tape in the DB:"
	    << std::endl << e.getMessage().str();
	  throw ex;
	}
      }
    }
    // If we reach this point, then we selected successfully
    // a tape and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tape by vid :"
      << std::endl << oe.getMessage();
    throw ie;
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
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tape for id " << id << " :"
      << std::endl << oe.getMessage();
    throw ie;
  }
  // We should never reach this point
}


// -----------------------------------------------------------------------
// selectTapeServer
// -----------------------------------------------------------------------
castor::vdqm::TapeServer* 
  castor::db::ora::OraVdqmSvc::selectOrCreateTapeServer(
  const std::string serverName, bool withTapeDrives)
  throw (castor::exception::Exception) {

  // Check if the parameter is empty 
  if(serverName == "") {
    return NULL;
  }

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_SERVER_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  u_signed64 id;
  try {
    stmt->setString(1, serverName);
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      
      // we found nothing, so let's create the tape sever
      castor::vdqm::TapeServer* tapeServer = new castor::vdqm::TapeServer();
      tapeServer->setServerName(serverName);
      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      try {
        cnvSvc()->createRep(&ad, tapeServer, false);
        return tapeServer;
      } catch (oracle::occi::SQLException &oe) {
        handleException(oe);
        delete tapeServer;
        if (1 == oe.getErrorCode()) {
          // if violation of unique constraint, ie means that
          // some other thread was quicker than us on the insertion
          // So let's select what was inserted
          rset = stmt->executeQuery();
          if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
            // Still nothing ! Here it's a real error
            stmt->closeResultSet(rset);
            castor::exception::Internal ex;
            ex.getMessage()
              << "Unable to select tapeServer while inserting "
              << "violated unique constraint :"
              << std::endl << oe.getMessage();
            throw ex;
          }
        }
        stmt->closeResultSet(rset);
        // Else, "standard" error, throw exception
        castor::exception::Internal ex;
        ex.getMessage()
          << "Exception while inserting new tapeServer in the DB :"
          << std::endl << oe.getMessage();
        throw ex;
      }
    }
    
    // If we reach this point, then we selected successfully
    // a tapeServer and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tapeServer by vid, side and tpmode :"
      << std::endl << oe.getMessage();
    throw ie;
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
          
      // For existing TapeDrives, we want also the corresponding TapeRequest
      for (std::vector<castor::vdqm::TapeDrive*>::iterator it =
        tapeServer->tapeDrives().begin(); it != tapeServer->tapeDrives().end();
        it++) {
        if ((*it) != NULL) {
          cnvSvc()->fillObj(&ad, (*it), castor::OBJ_TapeRequest);
        }
      }
    }
    
    // Reset Pointer
    obj = 0;
    
    return tapeServer;
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tapeServer for id " << id  << " :"
      << std::endl << oe.getMessage();
    throw ie;
  }
  // We should never reach this point
}


// -----------------------------------------------------------------------
// checkTapeRequest
// -----------------------------------------------------------------------
bool castor::db::ora::OraVdqmSvc::checkTapeRequest(
  const castor::vdqm::TapeRequest *const newTapeRequest) 
  throw (castor::exception::Exception) {

  // Get the Statement objects, creating them if necessary
  oracle::occi::Statement *stmt1 = NULL;
  const StatementId stmtId1 = CHECK_TAPE_REQUEST1_SQL_STMT;
  try {
    if(!(stmt1 = getStatement(stmtId1))) {
      stmt1 = createStatement(s_statementStrings[stmtId1]);
      stmt1->setAutoCommit(false);
      storeStatement(stmtId1, stmt1);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId1 << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId1 << ": " << e.getMessage().str();
    throw ie;
  }
  oracle::occi::Statement *stmt2 = NULL;
  const StatementId stmtId2 = CHECK_TAPE_REQUEST2_SQL_STMT;
  try {
    if(!(stmt2 = getStatement(stmtId2))) {
      stmt2 = createStatement(s_statementStrings[stmtId2]);
      stmt2->setAutoCommit(false);
      storeStatement(stmtId2, stmt2);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId2 << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId2 << ": " << e.getMessage().str();
    throw ie;
  }

  try {
    castor::vdqm::ClientIdentification *client = newTapeRequest->client();
    
    stmt1->setString(1, client->machine());
    stmt1->setString(2, client->userName());
    stmt1->setInt(3, client->port());
    stmt1->setInt(4, client->euid());
    stmt1->setInt(5, client->egid());
    stmt1->setInt(6, client->magic());
    client = 0;
    
    // execute the statement
    oracle::occi::ResultSet *rset = stmt1->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found
      stmt1->closeResultSet(rset);
      return true;
    }
    
    // For every found ClientIdentification has to be checked whether
    // a tape request can be found, with the matching values. If yes, 
    // we return false, because we don't want to queue twice the same
    // request!
    
    stmt2->setDouble(1, newTapeRequest->tapeAccessSpecification()->id());
    stmt2->setDouble(2, newTapeRequest->tape()->id());
    stmt2->setDouble(3, newTapeRequest->requestedSrv() ==
      0 ? 0 : newTapeRequest->requestedSrv()->id());
    
    oracle::occi::ResultSet *rset2 = NULL;
    u_signed64 clientId = 0;
    do {
      // If we reach this point, then we selected successfully
      // a ClientIdentification and it's id is in rset
      clientId = (u_signed64)rset->getDouble(1);
      
      stmt2->setDouble(4, clientId);
      rset2 = stmt2->executeQuery();
      if (oracle::occi::ResultSet::END_OF_FETCH == rset2->next()) {
        // Nothing found
        stmt2->closeResultSet(rset2);
      }
      else {
        // We found the same request in the database!
        stmt1->closeResultSet(rset);
        stmt2->closeResultSet(rset2);
        return false;
      } 
    } while (oracle::occi::ResultSet::END_OF_FETCH != rset->next());
    
    // If we are here, the request doesn't yet exist.
    stmt1->closeResultSet(rset);
    return true;
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Error caught in checkTapeRequest."
      << std::endl << oe.what();
    throw ie;
  }
  // We should never reach this point.
    
  return true;
}


// -----------------------------------------------------------------------
// getQueuePosition
// -----------------------------------------------------------------------
int castor::db::ora::OraVdqmSvc::getQueuePosition(
  const u_signed64 tapeRequestId) throw (castor::exception::Exception) {
    
  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = GET_QUEUE_POSITION_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  try {
    // Execute the statement
    stmt->setDouble(1, tapeRequestId);
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return -1
      // Normally, the statement should always find something!
      stmt->closeResultSet(rset);
      return -1;
    }
    
    // Return the TapeRequest queue position
    int queuePosition = (int)rset->getInt(1);
    stmt->closeResultSet(rset);
    
    // XXX: Maybe in future the return value should be double!
    // -1 means not found
    return queuePosition == 0 ? -1 : queuePosition;
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Error caught in getQueuePosition." << std::endl
      << oe.what();
    throw ie;
  }
  // We should never reach this point.
}


// -----------------------------------------------------------------------
// setVolPriority
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::setVolPriority(const int priority,
  const int clientUID, const int clientGID, const std::string clientHost,
  const std::string vid, const int tpMode, const int lifespanType)
  throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SET_VOL_PRIORITY_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  try {
    // Execute the statement
    stmt->setInt(1, priority);
    stmt->setInt(2, clientUID);
    stmt->setInt(3, clientGID);
    stmt->setString(4, clientHost);
    stmt->setString(5, vid);
    stmt->setInt(6, tpMode);
    stmt->setInt(7, lifespanType);

    stmt->executeUpdate();
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Error caught in setVolPriority." << std::endl
      << oe.what();
    throw ie;
  }
}


// -----------------------------------------------------------------------
// deleteVolPriority
// -----------------------------------------------------------------------
u_signed64 castor::db::ora::OraVdqmSvc::deleteVolPriority(
  const std::string vid, const int tpMode, const int lifespanType,
  int *const priority, int *const clientUID, int *const clientGID,
  std::string *const clientHost) throw (castor::exception::Exception) {

  u_signed64 id = 0;


  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = DELETE_VOL_PRIORITY_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->registerOutParam(4, oracle::occi::OCCIDOUBLE); // returnVar
      stmt->registerOutParam(5, oracle::occi::OCCIINT); // priorityVar
      stmt->registerOutParam(6, oracle::occi::OCCIINT); // clientUIDVar
      stmt->registerOutParam(7, oracle::occi::OCCIINT); // clientGIDVar
      stmt->registerOutParam(8, oracle::occi::OCCISTRING,
        2048); // clientHostVar
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  try {
    // Execute the statement
    stmt->setString(1, vid);
    stmt->setInt(2, tpMode);
    stmt->setInt(3, lifespanType);

    stmt->executeUpdate();

    id          = (u_signed64)stmt->getDouble(4);
    *priority   = stmt->getInt(5);
    *clientUID  = stmt->getInt(6);
    *clientGID  = stmt->getInt(7);
    *clientHost = stmt->getString(8);

  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Error caught in deleteVolPriority." << std::endl
      << oe.what();
    throw ie;
  }

  return id;
}


// -----------------------------------------------------------------------
// deleteOldVolPriorities
// -----------------------------------------------------------------------
unsigned int castor::db::ora::OraVdqmSvc::deleteOldVolPriorities(
  const unsigned int maxAge) throw (castor::exception::Exception) {

  unsigned int prioritiesDeletedVar = 0;


  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = DELETE_OLD_VOL_PRIORITIES_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->registerOutParam(2, oracle::occi::OCCIINT); // prioritiesDeletedVar
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  try {
    // Execute the statement
    stmt->setInt(1, maxAge);

    stmt->executeUpdate();

    prioritiesDeletedVar = stmt->getInt(2);

  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Error caught in deleteVolPriority." << std::endl
      << oe.what();
    throw ie;
  }

  return prioritiesDeletedVar;
}


// -----------------------------------------------------------------------
// getAllVolPriorities
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::getAllVolPriorities(
  std::list<castor::vdqm::IVdqmSvc::VolPriority> &priorities)
  throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = GET_ALL_VOL_PRIORITIES_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    oracle::occi::ResultSet *rs = stmt->executeQuery();

    VolPriority p;

    while(rs->next() != oracle::occi::ResultSet::END_OF_FETCH) {
      p.priority         = rs->getInt(1);;
      p.clientUID        = rs->getInt(2);;
      p.clientGID        = rs->getInt(3);
      strncpy(p.clientHost, rs->getString(4).c_str(), sizeof(p.clientHost));
      // Null-terminate in case source string is longer than destination
      p.clientHost[sizeof(p.clientHost) - 1] = '\0';
      strncpy(p.vid, rs->getString(5).c_str(), sizeof(p.vid));
      // Null-terminate in case source string is longer than destination
      p.clientHost[sizeof(p.clientHost) - 1] = '\0';
      p.tpMode           = rs->getInt(6);
      p.lifespanType     = rs->getInt(7);
      p.id               = (u_signed64)rs->getDouble(8);
      p.creationTime     = (u_signed64)rs->getDouble(9);
      p.modificationTime = (u_signed64)rs->getDouble(10);

      priorities.push_back(p);
    }

    stmt->closeResultSet(rs);
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to get list of all volume priorities:"
      << std::endl << oe.getMessage();

    throw ie;

  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to get list of all volume priorities:"
      << std::endl << e.getMessage().str();

    throw ie;
  }
}


// -----------------------------------------------------------------------
// getEffectiveVolPriorities
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::getEffectiveVolPriorities(
  std::list<castor::vdqm::IVdqmSvc::VolPriority> &priorities)
  throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = GET_EFFECTIVE_VOL_PRIORITIES_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    oracle::occi::ResultSet *rs = stmt->executeQuery();

    VolPriority p;

    while(rs->next() != oracle::occi::ResultSet::END_OF_FETCH) {
      p.priority         = rs->getInt(1);;
      p.clientUID        = rs->getInt(2);;
      p.clientGID        = rs->getInt(3);
      strncpy(p.clientHost, rs->getString(4).c_str(), sizeof(p.clientHost));
      // Null-terminate in case source string is longer than destination
      p.clientHost[sizeof(p.clientHost) - 1] = '\0';
      strncpy(p.vid, rs->getString(5).c_str(), sizeof(p.vid));
      // Null-terminate in case source string is longer than destination
      p.clientHost[sizeof(p.clientHost) - 1] = '\0';
      p.tpMode           = rs->getInt(6);
      p.lifespanType     = rs->getInt(7);
      p.id               = (u_signed64)rs->getDouble(8);
      p.creationTime     = (u_signed64)rs->getDouble(9);
      p.modificationTime = (u_signed64)rs->getDouble(10);

      priorities.push_back(p);
    }

    stmt->closeResultSet(rs);
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to get list of effective volume priorities:"
      << std::endl << oe.getMessage();

    throw ie;

  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to get list of effective volume priorities:"
      << std::endl << e.getMessage().str();

    throw ie;
  }
}


// -----------------------------------------------------------------------
// getVolPriorities
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::getVolPriorities(
  std::list<castor::vdqm::IVdqmSvc::VolPriority> &priorities,
  const int lifespanType) throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = GET_VOL_PRIORITIES_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    stmt->setInt(1, lifespanType);
    oracle::occi::ResultSet *rs = stmt->executeQuery();

    VolPriority p;

    while(rs->next() != oracle::occi::ResultSet::END_OF_FETCH) {
      p.priority         = rs->getInt(1);;
      p.clientUID        = rs->getInt(2);;
      p.clientGID        = rs->getInt(3);
      strncpy(p.clientHost, rs->getString(4).c_str(), sizeof(p.clientHost));
      // Null-terminate in case source string is longer than destination
      p.clientHost[sizeof(p.clientHost) - 1] = '\0';
      strncpy(p.vid, rs->getString(5).c_str(), sizeof(p.vid));
      // Null-terminate in case source string is longer than destination
      p.vid[sizeof(p.vid) - 1] = '\0';
      p.tpMode           = rs->getInt(6);
      p.lifespanType     = rs->getInt(7);
      p.id               = (u_signed64)rs->getDouble(8);
      p.creationTime     = (u_signed64)rs->getDouble(9);
      p.modificationTime = (u_signed64)rs->getDouble(10);

      priorities.push_back(p);
    }

    stmt->closeResultSet(rs);
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to get list of effective volume priorities:"
      << std::endl << oe.getMessage();

    throw ie;

  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to get list of effective volume priorities:"
      << std::endl << e.getMessage().str();

    throw ie;
  }
}


// -----------------------------------------------------------------------
// getVolRequestsPriorityOrder
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::getVolRequestsPriorityOrder(
  castor::vdqm::IVdqmSvc::VolRequestList &requests,
  const std::string dgn, const std::string requestedSrv)
  throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_VOL_REQS_PRIORITY_ORDER_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setPrefetchMemorySize(0);
      stmt->setPrefetchRowCount(100);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Set the query statements parameters
  try {
    stmt->setString(1, dgn);
    stmt->setString(2, dgn);
    stmt->setString(3, requestedSrv);
    stmt->setString(4, requestedSrv);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() <<
      "Failed to set the parameters of getVolRequestsPriorityOrder statement:"
      << std::endl << oe.getMessage();

    throw ie;
  }

  // Execute statement and get result
  try {
    oracle::occi::ResultSet *const rs = stmt->executeQuery();

    castor::vdqm::IVdqmSvc::VolRequest *request = NULL;

    while(rs->next() != oracle::occi::ResultSet::END_OF_FETCH) {
      requests.push_back(request = new castor::vdqm::IVdqmSvc::VolRequest());

      request->id             = (u_signed64)rs->getDouble(1);
      request->driveName      = rs->getString(2);
      request->tapeDriveId    = (u_signed64)rs->getDouble(3);
      request->priority       = rs->getInt(4);
      request->clientPort     = rs->getInt(5);
      request->clientEuid     = rs->getInt(6);
      request->clientEgid     = rs->getInt(7);
      request->accessMode     = rs->getInt(8);
      request->creationTime   = rs->getInt(9);
      request->clientMachine  = rs->getString(10);
      request->vid            = rs->getString(11);
      request->tapeServer     = rs->getString(12);
      request->dgName         = rs->getString(13);
      request->clientUsername = rs->getString(14);
      request->volumePriority = rs->getInt(15);
      request->remoteCopyType = rs->getString(16);
    }

    stmt->closeResultSet(rs);

  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to query tape drive queue:"
      << std::endl << oe.getMessage();

    throw ie;
  }
}


// -----------------------------------------------------------------------
// selectTapeDrive
// -----------------------------------------------------------------------
castor::vdqm::TapeDrive* 
  castor::db::ora::OraVdqmSvc::selectTapeDrive(
  const vdqmDrvReq_t* driveRequest,
  castor::vdqm::TapeServer* tapeServer)
  throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_DRIVE_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  u_signed64 id;
  try {
    stmt->setString(1, driveRequest->drive);
    stmt->setDouble(2, tapeServer->id());
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      
      // We found nothing, so return NULL
      return NULL;
    }
    
    // If we reach this point, then we selected successfully
    // a tapeDrive and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tapeDrive by driveName and tapeServer id: "
      << std::endl << oe.getMessage();
    throw ie;
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

    // If there is already an assigned tapeRequest, we want also its objects
    castor::vdqm::TapeRequest* tapeRequest = tapeDrive->runningTapeReq();
    if (tapeRequest != NULL) {
      cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_ClientIdentification);
      cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_VdqmTape);
      cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_DeviceGroupName);
      cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_TapeAccessSpecification);        
      cnvSvc()->fillObj(&ad, tapeRequest, castor::OBJ_TapeServer);
    }
     
    // Reset pointer
    obj = 0;
    tapeRequest = 0;
    
    return tapeDrive;
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tapeDrive for id " << id  << " :"
      << std::endl << oe.getMessage();
    throw ie;
  }
  // We should never reach this point
}


// -----------------------------------------------------------------------
// dedicateDrive
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::dedicateDrive(const std::string driveName,
  const std::string serverName, const std::string dgName,
  const std::string dedicate) throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = DEDICATE_DRIVE_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    stmt->setString(1, driveName );
    stmt->setString(2, serverName);
    stmt->setString(3, dgName    );
    stmt->setString(4, dedicate  );
    stmt->executeUpdate();
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);

    switch(oe.getErrorCode()) {
    case castor::vdqm::IVdqmSvc::DbExceptions::INVALID_DRIVE_DEDICATE:
      {
        castor::exception::Exception ex(EINVAL);
        ex.getMessage() << "Failed to try to dedicate a drive: "
          << std::endl << oe.getMessage();
        throw ex;
        break;
      }
    case castor::vdqm::IVdqmSvc::DbExceptions::DRIVE_NOT_FOUND:
    case castor::vdqm::IVdqmSvc::DbExceptions::DRIVE_SERVER_NOT_FOUND:
    case castor::vdqm::IVdqmSvc::DbExceptions::DRIVE_DGN_NOT_FOUND:
      {
        castor::exception::Exception ex(EVQNOSDRV);
        ex.getMessage() << "Failed to try to dedicate a drive: "
          << std::endl << oe.getMessage();
        throw ex;
        break;
      }
    default:
      {
        castor::exception::Internal ie;
        ie.getMessage() << "Failed to try to dedicate a drive: "
          << std::endl << oe.getMessage();
        throw ie;
      }
    }
  }
}


// -----------------------------------------------------------------------
// deleteDrive
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::deleteDrive(std::string driveName,
  std::string serverName, std::string dgName)
  throw (castor::exception::Exception){

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = DELETE_DRIVE_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->registerOutParam(4, oracle::occi::OCCIINT);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  int result = 0;
  try {
    stmt->setString(1, driveName );
    stmt->setString(2, serverName);
    stmt->setString(3, dgName    );
    stmt->executeUpdate();
    result = stmt->getInt(4);
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);

    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to try to delete a drive: "
      << std::endl << oe.getMessage();

    throw ie;
  }

  // Based on the result, continue as normal or generate the appropriate
  // exception
  switch(result) {
  case  0: // Success
    // Do nothing
    break;
  case -1: // Tape server does not exist
    {
      castor::exception::Exception ex(EVQNOSDRV);
      ex.getMessage()
        << "OraVdqmSvc::deleteDrive(): tape server does not exist "
        << driveName << "@" << serverName
        << " dgn='" << dgName << "'" << std::endl;

      throw ex;
    }
    break;
  case -2: // DGN does not exist
    {
      castor::exception::Exception ex(EVQNOSDRV);
      ex.getMessage()
        << "OraVdqmSvc::deleteDrive(): DGN does not exist "
        << driveName << "@" << serverName
        << " dgn='" << dgName << "'" << std::endl;

      throw ex;
    }
    break;
  case -3: // Device group name is not associated
    {
      castor::exception::Exception ex(EVQNOSDRV);
      ex.getMessage()
        << "OraVdqmSvc::deleteDrive(): Tape drive does not exist "
        << driveName << "@" << serverName
        << " dgn='" << dgName << "'" << std::endl;

      throw ex;
    }
    break;
  case -4: // Drive has a job assigned
    {
      castor::exception::Exception ex(EVQREQASS);
      ex.getMessage()
        << "OraVdqmSvc::deleteDrive(): Tape drive has a job assigned "
        << driveName << "@" << serverName
        << " dgn='" << dgName << "'" << std::endl;

      throw ex;
    }
  default:
    {
      castor::exception::Internal ie;

      ie.getMessage()
        << "Unknown result value from delete drive PL/SQL procedure: "
        << result << " "
        << driveName << "@" << serverName
        << " dgn='" << dgName << "'" << std::endl;

      throw ie;
    }
  }
}


// -----------------------------------------------------------------------
// requestSubmitted
// -----------------------------------------------------------------------
bool castor::db::ora::OraVdqmSvc::requestSubmitted(
  const u_signed64  driveId,
  const u_signed64  requestId,
  bool             &driveExists,
  int              &driveStatusBefore,
  int              &driveStatusAfter,
  u_signed64       &runningRequestIdBefore,
  u_signed64       &runningRequestIdAfter,
  bool             &requestExists,
  int              &requestStatusBefore,
  int              &requestStatusAfter,
  u_signed64       &requestDriveIdBefore,
  u_signed64       &requestDriveIdAfter)
  throw (castor::exception::Exception) {

  bool result = false;


  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = REQUEST_SUBMITTED_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->registerOutParam( 3, oracle::occi::OCCIINT);    // return
      stmt->registerOutParam( 4, oracle::occi::OCCIINT);    // driveExists
      stmt->registerOutParam( 5, oracle::occi::OCCIINT);    // driveStatusBef
      stmt->registerOutParam( 6, oracle::occi::OCCIINT);    // driveStatusAft
      stmt->registerOutParam( 7, oracle::occi::OCCIDOUBLE); // runningRequestBef
      stmt->registerOutParam( 8, oracle::occi::OCCIDOUBLE); // runningRequestAft
      stmt->registerOutParam( 9, oracle::occi::OCCIINT);    // requestExists
      stmt->registerOutParam(10, oracle::occi::OCCIINT);    // requestStatusBef
      stmt->registerOutParam(11, oracle::occi::OCCIINT);    // requestStatusAft
      stmt->registerOutParam(12, oracle::occi::OCCIDOUBLE); // requestDriveBef
      stmt->registerOutParam(13, oracle::occi::OCCIDOUBLE); // requestDriveAft
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    stmt->setDouble(1, driveId);
    stmt->setDouble(2, requestId);
    stmt->executeUpdate();
    result                 = stmt->getInt(3);
    driveExists            = stmt->getInt(4);
    driveStatusBefore      = stmt->getInt(5);
    driveStatusAfter       = stmt->getInt(6);
    runningRequestIdBefore = (u_signed64)stmt->getDouble(7);
    runningRequestIdAfter  = (u_signed64)stmt->getDouble(8);
    requestExists          = stmt->getInt(9);
    requestStatusBefore    = stmt->getInt(10);
    requestStatusAfter     = stmt->getInt(11);
    requestDriveIdBefore   = (u_signed64)stmt->getDouble(12);
    requestDriveIdAfter    = (u_signed64)stmt->getDouble(13);
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);

    castor::exception::Internal ie;
    ie.getMessage() << "Failed to execute REQUEST_SUBMITTED_SQL_STMT: "
      << oe.getMessage();

    throw ie;
  }

  return result;
}


// -----------------------------------------------------------------------
// resetDriveAndRequest
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::resetDriveAndRequest(
  const u_signed64  driveId,
  const u_signed64  requestId,
  bool             &driveExists,
  int              &driveStatusBefore,
  int              &driveStatusAfter,
  u_signed64       &runningRequestIdBefore,
  u_signed64       &runningRequestIdAfter,
  bool             &requestExists,
  int              &requestStatusBefore,
  int              &requestStatusAfter,
  u_signed64       &requestDriveIdBefore,
  u_signed64       &requestDriveIdAfter)
  throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = RESET_DRIVE_AND_REQUEST_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->registerOutParam( 3, oracle::occi::OCCIINT);    // driveExists
      stmt->registerOutParam( 4, oracle::occi::OCCIINT);    // driveStatusBef
      stmt->registerOutParam( 5, oracle::occi::OCCIINT);    // driveStatusAft
      stmt->registerOutParam( 6, oracle::occi::OCCIDOUBLE); // runningRequestBef
      stmt->registerOutParam( 7, oracle::occi::OCCIDOUBLE); // runningRequestAft
      stmt->registerOutParam( 8, oracle::occi::OCCIINT);    // requestExists
      stmt->registerOutParam( 9, oracle::occi::OCCIINT);    // requestStatusBef
      stmt->registerOutParam(10, oracle::occi::OCCIINT);    // requestStatusAft
      stmt->registerOutParam(11, oracle::occi::OCCIDOUBLE); // requestDriveBef
      stmt->registerOutParam(12, oracle::occi::OCCIDOUBLE); // requestDriveAft
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    stmt->setDouble(1, driveId);
    stmt->setDouble(2, requestId);
    stmt->executeUpdate();
    driveExists            = stmt->getInt(3);
    driveStatusBefore      = stmt->getInt(4);
    driveStatusAfter       = stmt->getInt(5);
    runningRequestIdBefore = (u_signed64)stmt->getDouble(6);
    runningRequestIdAfter  = (u_signed64)stmt->getDouble(7);
    requestExists          = stmt->getInt(8);
    requestStatusBefore    = stmt->getInt(9);
    requestStatusAfter     = stmt->getInt(10);
    requestDriveIdBefore   = (u_signed64)stmt->getDouble(11);
    requestDriveIdAfter    = (u_signed64)stmt->getDouble(12);
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);

    castor::exception::Internal ie;
    ie.getMessage() << "Failed to execute RESET_DRIVE_AND_REQUEST_SQL_STMT: "
      << oe.getMessage();

    throw ie;
  }
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
    
  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = EXIST_TAPE_DRIVE_WITH_TAPE_IN_USE_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  try {
    stmt->setString(1, volid);
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      // we found nothing, so let's return false
      return false;
    }
    // If we reach this point, then we selected successfully
    // a tape drive and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tape by vid: "
      << std::endl << oe.getMessage();
    throw ie;
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

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = EXIST_TAPE_DRIVE_WITH_TAPE_MOUNTED_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  try {
    stmt->setString(1, volid);
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      // we found nothing, so let's return false
      return false;
    }
    // If we reach this point, then we selected successfully
    // a tape drive and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tape by vid: "
      << std::endl << oe.getMessage();
    throw ie;
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
    
  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_BY_VID_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  u_signed64 id;
  try {
    stmt->setString(1, volid);
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      // we found nothing, so let's return NULL
      return NULL;
    }
    // If we reach this point, then we selected successfully
    // a tape and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tape by vid: "
      << std::endl << oe.getMessage();
    throw ie;
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
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tape for id " << id  << " :"
      << std::endl << oe.getMessage();
    throw ie;
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
  
  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_REQ_FOR_MOUNTED_TAPE_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  u_signed64 id;
  try {
    stmt->setDouble(1, tapeDrive->tape() == 0 ? 0 : tapeDrive->tape()->id());
    stmt->setDouble(2, tapeDrive->tapeServer()->id());
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      
      // we found nothing, so return NULL
      return NULL;
    }
    
    // If we reach this point, then we selected successfully
    // a tapeDrive and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tapeDrive by vid, side and tpmode :"
      << std::endl << oe.getMessage();
    throw ie;
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
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tapeRequest for id " << id << " :"
      << std::endl << oe.getMessage();
    throw ie;
  }
  // We should never reach this point  
}


// -----------------------------------------------------------------------
// selectTapeAccessSpecification
// -----------------------------------------------------------------------
castor::vdqm::TapeAccessSpecification* 
  castor::db::ora::OraVdqmSvc::selectTapeAccessSpecification(
  const int accessMode, const std::string density, const std::string tapeModel)
  throw (castor::exception::Exception) {
    
  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_ACCESS_SPECIFICATION_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  u_signed64 id;
  try {
    stmt->setInt(1, accessMode);
    stmt->setString(2, density);
    stmt->setString(3, tapeModel);    
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      // we found nothing, so let's return the NULL pointer  
      return NULL;
    }
    
    // If we reach this point, then we selected successfully
    // a dgName and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select TapeAccessSpecification by accessMode, density,"
         " tapeModel:"
      << std::endl << oe.getMessage();
    throw ie;
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
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select TapeAccessSpecification for id " << id  << " :"
      << std::endl << oe.getMessage();
    throw ie;
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

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_DEVICE_GROUP_NAME_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  u_signed64 id;
  try {
    stmt->setString(1, dgName);
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      // we found nothing, so let's return NULL
      
      return NULL;
    }
    // If we reach this point, then we selected successfully
    // a DeviceGroupName and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select DeviceGroupName by dgName :"
      << std::endl << oe.getMessage();
    throw ie;
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
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select DeviceGroupName for id " << id << " :"
      << std::endl << oe.getMessage();
    throw ie;
  }
  // We should never reach this point   
}


// -----------------------------------------------------------------------
// getTapeRequestQueue
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::getTapeRequestQueue(
  castor::vdqm::IVdqmSvc::VolReqMsgList &requests, const std::string dgn,
  const std::string requestedSrv)
  throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_VOL_REQS_DGN_CREATION_TIME_ORDER_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setPrefetchMemorySize(0);
      stmt->setPrefetchRowCount(100);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Set the query statements parameters
  try {
    stmt->setString(1, dgn);
    stmt->setString(2, dgn);
    stmt->setString(3, requestedSrv);
    stmt->setString(4, requestedSrv);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to set the parameters of getTapeRequestQueue statement:"
      << std::endl << oe.getMessage();

    throw ie;
  }

  // Execute statement and get result
  try
  {
    oracle::occi::ResultSet *const rs = stmt->executeQuery();

    vdqmVolReq_t *volReq = NULL;

    while(rs->next() != oracle::occi::ResultSet::END_OF_FETCH)
    {
      requests.push_back(volReq = new vdqmVolReq_t());

      volReq->VolReqID = rs->getInt(1);

      strncpy(volReq->drive, rs->getString(2).c_str(), sizeof(volReq->drive));
      // Null-terminate in case source string is longer than destination
      volReq->drive[sizeof(volReq->drive) - 1] = '\0';

      volReq->DrvReqID    = rs->getInt(3);
      volReq->priority    = rs->getInt(4);
      volReq->client_port = rs->getInt(5);
      volReq->clientUID   = rs->getInt(6);
      volReq->clientGID   = rs->getInt(7);
      volReq->mode        = rs->getInt(8);
      volReq->recvtime    = rs->getInt(9);

      strncpy(volReq->client_host, rs->getString(10).c_str(),
        sizeof(volReq->client_host));
      // Null-terminate in case source string is longer than destination
      volReq->client_host[sizeof(volReq->client_host) - 1] = '\0';

      strncpy(volReq->volid, rs->getString(11).c_str(), sizeof(volReq->volid));
      // Null-terminate in case source string is longer than destination
      volReq->volid[sizeof(volReq->volid) - 1] = '\0';

      strncpy(volReq->server,rs->getString(12).c_str(),sizeof(volReq->server));
      // Null-terminate in case source string is longer than destination
      volReq->server[sizeof(volReq->server) - 1] = '\0';

      strncpy(volReq->dgn, rs->getString(13).c_str(), sizeof(volReq->dgn));
      // Null-terminate in case source string is longer than destination
      volReq->dgn[sizeof(volReq->dgn) - 1] = '\0';

      strncpy(volReq->client_name, rs->getString(14).c_str(),
        sizeof(volReq->client_name));
      // Null-terminate in case source string is longer than destination
      volReq->client_name[sizeof(volReq->client_name) - 1] = '\0';
    }

    stmt->closeResultSet(rs);

  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to query tape request queue:"
      << std::endl << oe.getMessage();

    throw ie;
  }
}


// -----------------------------------------------------------------------
// getTapeDriveQueue
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::getTapeDriveQueue(std::list<vdqmDrvReq_t>
  &drvReqs, const std::string dgn, const std::string requestedSrv)
  throw (castor::exception::Exception) {

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_DRIVE_QUEUE_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Set the query statements parameters
  try {
    stmt->setString(1, dgn);
    stmt->setString(2, dgn);
    stmt->setString(3, requestedSrv);
    stmt->setString(4, requestedSrv);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to set the parameters of selectTapeDriveQueue statement:"
      << std::endl << oe.getMessage();

    throw ie;
  }
 
  // Execute statement and get result
  try {
    oracle::occi::ResultSet *rs = stmt->executeQuery();

    vdqmDrvReq_t drvReq;

    while(rs->next() != oracle::occi::ResultSet::END_OF_FETCH) {
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

      strncpy(drvReq.dedicate, rs->getString(16).c_str(),
        sizeof(drvReq.dedicate));
      // Null-terminate in case source string is longer than destination
      drvReq.dedicate[sizeof(drvReq.dedicate) - 1] = '\0';

      drvReqs.push_back(drvReq);
    }

    stmt->closeResultSet(rs);

  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to query tape drive queue:"
      << std::endl << oe.getMessage();

    throw ie;

  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to query tape drive queue:"
      << std::endl << e.getMessage().str();

    throw ie;
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

  // S. Murray & N. Bessone 01/09/09
  //
  // Both UNIT_STARTING and UNIT_ASSIGNED should appear as START in showqueues,
  // therefore the VDQM_UNIT_ASSIGN bit is not sent to showqueues when the
  // drive is assigned, unlike vdqm_UnitStatus which must give the complete
  // drive status in order for the cleanup logic of rtcpd_Deassign to work
  // correctly
  case castor::vdqm::UNIT_STARTING:
  case castor::vdqm::UNIT_ASSIGNED:
    oldStatus = VDQM_UNIT_UP | VDQM_UNIT_BUSY;
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
  const int volReqID) throw (castor::exception::Exception) {
    
  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_REQUEST_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  u_signed64 id;
  try {
    stmt->setInt(1, volReqID);
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      // we found nothing, so let's return NULL
      return NULL;
    }
    // If we reach this point, then we selected successfully
    // a tape and it's id is in rset
    id = (u_signed64)rset->getDouble(1);
    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tape request by volReqID: "
      << std::endl << oe.getMessage();
    throw ie;
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
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tape request for id " << id << " :"
      << std::endl << oe.getMessage();
    throw ie;
  }
  // We should never reach this point
}


// -----------------------------------------------------------------------
// selectTapeRequestForUpdate
// -----------------------------------------------------------------------
bool castor::db::ora::OraVdqmSvc::selectTapeRequestForUpdate(
  const int volReqID) throw (castor::exception::Exception) {
    
  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_REQUEST_FOR_UPDATE_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    stmt->setInt(1, volReqID);
    oracle::occi::ResultSet *rset = stmt->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      // Found nothing
      return false;
    }

    stmt->closeResultSet(rset);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select tape request by volReqID: "
      << std::endl << oe.getMessage();
    throw ie;
  }

  // Successful select for update
  return true;
}


// -----------------------------------------------------------------------
// allocateDrive
// -----------------------------------------------------------------------
int castor::db::ora::OraVdqmSvc::allocateDrive(u_signed64 *tapeDriveId,
  std::string *tapeDriveName, u_signed64 *tapeRequestId,
  std::string *tapeRequestVid)
  throw (castor::exception::Exception) {

  // 1 = drive allocated, 0 = no possible allocation found, -1 possible
  // allocation found, but invalidated by other threads
  int allocationResult = 0;
  

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = ALLOCATE_DRIVE_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->registerOutParam(1, oracle::occi::OCCIINT);
      stmt->registerOutParam(2, oracle::occi::OCCIDOUBLE);
      stmt->registerOutParam(3, oracle::occi::OCCISTRING,256);
      stmt->registerOutParam(4, oracle::occi::OCCIDOUBLE);
      stmt->registerOutParam(5, oracle::occi::OCCISTRING,256);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    stmt->executeUpdate();
    
    allocationResult   = stmt->getInt(1);
    *tapeDriveId       = (u_signed64)stmt->getDouble(2);
    *tapeDriveName     = stmt->getString(3);
    *tapeRequestId     = (u_signed64)stmt->getDouble(4);
    *tapeRequestVid    = stmt->getString(5);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to find a TapeRequest for a free TapeDrive: "
      << std::endl << oe.getMessage();
    throw ie;
  }
  
  return allocationResult;
}


// -----------------------------------------------------------------------
// reuseDriveAllocation
// -----------------------------------------------------------------------
int castor::db::ora::OraVdqmSvc::reuseDriveAllocation(
  castor::vdqm::VdqmTape *const tape, castor::vdqm::TapeDrive *const drive,
  const int accessMode, u_signed64 *const tapeRequestId)
  throw (castor::exception::Exception) {

  // 1 = driev allocation reused, 0 = no possible reuse found, -1 possible
  // reuse found, but invalidated by other threads
  int reuseResult = 0;

  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = REUSE_DRIVE_ALLOCATION_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->registerOutParam(4, oracle::occi::OCCIINT);
      stmt->registerOutParam(5, oracle::occi::OCCIDOUBLE);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    stmt->setDouble(1, tape->id());
    stmt->setDouble(2, drive->id());
    stmt->setInt(3, accessMode);
    stmt->executeUpdate();
    
    reuseResult    = stmt->getInt(4);
    *tapeRequestId = (u_signed64)stmt->getDouble(5);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Failed to try to reuse drive allocation: "
      << std::endl << oe.getMessage();
    throw ie;
  }
  
  return reuseResult;
}


// -----------------------------------------------------------------------
// requestToSubmit
// -----------------------------------------------------------------------
castor::vdqm::TapeRequest *castor::db::ora::OraVdqmSvc::requestToSubmit()
  throw (castor::exception::Exception) {

  u_signed64 idTapeRequest = 0;


  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = REQUEST_TO_SUBMIT_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->registerOutParam(1, oracle::occi::OCCIDOUBLE);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }

  // Execute statement and get result
  try {
    stmt->executeUpdate();

    idTapeRequest = (u_signed64)stmt->getDouble(1);

    if (idTapeRequest == 0 ) {
      // We found nothing, so return NULL
      return NULL;
    }
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to find a TapeRequest for a free TapeDrive: "
      << std::endl << oe.getMessage();
    throw ie;
  }

  // Needed to get the create objects from the database IDs
  castor::BaseAddress ad;
  ad.setTarget(idTapeRequest);
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);

  // Create the tape request object
  castor::IObject* obj = cnvSvc()->createObj(&ad);

  // Create an auto pointer to delete the tape request object if an exception
  // is thrown before the end of this method
  std::auto_ptr<castor::vdqm::TapeRequest>
    tapeRequest(dynamic_cast<castor::vdqm::TapeRequest*>(obj));
  if(tapeRequest.get() == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "createObj returned unexpected type "
                     << obj->type() << " for id " << idTapeRequest;
    delete obj;
    throw e;
  }


  ////////////////////////////////////////////////////////////////
  // Create the foreign related objects of the tape request

  cnvSvc()->fillObj(&ad, tapeRequest.get(), castor::OBJ_ClientIdentification);
  if(tapeRequest->client() == NULL) {
    castor::exception::Internal ie;
    ie.getMessage()
      << "Tape request is not linked to a set of client identification data";
    throw ie;
  }
  cnvSvc()->fillObj(&ad, tapeRequest.get(), castor::OBJ_TapeDrive);
  if(tapeRequest->tapeDrive() == NULL) {
    castor::exception::Internal ie;
    ie.getMessage() << "Tape request is not linked to a tape drive";
    throw ie;
  }
  // The destructor of a castor::vdqm::TapeRequest object does not delete the
  // tape drive it points to.  Therefore create an auto pointer to delete the
  // tape drive object if an exception is thrown before the end of this method.
  std::auto_ptr<castor::vdqm::TapeDrive> tapeDrive(tapeRequest->tapeDrive());

  // End of create the foreign related objects of the tape request
  ////////////////////////////////////////////////////////////////

  // Get the foreign related objects of the tape drive of the tape request
  cnvSvc()->fillObj(&ad, tapeRequest->tapeDrive(), castor::OBJ_DeviceGroupName);
  if(tapeRequest->tapeDrive()->deviceGroupName() == NULL) {
    castor::exception::Internal ie;
    ie.getMessage()
      << "Tape drive of tape request is not linked to a device group name";
    throw ie;
  }
  cnvSvc()->fillObj(&ad, tapeRequest->tapeDrive(), castor::OBJ_TapeServer);
  if(tapeRequest->tapeDrive()->tapeServer() == NULL) {
    castor::exception::Internal ie;
    ie.getMessage()
      << "Tape drive of tape request is not linked to a tape server";
    throw ie;
  }

  // Release objects from their auto pointers and return the tape request
  tapeDrive.release();
  return tapeRequest.release();
}


// -----------------------------------------------------------------------
// selectCompatibilitiesForDriveModel
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::selectCompatibilitiesForDriveModel(
  castor::vdqm::TapeDrive *const tapeDrive, const std::string tapeDriveModel)
  throw (castor::exception::Exception) {
  
  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_COMPATIBILITIES_FOR_DRIVE_MODEL_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }
 
  // Get the list of tape drive compatibilities
  try {
    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    
    stmt->setString(1, tapeDriveModel);
    oracle::occi::ResultSet *rs = stmt->executeQuery();

    u_signed64 driveCompatibilityId = 0;
    castor::vdqm::TapeDriveCompatibility* driveCompatibility = NULL;

    while(rs->next() != oracle::occi::ResultSet::END_OF_FETCH) {
      driveCompatibilityId = (u_signed64)rs->getDouble(1);
      ad.setTarget(driveCompatibilityId);
      castor::IObject* obj = cnvSvc()->createObj(&ad);
      driveCompatibility =
        dynamic_cast<castor::vdqm::TapeDriveCompatibility*> (obj);
      
      if (0 == driveCompatibility) {
        castor::exception::Internal e;
        e.getMessage() << "createObj return unexpected type "
                       << obj->type() << " for id " << driveCompatibilityId;
        delete obj;
        throw e;
      }

      tapeDrive->addTapeDriveCompatibilities(driveCompatibility);
    }
    
    stmt->closeResultSet(rs);
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to get the list of tape drive compatibilities: "
      << std::endl << oe.getMessage();
      
    throw ie;
  }
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
  
  // Get the Statement object, creating one if necessary
  oracle::occi::Statement *stmt = NULL;
  const StatementId stmtId = SELECT_TAPE_ACCESS_SPECIFICATIONS_SQL_STMT;
  try {
    if(!(stmt = getStatement(stmtId))) {
      stmt = createStatement(s_statementStrings[stmtId]);
      stmt->setAutoCommit(false);
      storeStatement(stmtId, stmt);
    }
  } catch(oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << oe.getMessage();
    throw ie;
  } catch(castor::exception::Exception& e) {
    castor::exception::Internal ie;
    ie.getMessage() << "Failed to get statement object with ID: "
      << stmtId << ": " << e.getMessage().str();
    throw ie;
  }
 
  try {
    stmt->setString(1, tapeModel);
    rset = stmt->executeQuery();
    
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      stmt->closeResultSet(rset);
      // we found nothing, so let's return NULL
      
      return NULL;
    }
    
    // If we reach this point, then we selected successfully
    // a TapeAccessSpecification object and it's id is in rset
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select TapeAccessSpecification by tapeModel :"
      << std::endl << oe.getMessage();
    throw ie;
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
    
    stmt->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException &oe) {
    handleException(oe);
    castor::exception::Internal ie;
    ie.getMessage()
      << "Unable to select TapeAccessSpecification for id " << id  << " :"
      << std::endl << oe.getMessage();
      
    for (unsigned int i = 0; i < result->size(); i++) {
      delete (*result)[i];
    }
    result->clear();
    delete result;
    result = 0;
      
    throw ie;
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


// -----------------------------------------------------------------------
// getStatement
// -----------------------------------------------------------------------
oracle::occi::Statement *castor::db::ora::OraVdqmSvc::getStatement(
  const StatementId stmtId) {

  std::map<int, oracle::occi::Statement* >::iterator p =
    m_statements.find(stmtId);

  if(p != m_statements.end()) {
    return p->second;
  } else {
    return NULL;
  }

  return NULL;
}


// -----------------------------------------------------------------------
// storeStatement
// -----------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::storeStatement(const StatementId stmtId,
  oracle::occi::Statement *const stmt) {

  m_statements[stmtId] = stmt;
}
