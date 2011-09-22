/******************************************************************************
 *                      OraStagerSvc.cpp
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
 * @(#)$RCSfile: OraStagerSvc.cpp,v $ $Revision: 1.274 $ $Release$ $Date: 2009/08/17 15:08:33 $ $Author: sponcec3 $
 *
 * Implementation of the IStagerSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/ErrorFileRequest.hpp"
#include "castor/stager/StageGetRequest.hpp"
#include "castor/stager/StagePutRequest.hpp"
#include "castor/stager/StageUpdateRequest.hpp"
#include "castor/stager/StagePrepareToGetRequest.hpp"
#include "castor/stager/StagePrepareToPutRequest.hpp"
#include "castor/stager/StagePrepareToUpdateRequest.hpp"
#include "castor/stager/StageRepackRequest.hpp"
#include "castor/stager/StagePutDoneRequest.hpp"
#include "castor/stager/StageRmRequest.hpp"
#include "castor/stager/SetFileGCWeight.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskPool.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/RecallJob.hpp"
#include "castor/stager/FileClass.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/Files2Delete.hpp"
#include "castor/stager/FilesDeleted.hpp"
#include "castor/stager/FilesDeletionFailed.hpp"
#include "castor/stager/GetUpdateDone.hpp"
#include "castor/stager/GetUpdateFailed.hpp"
#include "castor/stager/PutFailed.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "castor/stager/GCFile.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Busy.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/NoSegmentFound.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/exception/SegmentNotAccessible.hpp"
#include "castor/exception/TapeOffline.hpp"
#include "castor/stager/TapeStatusCodes.hpp"
#include "castor/stager/RecallJobStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/StageRepackRequest.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/stager/PriorityMap.hpp"
#include "castor/stager/BulkRequestResult.hpp"
#include "castor/stager/FileResult.hpp"
#include "castor/rh/Client.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/dlf/Dlf.hpp"
#include "OraStagerSvc.hpp"
#include "occi.h"
#include <Cuuid.h>
#include <string>
#include <sstream>
#include <map>
#include <set>
#include <vector>
#include <Cns_api.h>
#include <vmgr_api.h>
#include <Ctape_api.h>
#include <serrno.h>
#include <errno.h>
#include <time.h>
#include <string.h>


//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraStagerSvc>* s_factoryOraStagerSvc =
  new castor::SvcFactory<castor::db::ora::OraStagerSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL statement for subRequestToDo
const std::string castor::db::ora::OraStagerSvc::s_subRequestToDoStatementString =
  "BEGIN subRequestToDo(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29); END;";

/// SQL statement for processBulkRequest
const std::string castor::db::ora::OraStagerSvc::s_processBulkRequestStatementString =
  "BEGIN processBulkRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10); END;";

/// SQL statement for handleRepackSubRequest
const std::string castor::db::ora::OraStagerSvc::s_handleRepackSubRequestStatementString =
  "BEGIN handleRepackSubRequest(:1, :2, :3, :4, :5); END;";

/// SQL statement for subRequestFailedToDo
const std::string castor::db::ora::OraStagerSvc::s_subRequestFailedToDoStatementString =
  "BEGIN subRequestFailedToDo(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10); END;";

/// SQL statement for getDiskCopiesForJob
const std::string castor::db::ora::OraStagerSvc::s_getDiskCopiesForJobStatementString =
  "BEGIN getDiskCopiesForJob(:1, :2, :3); END;";

/// SQL statement for processPrepareRequest
const std::string castor::db::ora::OraStagerSvc::s_processPrepareRequestStatementString =
  "BEGIN processPrepareRequest(:1, :2); END;";

/// SQL statement for processPutDoneRequest
const std::string castor::db::ora::OraStagerSvc::s_processPutDoneRequestStatementString =
  "BEGIN processPutDoneRequest(:1, :2); END;";

/// SQL statement for createDiskCopyReplicaRequest
const std::string castor::db::ora::OraStagerSvc::s_createDiskCopyReplicaRequestStatementString =
  "BEGIN createDiskCopyReplicaRequest(:1, :2, :3, :4, :5, :6); END;";

/// SQL statement for createEmptyFile
const std::string castor::db::ora::OraStagerSvc::s_createEmptyFileStatementString =
  "BEGIN createEmptyFile(:1, :2, :3, :4, :5); END;";

/// SQL statement for selectCastorFile
const std::string castor::db::ora::OraStagerSvc::s_selectCastorFileStatementString =
"BEGIN selectCastorFile(:1, :2, :3, :4, :5, :6, :7, :8, :9); END;";

/// SQL statement for getBestDiskCopyToRead
const std::string castor::db::ora::OraStagerSvc::s_getBestDiskCopyToReadStatementString =
  "BEGIN getBestDiskCopyToRead(:1, :2, :3, :4); END;";

/// SQL statement for updateAndCheckSubRequest
const std::string castor::db::ora::OraStagerSvc::s_updateAndCheckSubRequestStatementString =
  "BEGIN updateAndCheckSubRequest(:1, :2, :3); END;";

/// SQL statement for recreateCastorFile
const std::string castor::db::ora::OraStagerSvc::s_recreateCastorFileStatementString =
  "BEGIN recreateCastorFile(:1, :2, :3, :4, :5, :6); END;";

/// SQL statement for archiveSubReq
const std::string castor::db::ora::OraStagerSvc::s_archiveSubReqStatementString =
  "BEGIN archiveSubReq(:1, :2); END;";

/// SQL statement for stageRm
const std::string castor::db::ora::OraStagerSvc::s_stageRmStatementString =
  "BEGIN stageRm(:1, :2, :3, :4, :5); END;";

/// SQL statement for stageForcedRm
const std::string castor::db::ora::OraStagerSvc::s_stageForcedRmStatementString =
  "BEGIN stageForcedRm(:1, :2); END;";

/// SQL statement for getCFByName
const std::string castor::db::ora::OraStagerSvc::s_getCFByNameStatementString =
  "SELECT /*+ INDEX(CastorFile I_CastorFile_LastKnownFileName) */ id FROM CastorFile WHERE lastKnownFileName = :1";

/// SQL statement for setFileGCWeight
const std::string castor::db::ora::OraStagerSvc::s_setFileGCWeightStatementString =
  "BEGIN setFileGCWeightProc(:1, :2, :3, :4, :5); END;";

/// SQL statement for selectPriority
const std::string castor::db::ora::OraStagerSvc::s_selectPriorityStatementString =
  "BEGIN selectPriority(:1, :2, :3, :4); END;";

/// SQL statement for enterPriority
const std::string castor::db::ora::OraStagerSvc::s_enterPriorityStatementString =
  "BEGIN enterPriority(:1, :2, :3); END;";

/// SQL statement for deletePriority
const std::string castor::db::ora::OraStagerSvc::s_deletePriorityStatementString =
  "BEGIN deletePriority(:1, :2); END;";

/// SQL statement for getConfigOption
const std::string castor::db::ora::OraStagerSvc::s_getConfigOptionStatementString =
  "SELECT getConfigOption(:1, :2, :3) FROM Dual";

//------------------------------------------------------------------------------
// OraStagerSvc
//------------------------------------------------------------------------------
castor::db::ora::OraStagerSvc::OraStagerSvc(const std::string name) :
  OraCommonSvc(name),
  m_subRequestToDoStatement(0),
  m_processBulkRequestStatement(0),
  m_handleRepackSubRequestStatement(0),
  m_subRequestFailedToDoStatement(0),
  m_getDiskCopiesForJobStatement(0),
  m_processPrepareRequestStatement(0),
  m_processPutDoneRequestStatement(0),
  m_createDiskCopyReplicaRequestStatement(0),
  m_createEmptyFileStatement(0),
  m_selectCastorFileStatement(0),
  m_getBestDiskCopyToReadStatement(0),
  m_updateAndCheckSubRequestStatement(0),
  m_recreateCastorFileStatement(0),
  m_archiveSubReqStatement(0),
  m_stageRmStatement(0),
  m_stageForcedRmStatement(0),
  m_getCFByNameStatement(0),
  m_setFileGCWeightStatement(0),
  m_selectPriorityStatement(0),
  m_enterPriorityStatement(0),
  m_deletePriorityStatement(0),
  m_getConfigOptionStatement(0) {
}

//------------------------------------------------------------------------------
// ~OraStagerSvc
//------------------------------------------------------------------------------
castor::db::ora::OraStagerSvc::~OraStagerSvc() throw() {
  reset();
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraStagerSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraStagerSvc::ID() {
  return castor::SVC_ORASTAGERSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::reset() throw() {
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if (m_subRequestToDoStatement) deleteStatement(m_subRequestToDoStatement);
    if (m_processBulkRequestStatement) deleteStatement(m_processBulkRequestStatement);
    if (m_handleRepackSubRequestStatement) deleteStatement(m_handleRepackSubRequestStatement);
    if (m_subRequestFailedToDoStatement) deleteStatement(m_subRequestFailedToDoStatement);
    if (m_getDiskCopiesForJobStatement) deleteStatement(m_getDiskCopiesForJobStatement);
    if (m_processPrepareRequestStatement) deleteStatement(m_processPrepareRequestStatement);
    if (m_processPutDoneRequestStatement) deleteStatement(m_processPutDoneRequestStatement);
    if (m_createDiskCopyReplicaRequestStatement) deleteStatement(m_createDiskCopyReplicaRequestStatement);
    if (m_createEmptyFileStatement) deleteStatement(m_createEmptyFileStatement);
    if (m_selectCastorFileStatement) deleteStatement(m_selectCastorFileStatement);
    if (m_getBestDiskCopyToReadStatement) deleteStatement(m_getBestDiskCopyToReadStatement);
    if (m_updateAndCheckSubRequestStatement) deleteStatement(m_updateAndCheckSubRequestStatement);
    if (m_recreateCastorFileStatement) deleteStatement(m_recreateCastorFileStatement);
    if (m_archiveSubReqStatement) deleteStatement(m_archiveSubReqStatement);
    if (m_stageRmStatement) deleteStatement(m_stageRmStatement);
    if (m_stageForcedRmStatement) deleteStatement(m_stageForcedRmStatement);
    if (m_getCFByNameStatement) deleteStatement(m_getCFByNameStatement);
    if (m_setFileGCWeightStatement) deleteStatement(m_setFileGCWeightStatement);
    if (m_selectPriorityStatement) deleteStatement(m_selectPriorityStatement);
    if (m_enterPriorityStatement) deleteStatement(m_enterPriorityStatement);
    if (m_deletePriorityStatement) deleteStatement(m_deletePriorityStatement);
    if (m_getConfigOptionStatement) deleteStatement(m_getConfigOptionStatement);
  } catch (castor::exception::Exception& ignored) {};

  // Now reset all pointers to 0
  m_subRequestToDoStatement = 0;
  m_processBulkRequestStatement = 0;
  m_handleRepackSubRequestStatement = 0;
  m_subRequestFailedToDoStatement = 0;
  m_getDiskCopiesForJobStatement = 0;
  m_processPrepareRequestStatement = 0;
  m_processPutDoneRequestStatement = 0;
  m_createDiskCopyReplicaRequestStatement = 0;
  m_createEmptyFileStatement = 0;
  m_selectCastorFileStatement = 0;
  m_getBestDiskCopyToReadStatement = 0;
  m_updateAndCheckSubRequestStatement = 0;
  m_recreateCastorFileStatement = 0;
  m_archiveSubReqStatement = 0;
  m_stageRmStatement = 0;
  m_stageForcedRmStatement = 0;
  m_getCFByNameStatement = 0;
  m_setFileGCWeightStatement = 0;
  m_selectPriorityStatement = 0;
  m_enterPriorityStatement = 0;
  m_deletePriorityStatement = 0;
  m_getConfigOptionStatement = 0;
}

//------------------------------------------------------------------------------
// subRequestToDo
//------------------------------------------------------------------------------
castor::stager::SubRequest*
castor::db::ora::OraStagerSvc::subRequestToDo
(const std::string service)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_subRequestToDoStatement) {
      m_subRequestToDoStatement =
        createStatement(s_subRequestToDoStatementString);
      m_subRequestToDoStatement->registerOutParam
        (2, oracle::occi::OCCIDOUBLE);
      m_subRequestToDoStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (6, oracle::occi::OCCIDOUBLE);
      m_subRequestToDoStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (8, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (9, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (10, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (11, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (12, oracle::occi::OCCIDOUBLE);
      m_subRequestToDoStatement->registerOutParam
        (13, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (14, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (15, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (16, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (17, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (18, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (19, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (20, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (21, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (22, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (23, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (24, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (25, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (26, oracle::occi::OCCIDOUBLE);
      m_subRequestToDoStatement->registerOutParam
        (27, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (28, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (29, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->setAutoCommit(true);
    }
    m_subRequestToDoStatement->setString(1, service);

    // execute the statement and see whether we found something
    unsigned int rc = m_subRequestToDoStatement->executeUpdate();
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "subRequestToDo : "
        << "unable to get next SubRequest to process.";
      throw ex;
    }
    u_signed64 srId = (u_signed64)m_subRequestToDoStatement->getDouble(2);
    if (0 == srId) {
      // Found no SubRequest to handle
      return 0;
    }
    // Create result
    castor::stager::SubRequest* result =
      new castor::stager::SubRequest();
    result->setId(srId);
    result->setSvcHandler(service);
    result->setRetryCounter(m_subRequestToDoStatement->getInt(3));
    result->setFileName(m_subRequestToDoStatement->getString(4));
    result->setProtocol(m_subRequestToDoStatement->getString(5));
    result->setXsize((u_signed64)m_subRequestToDoStatement->getDouble(6));
    result->setStatus(castor::stager::SUBREQUEST_WAITSCHED);
    result->setModeBits(m_subRequestToDoStatement->getInt(7));
    result->setFlags(m_subRequestToDoStatement->getInt(8));
    result->setSubreqId(m_subRequestToDoStatement->getString(9));
    result->setAnswered(m_subRequestToDoStatement->getInt(10));
    result->setReqType(m_subRequestToDoStatement->getInt(11));
    castor::stager::FileRequest* req;
    switch(result->reqType()) {
      case castor::OBJ_StageGetRequest:
        req = new castor::stager::StageGetRequest();
        break;
      case castor::OBJ_StagePutRequest:
        req = new castor::stager::StagePutRequest();
        break;
      case castor::OBJ_StageUpdateRequest:
        req = new castor::stager::StageUpdateRequest();
        break;
      case castor::OBJ_StagePrepareToGetRequest:
        req = new castor::stager::StagePrepareToGetRequest();
        break;
      case castor::OBJ_StagePrepareToPutRequest:
        req = new castor::stager::StagePrepareToPutRequest();
        break;
      case castor::OBJ_StagePrepareToUpdateRequest:
        req = new castor::stager::StagePrepareToUpdateRequest();
        break;
      case castor::OBJ_StageRepackRequest:
        {
          castor::stager::StageRepackRequest* rreq =
            new castor::stager::StageRepackRequest();
          rreq->setRepackVid(m_subRequestToDoStatement->getString(25));
          req = rreq;
          break;
        }
      case castor::OBJ_StagePutDoneRequest:
        req = new castor::stager::StagePutDoneRequest();
        break;
      case castor::OBJ_StageRmRequest:
        req = new castor::stager::StageRmRequest();
        break;
      case castor::OBJ_SetFileGCWeight:
        {
          castor::stager::SetFileGCWeight* greq =
            new castor::stager::SetFileGCWeight();
          greq->setWeight((u_signed64)m_subRequestToDoStatement->getDouble(26));
          req = greq;
          break;
        }
    }
    result->setRequest(req);
    req->addSubRequests(result);
    req->setId((u_signed64)m_subRequestToDoStatement->getDouble(12));
    req->setFlags(m_subRequestToDoStatement->getInt(13));
    req->setUserName(m_subRequestToDoStatement->getString(14));
    req->setEuid(m_subRequestToDoStatement->getInt(15));
    req->setEgid(m_subRequestToDoStatement->getInt(16));
    req->setMask(m_subRequestToDoStatement->getInt(17));
    req->setPid(m_subRequestToDoStatement->getInt(18));
    req->setMachine(m_subRequestToDoStatement->getString(19));
    req->setSvcClassName(m_subRequestToDoStatement->getString(20));
    req->setUserTag(m_subRequestToDoStatement->getString(21));
    req->setReqId(m_subRequestToDoStatement->getString(22));
    req->setCreationTime(m_subRequestToDoStatement->getInt(23));
    req->setLastModificationTime(m_subRequestToDoStatement->getInt(24));
    castor::rh::Client* cl = new castor::rh::Client();
    req->setClient(cl);
    cl->setIpAddress(m_subRequestToDoStatement->getInt(27));
    cl->setPort(m_subRequestToDoStatement->getInt(28));
    cl->setVersion(m_subRequestToDoStatement->getInt(29));
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in subRequestToDo."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processBulkRequest
//------------------------------------------------------------------------------
castor::IObject*
castor::db::ora::OraStagerSvc::processBulkRequest
(const std::string service)
  throw (castor::exception::Exception) {
  IObject *retObj = 0;
  try {
    // Check whether the statements are ok
    if (0 == m_processBulkRequestStatement) {
      m_processBulkRequestStatement =
        createStatement(s_processBulkRequestStatementString);
      m_processBulkRequestStatement->registerOutParam
        (2, oracle::occi::OCCIINT);
      m_processBulkRequestStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_processBulkRequestStatement->registerOutParam
        (4, oracle::occi::OCCIINT);
      m_processBulkRequestStatement->registerOutParam
        (5, oracle::occi::OCCIINT);
      m_processBulkRequestStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048);
      m_processBulkRequestStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
      m_processBulkRequestStatement->registerOutParam
        (8, oracle::occi::OCCIINT);
      m_processBulkRequestStatement->registerOutParam
        (9, oracle::occi::OCCISTRING, 2048);
      m_processBulkRequestStatement->registerOutParam
        (10, oracle::occi::OCCICURSOR);
      m_processBulkRequestStatement->setAutoCommit(true);
    }
    m_processBulkRequestStatement->setString(1, service);

    // execute the statement and see whether we found something
    unsigned int rc = m_processBulkRequestStatement->executeUpdate();
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "processBulkRequest : unable to process bulk request.";
      throw ex;
    }
    int reqType = m_processBulkRequestStatement->getInt(3);
    switch (reqType) {
    case castor::OBJ_StageAbortRequest:
      {
        // Create result
        castor::stager::BulkRequestResult* result =
          new castor::stager::BulkRequestResult();
        result->setReqType((castor::ObjectsIds)reqType);
        result->client().setIpAddress(m_processBulkRequestStatement->getInt(4));
        result->client().setPort(m_processBulkRequestStatement->getInt(5));
        result->setReqId(m_processBulkRequestStatement->getString(6));
        oracle::occi::ResultSet *rset =
          m_processBulkRequestStatement->getCursor(10);
        while (rset->next() != oracle::occi::ResultSet::END_OF_FETCH) {
          // create the new Object
          castor::stager::FileResult fr;
          // Now retrieve and set members
          fr.setFileId((u_signed64)rset->getDouble(1));
          fr.setNsHost(rset->getString(2));
          fr.setErrorCode(rset->getInt(3));
          fr.setErrorMessage(rset->getString(4));
          result->subResults().push_back(fr);
        }
        delete rset;
        retObj = result;
      }
      break;
    case castor::OBJ_StageRepackRequest:
      {
        castor::stager::StageRepackRequest* req =
          new castor::stager::StageRepackRequest();
        castor::rh::Client* client = new castor::rh::Client();
        client->setIpAddress(m_processBulkRequestStatement->getInt(4));
        client->setPort(m_processBulkRequestStatement->getInt(5));
        req->setClient(client);
        req->setId(m_processBulkRequestStatement->getInt(2));
        req->setReqId(m_processBulkRequestStatement->getString(6));
        req->setEuid(m_processBulkRequestStatement->getInt(7));
        req->setEgid(m_processBulkRequestStatement->getInt(8));
        req->setRepackVid(m_processBulkRequestStatement->getString(9));
        retObj = req;
      }
      break;
    }
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in processBulkRequest."
      << std::endl << e.what();
    throw ex;
  }
  return retObj;
}

//------------------------------------------------------------------------------
// handleRepackSubRequest
//------------------------------------------------------------------------------
castor::stager::BulkRequestResult*
castor::db::ora::OraStagerSvc::handleRepackSubRequest (const u_signed64 subReqId)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_handleRepackSubRequestStatement) {
      m_handleRepackSubRequestStatement =
        createStatement(s_handleRepackSubRequestStatementString);
      m_handleRepackSubRequestStatement->registerOutParam
        (2, oracle::occi::OCCIINT);
      m_handleRepackSubRequestStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_handleRepackSubRequestStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_handleRepackSubRequestStatement->registerOutParam
        (5, oracle::occi::OCCICURSOR);
      m_handleRepackSubRequestStatement->setAutoCommit(true);
    }
    m_handleRepackSubRequestStatement->setDouble(1, subReqId);

    // execute the statement and see whether we found something
    unsigned int rc = m_handleRepackSubRequestStatement->executeUpdate();
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "handleRepackSubRequest : unable to handle repack subrequest.";
      throw ex;
    }
    // Create result
    castor::stager::BulkRequestResult* result =
      new castor::stager::BulkRequestResult();
    result->setReqType(castor::OBJ_StageRepackRequest);
    result->client().setIpAddress(m_handleRepackSubRequestStatement->getInt(2));
    result->client().setPort(m_handleRepackSubRequestStatement->getInt(3));
    result->setReqId(m_handleRepackSubRequestStatement->getString(4));
    oracle::occi::ResultSet *rset =
      m_handleRepackSubRequestStatement->getCursor(5);
    while (rset->next() != oracle::occi::ResultSet::END_OF_FETCH) {
      // create the new Object
      castor::stager::FileResult fr;
      // Now retrieve and set members
      fr.setFileId((u_signed64)rset->getDouble(1));
      fr.setNsHost(rset->getString(2));
      fr.setErrorCode(rset->getInt(3));
      fr.setErrorMessage(rset->getString(4));
      result->subResults().push_back(fr);
    }
    delete rset;
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in handleRepackSubRequest."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// subRequestFailedToDo
//------------------------------------------------------------------------------
castor::stager::SubRequest*
castor::db::ora::OraStagerSvc::subRequestFailedToDo()
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_subRequestFailedToDoStatement) {
      m_subRequestFailedToDoStatement =
        createStatement(s_subRequestFailedToDoStatementString);
      m_subRequestFailedToDoStatement->registerOutParam
        (1, oracle::occi::OCCIDOUBLE);
      m_subRequestFailedToDoStatement->registerOutParam
        (2, oracle::occi::OCCISTRING, 2048);
      m_subRequestFailedToDoStatement->registerOutParam
        (3, oracle::occi::OCCISTRING, 2048);
      m_subRequestFailedToDoStatement->registerOutParam
        (4, oracle::occi::OCCIINT);
      m_subRequestFailedToDoStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048);
      m_subRequestFailedToDoStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048);
      m_subRequestFailedToDoStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
      m_subRequestFailedToDoStatement->registerOutParam
        (8, oracle::occi::OCCIINT);
      m_subRequestFailedToDoStatement->registerOutParam
        (9, oracle::occi::OCCIINT);
      m_subRequestFailedToDoStatement->registerOutParam
        (10, oracle::occi::OCCIDOUBLE);
      m_subRequestFailedToDoStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    unsigned int rc =
      m_subRequestFailedToDoStatement->executeUpdate();
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "subRequestFailedToDo : "
        << "unable to get next SubRequest to process.";
      throw ex;
    }
    u_signed64 srId = (u_signed64)m_subRequestFailedToDoStatement->getDouble(1);
    if (0 == srId) {
      // Found no SubRequest to handle
      return 0;
    }
    // Create result
    castor::stager::SubRequest* result =
      new castor::stager::SubRequest();
    result->setId((u_signed64)m_subRequestFailedToDoStatement->getDouble(1));
    result->setFileName(m_subRequestFailedToDoStatement->getString(2));
    result->setStatus(castor::stager::SUBREQUEST_FAILED_ANSWERING);
    result->setSubreqId(m_subRequestFailedToDoStatement->getString(3));
    result->setErrorCode(m_subRequestFailedToDoStatement->getInt(4));
    result->setErrorMessage(m_subRequestFailedToDoStatement->getString(5));
    result->setSvcHandler("ErrorSvc");
    castor::stager::ErrorFileRequest* req = new castor::stager::ErrorFileRequest();
    result->setRequest(req);
    req->addSubRequests(result);
    req->setReqId(m_subRequestFailedToDoStatement->getString(6));
    castor::rh::Client* cl = new castor::rh::Client();
    req->setClient(cl);
    cl->setIpAddress(m_subRequestFailedToDoStatement->getInt(7));
    cl->setPort(m_subRequestFailedToDoStatement->getInt(8));
    cl->setVersion(m_subRequestFailedToDoStatement->getInt(9));
    u_signed64 fileid = (u_signed64)m_subRequestFailedToDoStatement->getDouble(10);
    if(fileid > 0) {
      castor::stager::CastorFile* cf = new castor::stager::CastorFile();
      cf->setFileId(fileid);
      result->setCastorFile(cf);
    }
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in subRequestFailedToDo."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getDiskCopiesForJob
//------------------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::getDiskCopiesForJob
(castor::stager::SubRequest* subreq,
 std::list<castor::stager::DiskCopyForRecall*>& sources)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_getDiskCopiesForJobStatement) {
      m_getDiskCopiesForJobStatement =
        createStatement(s_getDiskCopiesForJobStatementString);
      m_getDiskCopiesForJobStatement->registerOutParam
        (2, oracle::occi::OCCIINT);
      m_getDiskCopiesForJobStatement->registerOutParam
        (3, oracle::occi::OCCICURSOR);
    }
    // execute the statement and see whether we found something
    m_getDiskCopiesForJobStatement->setDouble(1, subreq->id());
    unsigned int nb = m_getDiskCopiesForJobStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "getDiskCopiesForJob : "
        << "unable to know whether the SubRequest should be scheduled.";
      throw ex;
    }
    int status = m_getDiskCopiesForJobStatement->getInt(2);

    if (castor::stager::DISKCOPY_STAGED == status ||
        castor::stager::DISKCOPY_WAITDISK2DISKCOPY == status) {
      // diskcopies are available, list them
      try {
        oracle::occi::ResultSet *rs =
          m_getDiskCopiesForJobStatement->getCursor(3);
        // Run through the cursor
        oracle::occi::ResultSet::Status status = rs->next();
        while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
          castor::stager::DiskCopyForRecall* item =
            new castor::stager::DiskCopyForRecall();
          item->setId((u_signed64) rs->getDouble(1));
          item->setPath(rs->getString(2));
          item->setStatus((castor::stager::DiskCopyStatusCodes)rs->getInt(3));
          item->setFsWeight(rs->getFloat(4));
          item->setMountPoint(rs->getString(5));
          item->setDiskServer(rs->getString(6));
          sources.push_back(item);
          status = rs->next();
        }
        m_getDiskCopiesForJobStatement->closeResultSet(rs);
      } catch (oracle::occi::SQLException e) {
        handleException(e);
        if (e.getErrorCode() != 24338) {
          // if not "statement handle not executed"
          // it's really wrong, else, it's normal
          throw e;
        }
      }
    } else if (-1 == status || -2 == status) {
      // In case of error, we can safely commit
      commit();
    }
    return status;
     /* -2,-3 = SubRequest put in WAITSUBREQ (-3 for disk copy replication)
      * -1 = no schedule, user error
      *  0 = DISKCOPY_STAGED, disk copies available
      *  1 = DISKCOPY_WAITDISK2DISKCOPY, disk copies available and replication allowed
      *  2 = DISKCOPY_WAITTAPERECALL, a tape recall is needed
      *  5 = DISKCOPY_WAITFS, update inside prepareToPut, recreateCastorFile is needed */

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getDiskCopiesForJob."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processPrepareRequest
//------------------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::processPrepareRequest
(castor::stager::SubRequest* subreq)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_processPrepareRequestStatement) {
      m_processPrepareRequestStatement =
        createStatement(s_processPrepareRequestStatementString);
      m_processPrepareRequestStatement->registerOutParam
        (2, oracle::occi::OCCIINT);
    }
    // execute the statement and see whether we found something
    m_processPrepareRequestStatement->setDouble(1, subreq->id());
    unsigned int nb =
      m_processPrepareRequestStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "processPrepareRequest : unable to perform the processing.";
      throw ex;
    }
    // return result
    int result = m_processPrepareRequestStatement->getInt(2);
     /* -2 = SubRequest put in WAITSUBREQ (only in case of Repack)
      * -1 = user error
      *  0 = DISKCOPY_STAGED, disk copies available
      *  1 = DISKCOPY_WAITDISK2DISKCOPY, a disk copy replication is needed
      *  2 = DISKCOPY_WAITTAPERECALL, a tape recall is needed */
    // In case of error, we can safely commit
    if (-1 == result) commit();
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in processPrepareRequest."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processPutDoneRequest
//------------------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::processPutDoneRequest
(castor::stager::SubRequest* subreq)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_processPutDoneRequestStatement) {
      m_processPutDoneRequestStatement =
        createStatement(s_processPutDoneRequestStatementString);
      m_processPutDoneRequestStatement->registerOutParam
        (2, oracle::occi::OCCIINT);
      m_processPutDoneRequestStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_processPutDoneRequestStatement->setDouble(1, subreq->id());
    unsigned int nb =
      m_processPutDoneRequestStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "processPutDoneRequest : unable to perform the processing.";
      throw ex;
    }
    // return status
    int result = m_processPutDoneRequestStatement->getInt(2);
    // In case of error, we can safely commit
    if (0 == result) commit();
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in processPutDoneRequest."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createDiskCopyReplicaRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::createDiskCopyReplicaRequest
(const castor::stager::SubRequest* subreq,
 const castor::stager::DiskCopyForRecall* srcDiskCopy,
 const castor::stager::SvcClass* srcSc,
 const castor::stager::SvcClass* destSc,
 const bool internal)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_createDiskCopyReplicaRequestStatement) {
      m_createDiskCopyReplicaRequestStatement =
        createStatement(s_createDiskCopyReplicaRequestStatementString);
    }
    // Execute the statement
    m_createDiskCopyReplicaRequestStatement->setDouble(1, (internal) ? 0 : subreq->id());
    m_createDiskCopyReplicaRequestStatement->setDouble(2, srcDiskCopy->id());
    m_createDiskCopyReplicaRequestStatement->setDouble(3, srcSc->id());
    m_createDiskCopyReplicaRequestStatement->setDouble(4, destSc->id());
    m_createDiskCopyReplicaRequestStatement->setInt(5, subreq->request()->euid());
    m_createDiskCopyReplicaRequestStatement->setInt(6, subreq->request()->egid());

    unsigned int rc =
      m_createDiskCopyReplicaRequestStatement->executeUpdate();
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "createDiskCopyReplicaRequest : unable to create the request.";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in createDiskCopyReplicaRequest."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createEmptyFile
//------------------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::createEmptyFile
(castor::stager::SubRequest* subreq,
 bool schedule)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_createEmptyFileStatement) {
      m_createEmptyFileStatement =
        createStatement(s_createEmptyFileStatementString);
      m_createEmptyFileStatement->setAutoCommit(true);
    }
    // Execute the statement
    m_createEmptyFileStatement->setDouble(1, subreq->castorFile()->id());
    m_createEmptyFileStatement->setDouble(2, subreq->castorFile()->fileId());
    m_createEmptyFileStatement->setString(3, subreq->castorFile()->nsHost());
    m_createEmptyFileStatement->setDouble(4, subreq->id());
    m_createEmptyFileStatement->setInt(5, (int)schedule);
    unsigned int rc =
      m_createEmptyFileStatement->executeUpdate();
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "createEmptyFile : unable to create the empty file.";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in createEmptyFile."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createRecallCandidate
//------------------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::createRecallCandidate
(castor::stager::SubRequest* subreq,
 castor::stager::SvcClass* svcClass,
 castor::stager::Tape* &tape)
  throw (castor::exception::Exception) {

  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  ad.setTarget(subreq->id());
  castor::stager::DiskCopy *dc = 0;

  try {
      castor::stager::CastorFile* cf = subreq->castorFile();

      // Create needed RecallJob(ies) and Segment(s). We already
      // have a lock on the castorfile and so we prevent the
      // concurrent creation of 2 recalls for a single file
      if (!subreq->request()) {
        cnvSvc()->fillObj(&ad, subreq, OBJ_FileRequest, false);
      }
      cnvSvc()->fillObj(&ad, cf, OBJ_FileClass, false);
      int nbCopies = cf->fileClass()->nbCopies();
      createRecallJobSegments
        (cf, subreq->request()->euid(),
         subreq->request()->egid(), svcClass, tape,
         &nbCopies);

      // If we are here, we do have segments to recall;
      // create DiskCopy and store in the DB so we have the id for
      // the following operation; we don't fillRep() from castorFile
      // to the diskCopy as there may be other diskcopies, which are
      // not in memory now and would loose their FK to the castorFile
      dc = new castor::stager::DiskCopy();
      dc->setStatus(castor::stager::DISKCOPY_WAITTAPERECALL);
      dc->setCreationTime(time(NULL));
      dc->setCastorFile(cf);
      dc->setOwneruid(subreq->request()->euid());
      dc->setOwnergid(subreq->request()->egid());
      cf->addDiskCopies(dc);
      cnvSvc()->createRep(&ad, dc, false);
      cnvSvc()->fillRep(&ad, dc, OBJ_CastorFile, false);
      cnvSvc()->updateRep(&ad, cf, false);

      // Build the path, since we now have the DB id from the DiskCopy;
      // note that this line duplicates the buildPathFromFileId PL/SQL
      // procedure, but we don't want to call Oracle only for it
      std::ostringstream spath;
      if (cf->fileId() % 100 < 10)
        spath << "0";
      spath << cf->fileId() % 100 << "/" << cf->fileId()
            << "@" << cf->nsHost() << "." << dc->id();
      dc->setPath(spath.str());
      cnvSvc()->updateRep(&ad, dc, false);

      // We link the subreq with the diskcopy
      subreq->setDiskcopy(dc);
      cnvSvc()->fillRep(&ad, subreq, OBJ_DiskCopy, false);

      // Set SubRequest to WAITTAPERECALL
      subreq->setStatus(castor::stager::SUBREQUEST_WAITTAPERECALL);
      cnvSvc()->updateRep(&ad, subreq, false);

      cnvSvc()->commit();
      subreq->setDiskcopy(0);

      // Cleanup
      delete dc;

      return 1;
  } catch (castor::exception::NoSegmentFound& e) {
    // File has no copy on tape. In such a case we set the
    // subrequest to failed and we commit the transaction
    try {
      subreq->setStatus(castor::stager::SUBREQUEST_FAILED);
      subreq->setErrorCode(e.code());
      cnvSvc()->updateRep(&ad, subreq, true);
      return 0;
    }
    catch(castor::exception::Exception& e) {
      if (tape) {
        delete tape;
        tape = 0;
      }
      // Should never happen
      castor::exception::Internal ex2;
      ex2.getMessage() << "Couldn't fail subrequest in createRecallCandidate: "
		       << std::endl << e.getMessage().str();
      throw ex2;
    }
  } catch(castor::exception::Exception& forward) {
    // Any other exception is forwarded, the stager will reply to the client,
    // log the error and close the db transaction.
    // SegmentNotAccessible and TapeOffline errors are included here.
    if(dc) {
      delete dc;
      subreq->setDiskcopy(0);
    }
    if (tape) {
      delete tape;
      tape = 0;
    }
    throw(forward);
  }
}

//------------------------------------------------------------------------------
// selectCastorFile
//------------------------------------------------------------------------------
castor::stager::CastorFile*
castor::db::ora::OraStagerSvc::selectCastorFile(castor::stager::SubRequest* subreq,
  const Cns_fileid* cnsFileId, const Cns_filestatcs* cnsFileStat)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectCastorFileStatement) {
    m_selectCastorFileStatement =
      createStatement(s_selectCastorFileStatementString);
    m_selectCastorFileStatement->registerOutParam
      (8, oracle::occi::OCCIDOUBLE);
    m_selectCastorFileStatement->registerOutParam
      (9, oracle::occi::OCCIDOUBLE);
    m_selectCastorFileStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  try {
    m_selectCastorFileStatement->setDouble(1, cnsFileId->fileid);
    m_selectCastorFileStatement->setString(2, cnsFileId->server);
    m_selectCastorFileStatement->setDouble(3, cnsFileStat->fileclass);
    m_selectCastorFileStatement->setDouble(4, cnsFileStat->filesize);
    m_selectCastorFileStatement->setString(5, subreq->fileName());
    m_selectCastorFileStatement->setDouble(6, subreq->id());
    m_selectCastorFileStatement->setDouble(7, cnsFileStat->mtime);

    int nb  = m_selectCastorFileStatement->executeUpdate();
    if (0 == nb) {
      // Nothing found, throw exception
      castor::exception::Internal e;
      e.getMessage()
        << "selectCastorFile returned no CastorFile";
      throw e;
    }
    // Found the CastorFile, so create it in memory
    castor::stager::CastorFile* result =
      new castor::stager::CastorFile();
    result->setId((u_signed64)m_selectCastorFileStatement->getDouble(8));
    result->setFileId(cnsFileId->fileid);
    result->setNsHost(cnsFileId->server);
    result->setLastKnownFileName(subreq->fileName());
    result->setFileSize((u_signed64)m_selectCastorFileStatement->getDouble(9));
    result->setLastUpdateTime(cnsFileStat->mtime);
    subreq->setCastorFile(result);    // executed in the PL/SQL procedure
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select castorFile by fileId :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getBestDiskCopyToRead
//------------------------------------------------------------------------------
castor::stager::DiskCopyInfo*
castor::db::ora::OraStagerSvc::getBestDiskCopyToRead
(const castor::stager::CastorFile *castorFile,
 const castor::stager::SvcClass *svcClass)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  castor::stager::DiskCopyInfo *result = 0;
  try {
    if (0 == m_getBestDiskCopyToReadStatement) {
      m_getBestDiskCopyToReadStatement =
	createStatement(s_getBestDiskCopyToReadStatementString);
      m_getBestDiskCopyToReadStatement->registerOutParam
	(3, oracle::occi::OCCISTRING, 2048);
      m_getBestDiskCopyToReadStatement->registerOutParam
	(4, oracle::occi::OCCISTRING, 2048);
      m_getBestDiskCopyToReadStatement->setAutoCommit(true);
    }
    // Execute statement and get result
    m_getBestDiskCopyToReadStatement->setDouble(1, castorFile->id());
    m_getBestDiskCopyToReadStatement->setDouble(2, svcClass->id());

    int nb = m_getBestDiskCopyToReadStatement->executeUpdate();
    if (0 == nb) {
      // Nothing found, throw exception
      castor::exception::Internal e;
      e.getMessage()
        << "getBestDiskCopyToRead returned no DiskCopyInfo";
      throw e;
    }

    // Found the CastorFile, so create it in memory
    result = new castor::stager::DiskCopyInfo();
    result->setDiskServer(m_getBestDiskCopyToReadStatement->getString(3));
    result->setDiskCopyPath(m_getBestDiskCopyToReadStatement->getString(4));
    result->setFileId(castorFile->fileId());
    result->setNsHost(castorFile->nsHost());
    result->setSize(castorFile->fileSize());
    return result;

  } catch (oracle::occi::SQLException e) {
    // Free resources
    if (result) {
      delete result;
      result = 0;
    };
    // Ignore ORA-01403: no data found
    if (1403 == e.getErrorCode()) {
      return result;
    }
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getDiskCopyToRead."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateAndCheckSubRequest
//------------------------------------------------------------------------------
bool castor::db::ora::OraStagerSvc::updateAndCheckSubRequest
(castor::stager::SubRequest* subreq)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_updateAndCheckSubRequestStatement) {
      m_updateAndCheckSubRequestStatement =
        createStatement(s_updateAndCheckSubRequestStatementString);
      m_updateAndCheckSubRequestStatement->registerOutParam
        (3, oracle::occi::OCCIDOUBLE);
      m_updateAndCheckSubRequestStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_updateAndCheckSubRequestStatement->setDouble(1, subreq->id());
    m_updateAndCheckSubRequestStatement->setInt(2, subreq->status());
    unsigned int nb = m_updateAndCheckSubRequestStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "updateAndCheckSubRequest did not return any result.";
      throw ex;
    }
    // return
    return m_updateAndCheckSubRequestStatement->getDouble(3) != 0;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateAndCheckSubRequest."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// recreateCastorFile
//------------------------------------------------------------------------------
castor::stager::DiskCopyForRecall*
castor::db::ora::OraStagerSvc::recreateCastorFile
(castor::stager::CastorFile *castorFile,
 castor::stager::SubRequest *subreq)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_recreateCastorFileStatement) {
      m_recreateCastorFileStatement =
        createStatement(s_recreateCastorFileStatementString);
      m_recreateCastorFileStatement->registerOutParam
        (3, oracle::occi::OCCIDOUBLE);
      m_recreateCastorFileStatement->registerOutParam
        (4, oracle::occi::OCCIINT);
      m_recreateCastorFileStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048);
      m_recreateCastorFileStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048);
    }
    // execute the statement and see whether we found something
    m_recreateCastorFileStatement->setDouble(1, castorFile->id());
    m_recreateCastorFileStatement->setDouble(2, subreq->id());
    unsigned int nb = m_recreateCastorFileStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "recreateCastorFile did not return any result.";
      throw ex;
    }
    // get the result
    u_signed64 id =
      (u_signed64)m_recreateCastorFileStatement->getDouble(3);
    // case of no recreation due to user error, commit and return 0
    if (0 == id) {
      commit();
      return 0;
    }
    // Otherwise, build a DiskCopyForRecall.
    // The case of status == WAITFS_SCHEDULING means that
    // the subRequest has been put in wait: the stager will handle this case
    castor::stager::DiskCopyForRecall *result =
      new castor::stager::DiskCopyForRecall();
    result->setId(id);
    result->setStatus((castor::stager::DiskCopyStatusCodes)
      m_recreateCastorFileStatement->getInt(4));
    result->setMountPoint(m_recreateCastorFileStatement->getString(5));
    result->setDiskServer(m_recreateCastorFileStatement->getString(6));
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in recreateCastorFile."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// archiveSubReq
//------------------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::archiveSubReq
(u_signed64 subReqId, castor::stager::SubRequestStatusCodes finalStatus)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_archiveSubReqStatement) {
    m_archiveSubReqStatement =
      createStatement(s_archiveSubReqStatementString);
    m_archiveSubReqStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  try {
    m_archiveSubReqStatement->setDouble(1, subReqId);
    m_archiveSubReqStatement->setInt(2, finalStatus);
    m_archiveSubReqStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to archive subRequest :"
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// stageRm
//------------------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::stageRm
(castor::stager::SubRequest* subreq, const u_signed64 fileId,
 const std::string nsHost, const u_signed64 svcClassId)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_stageRmStatement) {
      m_stageRmStatement =
        createStatement(s_stageRmStatementString);
      m_stageRmStatement->registerOutParam
        (5, oracle::occi::OCCIINT);
      m_stageRmStatement->setAutoCommit(true);
      m_stageForcedRmStatement =
        createStatement(s_stageForcedRmStatementString);
      m_stageForcedRmStatement->setAutoCommit(true);
      m_getCFByNameStatement =
        createStatement(s_getCFByNameStatementString);
    }
    if(fileId > 0) {
      // the file exists, try to execute stageRm
      m_stageRmStatement->setDouble(1, subreq->id());
      m_stageRmStatement->setDouble(2, fileId);
      m_stageRmStatement->setString(3, nsHost);
      m_stageRmStatement->setDouble(4, svcClassId);
      unsigned int nb = m_stageRmStatement->executeUpdate();
      if (0 == nb) {
        castor::exception::Internal ex;
        ex.getMessage()
          << "stageRm : No return code after PL/SQL call.";
        throw ex;
      }
      // Return the return code given by the procedure
      return m_stageRmStatement->getInt(5);
    }
    else {
      // the file does not exist, try to see whether it got renamed
      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      m_getCFByNameStatement->setString(1, subreq->fileName());
      oracle::occi::ResultSet *rset = m_getCFByNameStatement->executeQuery();
      if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
        // Nothing found, fail the request
        m_getCFByNameStatement->closeResultSet(rset);
        subreq->setStatus(castor::stager::SUBREQUEST_FAILED);
        subreq->setErrorCode(ENOENT);
        cnvSvc()->updateRep(&ad, subreq, true);
        return 0;
      }
      // found something, let's validate it against the NameServer
      castor::stager::CastorFile* cf = dynamic_cast<castor::stager::CastorFile*>
        (cnvSvc()->getObjFromId((u_signed64)rset->getDouble(1), OBJ_CastorFile));
      m_getCFByNameStatement->closeResultSet(rset);
      char nspath[CA_MAXPATHLEN+1];

      if(Cns_getpath((char*)cf->nsHost().c_str(), cf->fileId(), nspath) != 0
         && serrno == ENOENT) {
        // indeed the file exists only in the stager db,
        // execute stageForcedRm (cf. ns synch performed in GC daemon)
        m_stageForcedRmStatement->setDouble(1, cf->fileId());
        m_stageForcedRmStatement->setString(2, cf->nsHost());
        unsigned int nb = m_stageForcedRmStatement->executeUpdate();
        if (0 == nb) {
          castor::exception::Internal ex;
          ex.getMessage()
            << "stageRm : No return code after PL/SQL call.";
          delete cf;
          throw ex;
        }
        delete cf;
        // The removal was successful
        return 1;
      }
      else {
        // the nameserver contains a file with this fileid, but
        // with a different name than the stager. Obviously the
        // file got renamed and the requested deletion cannot succeed;
        // anyway we update the stager catalogue with the new name
        cf->setLastKnownFileName(nspath);
        cnvSvc()->updateRep(&ad, cf, false);
        // and we fail the request
        subreq->setStatus(castor::stager::SUBREQUEST_FAILED);
        subreq->setErrorCode(ENOENT);
        subreq->setErrorMessage("The file got renamed by another user request");
        cnvSvc()->updateRep(&ad, subreq, true);
        delete cf;
        return 0;
      }
    }
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in stageRm."
      << std::endl << e.what();
    throw ex;
  }
}


//------------------------------------------------------------------------------
// setFileGCWeight
//------------------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::setFileGCWeight
(const u_signed64 fileId, const std::string nsHost,
 const u_signed64 svcClassId, const float weight)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_setFileGCWeightStatement) {
      m_setFileGCWeightStatement =
        createStatement(s_setFileGCWeightStatementString);
      m_setFileGCWeightStatement->registerOutParam
        (5, oracle::occi::OCCIINT);
      m_setFileGCWeightStatement->setAutoCommit(true);
    }
    // execute the statement and return the return code given by the procedure
    m_setFileGCWeightStatement->setDouble(1, fileId);
    m_setFileGCWeightStatement->setString(2, nsHost);
    m_setFileGCWeightStatement->setDouble(3, svcClassId);
    m_setFileGCWeightStatement->setFloat(4, weight);
    m_setFileGCWeightStatement->executeUpdate();
    return m_setFileGCWeightStatement->getInt(5);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in setFileGCWeight."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// validateNsSegments
//------------------------------------------------------------------------------
int validateNsSegments(struct Cns_segattrs *nsSegments,
                       int nbNsSegments, struct Cns_fileid *fileid,
                       const char* vid, int *nbTapeCopies)
  throw (castor::exception::Exception) {
  // list of temporarily and permanently invalid copies
  std::set<int> tmpInvalidCopies;
  std::set<int> permInvalidCopies;
  // number of unique copies
  std::set<int> nbCopies;
  // list of valid and the tape there are on
  // Note that the case of multi segmented tapes is not well supported
  // and only the last tape found will be kept.
  // Multi segmented files are anyway disappearing
  std::map<int,char*> validCopies;
  // Loop through the segments for this file
  for(short i = 0; i < nbNsSegments; i++) {
    // consistency checks for this segment
    int errmsg = 0;
    // Ammend the unique copies set
    nbCopies.insert(nsSegments[i].copyno);
    // Checks that the segment is associated to a tape
    if (nsSegments[i].vid == '\0') {
      // "Segment has no tape associated"
      permInvalidCopies.insert(nsSegments[i].copyno);
      errmsg = 19;
    }
    // Check segment status
    if (0 == errmsg && nsSegments[i].s_status != '-') {
      // "Segment is in bad status, could not be used for recall"
      permInvalidCopies.insert(nsSegments[i].copyno);
      errmsg = 20;
    }
    // Check Tape status
    if (0 == errmsg) {
      struct vmgr_tape_info_byte_u64 vmgrTapeInfo;
      if (-1 == vmgr_querytape_byte_u64(nsSegments[i].vid, nsSegments[i].side,
        &vmgrTapeInfo, 0)) {
        // "Error while contacting VMGR"
        tmpInvalidCopies.insert(nsSegments[i].copyno);
        errmsg = 21;
      } else {
        // when STANDBY tapes will be supported, they will
        // be let through, while we consider any of the following as permanent errors
        if(((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
           ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ||
           ((vmgrTapeInfo.status & EXPORTED) == EXPORTED)) {
          // "Tape disabled, could not be used for recall"
          tmpInvalidCopies.insert(nsSegments[i].copyno);
          errmsg = 22;
        }
      }
    }
    if (0 != errmsg) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("TPVID", nsSegments[i].vid),
        castor::dlf::Param("copyNb", nsSegments[i].copyno),
        castor::dlf::Param("fsec", nsSegments[i].fsec),
        castor::dlf::Param("segStatus", nsSegments[i].s_status)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                              DLF_BASE_ORACLELIB + errmsg,
                              4, params, fileid);
    } else {
      validCopies[nsSegments[i].copyno] = nsSegments[i].vid;
    }
  }
  // Reduce the list of valid copies by droping permanently invalid ones
  // Note that this can be dropped when multi segment files are gone
  for (std::set<int>::const_iterator it = permInvalidCopies.begin();
       it != permInvalidCopies.end();
       it++) {
    std::map<int,char*>::iterator it2 = validCopies.find(*it);
    if (it2 != validCopies.end()) {
      validCopies.erase(it2);
    }
  }

  // Update the number of missing copies if we have less copies than expected
  *nbTapeCopies -= nbCopies.size();
  
  // If we have more copies than expected we do nothing
  if (*nbTapeCopies < 0) {
    *nbTapeCopies = 0;
  } 

  // Reduce the list of valid copies by droping temporarily invalid ones
  // Note that this can be dropped when multi segment files are gone
  // Actually, the whole tmpInvalidCopies vector can be dropped !
  for (std::set<int>::const_iterator it = tmpInvalidCopies.begin();
       it != tmpInvalidCopies.end();
       it++) {
    std::map<int,char*>::iterator it2 = validCopies.find(*it);
    if (it2 != validCopies.end()) {
      validCopies.erase(it2);
    }
  }
  // deal with the case where no segment was found
  if (validCopies.begin() == validCopies.end()) {
    free(nsSegments);
    castor::exception::Exception e(ESTNOSEGFOUND);
    e.getMessage() << sstrerror(ESTNOSEGFOUND);
    throw e;
  }
  // Choose the best copy in those available, that is
  // the one matching the hint, if any, or the first one
  if (0 != vid ) {
    for (std::map<int,char*>::const_iterator it = validCopies.begin();
         it != validCopies.end();
         it++) {
      if (0 == strcmp(vid, it->second)) {
        return it->first;
      }
    }
  }
  return validCopies.begin()->first;
}

//------------------------------------------------------------------------------
// createRecallJobSegments (private)
//------------------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::createRecallJobSegments
(castor::stager::CastorFile* castorFile,
 unsigned long euid,
 unsigned long egid,
 castor::stager::SvcClass* svcClass,
 castor::stager::Tape* &tape,
 int *nbTapeCopies)
  throw (castor::exception::Exception) {

  // Check argument
  if (0 == castorFile) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "createRecallJobSegments "
                   << "called with null argument";
    throw e;
  }

  // Prepare access to name server
  char cns_error_buffer[512];
  *cns_error_buffer = 0;
  if (Cns_seterrbuf(cns_error_buffer, sizeof(cns_error_buffer)) != 0) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "createRecallJobSegments : Cns_seterrbuf failed";
    throw ex;
  }

  // Get segments for castorFile
  struct Cns_fileid fileid;
  fileid.fileid = castorFile->fileId();
  strncpy(fileid.server,
          castorFile->nsHost().c_str(),
          CA_MAXHOSTNAMELEN+1);
  struct Cns_segattrs *nsSegmentAttrs = 0;
  int nbNsSegments = 0;
  int rc = Cns_getsegattrs
    (0, &fileid, &nbNsSegments, &nsSegmentAttrs);
  if (-1 == rc) {
    castor::exception::Exception e(serrno);
    e.getMessage() << "createRecallJobSegments : "
                   << "Cns_getsegattrs failed";
    throw e;
  }

  if (nbNsSegments == 0) {
    // This file has no copy on tape. This is considered user error
    castor::exception::NoSegmentFound e;
    throw e;
  }

  // Validate all segments and get the copy to be used for recall;
  // this may throw an exception, in such a case nsSegmentAttrs is freed
  int useCopyNb = validateNsSegments(nsSegmentAttrs, nbNsSegments,
                                     &fileid, tape ? tape->vid().c_str() : 0,
                                     nbTapeCopies);

  // Log something in case we will trigger extra migrations after the recall
  if (*nbTapeCopies > 0) {
    // "Missing tape copies detected, this recall will trigger new migration(s)
    castor::dlf::Param params[] = {
      castor::dlf::Param("nbNewCopies", *nbTapeCopies)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                            DLF_BASE_ORACLELIB + 23,
                            1, params, &fileid);
  }

  // Store info on DB
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);

  // Create RecallJob
  castor::stager::RecallJob recallJob;
  recallJob.setCopyNb(useCopyNb);
  recallJob.setStatus(castor::stager::RECALLJOB_TOBERECALLED);
  recallJob.setMissingCopies(*nbTapeCopies);
  recallJob.setCastorFile(castorFile);
  castorFile->addTapeCopies(&recallJob);
  cnvSvc()->fillRep(&ad, castorFile,
                    castor::OBJ_RecallJob, false);

  // Go through Segments
  u_signed64 totalSize = 0;
  castor::stager::Tape *tp = 0;
  for (int i = 0; i < nbNsSegments; i++) {

    // Only deal with segments of the copy we choose
    if (nsSegmentAttrs[i].copyno != useCopyNb) continue;

    // Create Segment
    castor::stager::Segment *segment =
      new castor::stager::Segment();
    segment->setBlockId0(nsSegmentAttrs[i].blockid[0]);
    segment->setBlockId1(nsSegmentAttrs[i].blockid[1]);
    segment->setBlockId2(nsSegmentAttrs[i].blockid[2]);
    segment->setBlockId3(nsSegmentAttrs[i].blockid[3]);
    segment->setFseq(nsSegmentAttrs[i].fseq);
    segment->setOffset(totalSize);
    segment->setCreationTime(time(NULL));
    segment->setStatus(castor::stager::SEGMENT_UNPROCESSED);
    totalSize += nsSegmentAttrs[i].segsize;

    // Get tape for this segment
    tp = selectTape(nsSegmentAttrs[i].vid,
                   nsSegmentAttrs[i].side,
                   WRITE_DISABLE);

    // update priority
    std::vector<castor::stager::PriorityMap*> listPriority=selectPriority(euid,egid,-1);
    // never more than one for the same uig-gid
    u_signed64 priority=0;
    if (!listPriority.empty())
      priority=listPriority.at(0)->priority();
    segment->setPriority(priority);

    //  recaller policy retrieved
    //  in case it is given the tape status won't change into TAPE_PENDING
    std::string recallerPolicyStr = svcClass->recallerPolicy();
    switch (tp->status()) {
      case castor::stager::TAPE_UNUSED:
      case castor::stager::TAPE_FINISHED:
      case castor::stager::TAPE_FAILED:
      case castor::stager::TAPE_UNKNOWN:
        if (recallerPolicyStr.empty())
          tp->setStatus(castor::stager::TAPE_PENDING);
        else
          tp->setStatus(castor::stager::TAPE_WAITPOLICY);
        cnvSvc()->updateRep(&ad, tp, false);
        break;
      default:
        break;
    }

    // In any case with or without recaller policy the following operation are executed
    // Link Tape with Segment
    segment->setTape(tp);
    tp->addSegments(segment);

    // Link Segment with RecallJob
    segment->setCopy(&recallJob);
    recallJob.addSegments(segment);
  }

  // Set the tape VID and status of the tape to be returned to the calling
  // function
  if (0 == tape) {
    tape = new castor::stager::Tape();
  }
  tape->setVid(tp->vid());
  tape->setStatus(tp->status());

  // Create Segments in DataBase
  cnvSvc()->fillRep(&ad, &recallJob, castor::OBJ_Segment, false);

  // Fill Segment to Tape link
  for (unsigned i = 0; i < recallJob.segments().size(); i++) {
    castor::stager::Segment* seg = recallJob.segments()[i];
    cnvSvc()->fillRep(&ad, seg, castor::OBJ_Tape, false);
  }

  // Cleanup
  for (unsigned i = 0; i < recallJob.segments().size(); i++)
    delete recallJob.segments()[i]->tape();
  if (nsSegmentAttrs != NULL) free(nsSegmentAttrs);

  return 0;
}

//------------------------------------------------------------------------------
// selectPriority
//------------------------------------------------------------------------------
std::vector<castor::stager::PriorityMap*>
castor::db::ora::OraStagerSvc::selectPriority
(int euid, int egid, int priority)
  throw (castor::exception::Exception) {
  try {

    // Check whether the statements are ok
    if (0 == m_selectPriorityStatement) {
      m_selectPriorityStatement =
	createStatement(s_selectPriorityStatementString);
      m_selectPriorityStatement->registerOutParam
        (4, oracle::occi::OCCICURSOR);
    }

    // Execute the statement
    m_selectPriorityStatement->setInt(1, euid);
    m_selectPriorityStatement->setInt(2, egid);
    m_selectPriorityStatement->setInt(3, priority);

    m_selectPriorityStatement->executeUpdate();

    // Return value
    std::vector<castor::stager::PriorityMap*> priorityList;
    oracle::occi::ResultSet *rs = m_selectPriorityStatement->getCursor(4);

    // Run through the cursor
    oracle::occi::ResultSet::Status status = rs->next();
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::stager::PriorityMap* item = new castor::stager::PriorityMap();
      item->setEuid((u_signed64)rs->getDouble(1));
      item->setEgid((u_signed64)rs->getDouble(2));
      item->setPriority((u_signed64)rs->getDouble(3));
      priorityList.push_back(item);
      status = rs->next();
    }
    m_selectPriorityStatement->closeResultSet(rs);
    return priorityList;

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in selectPriority(): uid ("
      << euid << ")" << " gid (" << egid << ")"
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// enterPriority
//------------------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::enterPriority
(u_signed64 euid, u_signed64 egid, u_signed64 priority)
  throw (castor::exception::Exception) {
  try {

    // Check whether the statements are ok
    if (0 == m_enterPriorityStatement) {
      m_enterPriorityStatement =
        createStatement(s_enterPriorityStatementString);
      m_enterPriorityStatement->setAutoCommit(true);
    }

    // Execute the statement
    m_enterPriorityStatement->setDouble(1, euid);
    m_enterPriorityStatement->setDouble(2, egid);
    m_enterPriorityStatement->setDouble(3, priority);
    m_enterPriorityStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Exception ex(e.getErrorCode());
    ex.getMessage()
      << "Invalid input: uid ("
      << euid << ")" << " gid (" << egid << ")"
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// deletePriority
//------------------------------------------------------------------------------
void  castor::db::ora::OraStagerSvc::deletePriority(int euid, int egid)
  throw (castor::exception::Exception) {
  try {

    // Check whether the statements are ok
    if (0 == m_deletePriorityStatement) {
      m_deletePriorityStatement =
        createStatement(s_deletePriorityStatementString);
      m_deletePriorityStatement->setAutoCommit(true);
    }

    // Execute the statement
    m_deletePriorityStatement->setInt(1, euid);
    m_deletePriorityStatement->setInt(2, egid);
    m_deletePriorityStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in deletePriority(): uid ("
      << euid << ")" << " gid (" << egid << ")"
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getConfigOption
//------------------------------------------------------------------------------
std::string castor::db::ora::OraStagerSvc::getConfigOption(std::string confClass,
                                                           std::string confKey,
                                                           std::string defaultValue)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_getConfigOptionStatement) {
      m_getConfigOptionStatement =
        createStatement(s_getConfigOptionStatementString);
    }
    // Execute the statement
    m_getConfigOptionStatement->setString(1, confClass);
    m_getConfigOptionStatement->setString(2, confKey);
    m_getConfigOptionStatement->setString(3, defaultValue);
    std::string res;
    oracle::occi::ResultSet *rs = m_getConfigOptionStatement->executeQuery();
    if (oracle::occi::ResultSet::DATA_AVAILABLE == rs->next()) {
      res = rs->getString(1);
    }
    m_getConfigOptionStatement->closeResultSet(rs);
    return res;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getConfigOption(): confClass = "
      << confClass << ", confKey = " << confKey
      << std::endl << e.what();
    throw ex;
  }
}
