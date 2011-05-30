/******************************************************************************
 *                      OraRHSvc.cpp
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
 * @(#)$RCSfile: OraRHSvc.cpp,v $ $Revision: 1.18 $ $Release$ $Date: 2009/04/14 11:30:00 $ $Author: itglp $
 *
 * Implementation of the IRHSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/db/ora/OraRHSvc.hpp"
#include "castor/Constants.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/PermissionDenied.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/OutOfMemory.hpp"
#include "castor/bwlist/BWUser.hpp"
#include "castor/bwlist/RequestType.hpp"
#include "castor/bwlist/Privilege.hpp"
#include "castor/bwlist/ChangePrivilege.hpp"
#include "castor/bwlist/RequestType.hpp"
#include "castor/bwlist/ListPrivileges.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/StartRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/GetUpdateDone.hpp"
#include "castor/stager/GetUpdateFailed.hpp"
#include "castor/stager/PutFailed.hpp"
#include "castor/stager/FirstByteWritten.hpp"
#include "castor/stager/Disk2DiskCopyDoneRequest.hpp"
#include "castor/stager/Disk2DiskCopyStartRequest.hpp"
#include "castor/query/VersionQuery.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/query/DiskPoolQuery.hpp"
#include "castor/stager/MoverCloseRequest.hpp"
#include "castor/stager/Files2Delete.hpp"
#include "castor/stager/StageAbortRequest.hpp"
#include "castor/stager/StagePutDoneRequest.hpp"
#include "castor/stager/StageRepackRequest.hpp"
#include "castor/stager/SetFileGCWeight.hpp"
#include "castor/stager/NsFileId.hpp"
#include "castor/stager/GCFileList.hpp"
#include "castor/stager/NsFilesDeleted.hpp"
#include "castor/stager/StgFilesDeleted.hpp"
#include "castor/stager/GCFile.hpp"
#include "castor/stager/QueryParameter.hpp"
#include "castor/rh/Client.hpp"
#include <string>
#include <vector>
#include "occi.h"


//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraRHSvc>* s_factoryOraRHSvc =
  new castor::SvcFactory<castor::db::ora::OraRHSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL statement for storeSimpleRequest
const std::string castor::db::ora::OraRHSvc::s_storeSimpleRequestStatementString =
  "BEGIN insertSimpleRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15); END;";

/// SQL statement for storeFileRequest
const std::string castor::db::ora::OraRHSvc::s_storeFileRequestStatementString =
  "BEGIN insertFileRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22); END;";

/// SQL statement for storeStartRequest
const std::string castor::db::ora::OraRHSvc::s_storeStartRequestStatementString =
  "BEGIN insertStartRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17); END;";

/// SQL statement for storeD2dRequest
const std::string castor::db::ora::OraRHSvc::s_storeD2dRequestStatementString =
  "BEGIN insertD2dRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19); END;";

/// SQL statement for storeVersionQueryRequest
const std::string castor::db::ora::OraRHSvc::s_storeVersionQueryStatementString =
  "BEGIN insertVersionQueryRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12); END;";

/// SQL statement for storeStageFileQueryRequest
const std::string castor::db::ora::OraRHSvc::s_storeStageFileQueryRequestStatementString =
  "BEGIN insertStageFileQueryRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15); END;";

/// SQL statement for storeDiskPoolQueryRequest
const std::string castor::db::ora::OraRHSvc::s_storeDiskPoolQueryStatementString =
  "BEGIN insertDiskPoolQueryRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14); END;";

/// SQL statement for storeMoverCloseRequest
const std::string castor::db::ora::OraRHSvc::s_storeMoverCloseRequestStatementString =
  "BEGIN insertMoverCloseRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19); END;";

/// SQL statement for storeFiles2DeleteRequest
const std::string castor::db::ora::OraRHSvc::s_storeFiles2DeleteStatementString =
  "BEGIN insertFiles2DeleteRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13); END;";

/// SQL statement for storeListPrivilegesRequest
const std::string castor::db::ora::OraRHSvc::s_storeListPrivilegesStatementString =
  "BEGIN insertListPrivilegesRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15); END;";

/// SQL statement for storeStageAbortRequest
const std::string castor::db::ora::OraRHSvc::s_storeStageAbortRequestStatementString =
  "BEGIN insertStageAbortRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15); END;";

/// SQL statement for storeChangePrivilegeRequest
const std::string castor::db::ora::OraRHSvc::s_storeChangePrivilegeStatementString =
  "BEGIN insertChangePrivilegeRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16); END;";

/// SQL statement for storeGCRequest
const std::string castor::db::ora::OraRHSvc::s_storeGCStatementString =
  "BEGIN insertGCRequest(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14); END;";

/// SQL statement for addPrivilege
const std::string castor::db::ora::OraRHSvc::s_addPrivilegeStatementString =
  "BEGIN castorbw.addPrivilege(:1, :2, :3, :4); END;";

/// SQL statement for removePrivilege
const std::string castor::db::ora::OraRHSvc::s_removePrivilegeStatementString =
  "BEGIN castorbw.removePrivilege(:1, :2, :3, :4); END;";

/// SQL statement for listPrivileges
const std::string castor::db::ora::OraRHSvc::s_listPrivilegesStatementString =
  "BEGIN castorbw.listPrivileges(:1, :2, :3, :4, :5); END;";

//------------------------------------------------------------------------------
// OraRHSvc
//------------------------------------------------------------------------------
castor::db::ora::OraRHSvc::OraRHSvc(const std::string name) :
  OraCommonSvc(name),
  m_storeSimpleRequestStatement(0),
  m_storeFileRequestStatement(0),
  m_storeStartRequestStatement(0),
  m_storeD2dRequestStatement(0),
  m_storeVersionQueryStatement(0),
  m_storeStageFileQueryRequestStatement(0),
  m_storeDiskPoolQueryStatement(0),
  m_storeMoverCloseRequestStatement(0),
  m_storeFiles2DeleteStatement(0),
  m_storeListPrivilegesStatement(0),
  m_storeStageAbortRequestStatement(0),
  m_storeChangePrivilegeStatement(0),
  m_storeGCStatement(0),
  m_addPrivilegeStatement(0),
  m_removePrivilegeStatement(0),
  m_listPrivilegesStatement(0) {
}

//------------------------------------------------------------------------------
// ~OraRHSvc
//------------------------------------------------------------------------------
castor::db::ora::OraRHSvc::~OraRHSvc() throw() {
  reset();
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraRHSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraRHSvc::ID() {
  return castor::SVC_ORARHSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::reset() throw() {
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if (m_storeSimpleRequestStatement) deleteStatement(m_storeSimpleRequestStatement);
    if (m_storeFileRequestStatement) deleteStatement(m_storeFileRequestStatement);
    if (m_storeStartRequestStatement) deleteStatement(m_storeStartRequestStatement);
    if (m_storeD2dRequestStatement) deleteStatement(m_storeD2dRequestStatement);
    if (m_storeVersionQueryStatement) deleteStatement(m_storeVersionQueryStatement);
    if (m_storeStageFileQueryRequestStatement) deleteStatement(m_storeStageFileQueryRequestStatement);
    if (m_storeDiskPoolQueryStatement) deleteStatement(m_storeDiskPoolQueryStatement);
    if (m_storeMoverCloseRequestStatement) deleteStatement(m_storeMoverCloseRequestStatement);
    if (m_storeFiles2DeleteStatement) deleteStatement(m_storeFiles2DeleteStatement);
    if (m_storeListPrivilegesStatement) deleteStatement(m_storeListPrivilegesStatement);
    if (m_storeStageAbortRequestStatement) deleteStatement(m_storeStageAbortRequestStatement);
    if (m_storeChangePrivilegeStatement) deleteStatement(m_storeChangePrivilegeStatement);
    if (m_storeGCStatement) deleteStatement(m_storeGCStatement);
    if (m_addPrivilegeStatement) deleteStatement(m_addPrivilegeStatement);
    if (m_removePrivilegeStatement) deleteStatement(m_removePrivilegeStatement);
    if (m_listPrivilegesStatement) deleteStatement(m_listPrivilegesStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_storeSimpleRequestStatement = 0;
  m_storeFileRequestStatement = 0;
  m_storeStartRequestStatement = 0;
  m_storeD2dRequestStatement = 0;
  m_storeVersionQueryStatement = 0;
  m_storeStageFileQueryRequestStatement = 0;
  m_storeDiskPoolQueryStatement = 0;
  m_storeMoverCloseRequestStatement = 0;
  m_storeFiles2DeleteStatement = 0;
  m_storeListPrivilegesStatement = 0;
  m_storeStageAbortRequestStatement = 0;
  m_storeChangePrivilegeStatement = 0;
  m_storeGCStatement = 0;
  m_addPrivilegeStatement = 0;
  m_removePrivilegeStatement = 0;
  m_listPrivilegesStatement = 0;
}

//------------------------------------------------------------------------------
// storeRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeRequest
(castor::stager::Request* req)
  throw (castor::exception::Exception) {
  try {
    // get the corresponding client object
    castor::rh::Client* client = dynamic_cast<castor::rh::Client*>(req->client());
    if (0 == client) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to cast client object to castor::rh::Client" << std::endl;
      throw ex;
    }
    // Depending on the request, call the appropriate store method
    switch (req->type()) {
    case castor::OBJ_GetUpdateDone:
      {
        // get the precise request
        const castor::stager::GetUpdateDone* preq = dynamic_cast<const castor::stager::GetUpdateDone*>(req);
        // and store it
        storeSimpleRequest(req, client, preq->subReqId(), preq->fileId(), preq->nsHost());
      }
      break;
    case castor::OBJ_GetUpdateFailed:
      {
        // get the precise request
        const castor::stager::GetUpdateFailed* preq = dynamic_cast<const castor::stager::GetUpdateFailed*>(req);
        // and store it
        storeSimpleRequest(req, client, preq->subReqId(), preq->fileId(), preq->nsHost());
      }
      break;
    case castor::OBJ_PutFailed:
      {
        // get the precise request
        const castor::stager::PutFailed* preq = dynamic_cast<const castor::stager::PutFailed*>(req);
        // and store it
        storeSimpleRequest(req, client, preq->subReqId(), preq->fileId(), preq->nsHost());
      }
      break;
    case castor::OBJ_FirstByteWritten:
      {
        // get the precise request
        const castor::stager::FirstByteWritten* preq = dynamic_cast<const castor::stager::FirstByteWritten*>(req);
        // and store it
        storeSimpleRequest(req, client, preq->subReqId(), preq->fileId(), preq->nsHost());
      }
      break;
    case castor::OBJ_StageGetRequest:
    case castor::OBJ_StagePrepareToGetRequest:
    case castor::OBJ_StagePutRequest:
    case castor::OBJ_StagePrepareToPutRequest:
    case castor::OBJ_StageUpdateRequest:
    case castor::OBJ_StagePrepareToUpdateRequest:
    case castor::OBJ_StageRmRequest:
      {
        // get the FileRequest
        castor::stager::FileRequest* freq = dynamic_cast<castor::stager::FileRequest*>(req);
        // and store it
        storeFileRequest(freq, client);
      }
      break;
    case castor::OBJ_StagePutDoneRequest:
      {
        // get the StagePutDoneRequest
        castor::stager::StagePutDoneRequest* freq = dynamic_cast<castor::stager::StagePutDoneRequest*>(req);
        // and store it
        storeFileRequest(freq, client, freq->parentUuid());
      }
      break;
    case castor::OBJ_StageRepackRequest:
      {
        // get the StagerRepackRequest
        castor::stager::StageRepackRequest* freq = dynamic_cast<castor::stager::StageRepackRequest*>(req);
        // and store it
        storeFileRequest(freq, client, freq->repackVid());
      }
      break;
    case castor::OBJ_SetFileGCWeight:
      {
        // get the SetFileGCWeight
        castor::stager::SetFileGCWeight* freq = dynamic_cast<castor::stager::SetFileGCWeight*>(req);
        // and store it
        storeFileRequest(freq, client, "", freq->weight());
      }
      break;
    case castor::OBJ_PutStartRequest:
    case castor::OBJ_GetUpdateStartRequest:
      {
        // get the StartRequest
        castor::stager::StartRequest* sreq = dynamic_cast<castor::stager::StartRequest*>(req);
        // and store it
        storeStartRequest(sreq, client);        
      }
      break;
    case castor::OBJ_Disk2DiskCopyDoneRequest:
      {
        // get the Disk2DiskCopyDoneRequest
        castor::stager::Disk2DiskCopyDoneRequest* dreq = dynamic_cast<castor::stager::Disk2DiskCopyDoneRequest*>(req);
        // and store it
        storeD2dRequest(dreq, client, dreq->diskCopyId(), dreq->sourceDiskCopyId(), "", "",
                        dreq->replicaFileSize(), dreq->fileId(), dreq->nsHost());
      }
      break;
    case castor::OBJ_Disk2DiskCopyStartRequest:
      {
        // get the Disk2DiskCopyStartRequest
        castor::stager::Disk2DiskCopyStartRequest* dreq = dynamic_cast<castor::stager::Disk2DiskCopyStartRequest*>(req);
        // and store it
        storeD2dRequest(dreq, client, dreq->diskCopyId(), dreq->sourceDiskCopyId(), dreq->diskServer(),
                        dreq->mountPoint(), 0, dreq->fileId(), dreq->nsHost());
      }
      break;
    case castor::OBJ_VersionQuery:
      {
        // get the VersionQuery
        castor::query::VersionQuery* dreq = dynamic_cast<castor::query::VersionQuery*>(req);
        // and store it
        storeVersionQueryRequest(dreq, client);
      }
      break;
    case castor::OBJ_StageFileQueryRequest:
      {
        // get the StageFileQueryRequest
        castor::stager::StageFileQueryRequest* dreq = dynamic_cast<castor::stager::StageFileQueryRequest*>(req);
        // and store it
        storeStageFileQueryRequest(dreq, client);
      }
      break;
    case castor::OBJ_DiskPoolQuery:
      {
        // get the DiskPoolQuery
        castor::query::DiskPoolQuery* dreq = dynamic_cast<castor::query::DiskPoolQuery*>(req);
        // and store it
        storeDiskPoolQueryRequest(dreq, client);
      }
      break;
    case castor::OBJ_MoverCloseRequest:
      {
        // get the MoverCloseRequest
        castor::stager::MoverCloseRequest* dreq = dynamic_cast<castor::stager::MoverCloseRequest*>(req);
        // and store it
        storeMoverCloseRequest(dreq, client);
      }
      break;
    case castor::OBJ_Files2Delete:
      {
        // get the Files2Delete
        castor::stager::Files2Delete* dreq = dynamic_cast<castor::stager::Files2Delete*>(req);
        // and store it
        storeFiles2DeleteRequest(dreq, client);
      }
      break;
    case castor::OBJ_ListPrivileges:
      {
        // get the ListPrivileges
        castor::bwlist::ListPrivileges* dreq = dynamic_cast<castor::bwlist::ListPrivileges*>(req);
        // and store it
        storeListPrivilegesRequest(dreq, client);
      }
      break;
    case castor::OBJ_StageAbortRequest:
      {
        // get the StageAbortRequest
        castor::stager::StageAbortRequest* dreq = dynamic_cast<castor::stager::StageAbortRequest*>(req);
        // and store it
        storeStageAbortRequest(dreq, client);
      }
      break;
    case castor::OBJ_ChangePrivilege:
      {
        // get the ChangePrivilege
        castor::bwlist::ChangePrivilege* dreq = dynamic_cast<castor::bwlist::ChangePrivilege*>(req);
        // and store it
        storeChangePrivilegeRequest(dreq, client);
      }
      break;
    case castor::OBJ_FilesDeleted:
    case castor::OBJ_FilesDeletionFailed:
      {
        // get the GC
        castor::stager::GCFileList* gcreq = dynamic_cast<castor::stager::GCFileList*>(req);
        // and store it
        storeGCRequest(gcreq, client);
      }
      break;
    case castor::OBJ_NsFilesDeleted:
      {
        // get the GC
        castor::stager::NsFilesDeleted* gcreq = dynamic_cast<castor::stager::NsFilesDeleted*>(req);
        // and store it
        storeGCRequest(gcreq, client, gcreq->nsHost());
      }
      break;
    case castor::OBJ_StgFilesDeleted:
      {
        // get the GC
        castor::stager::StgFilesDeleted* gcreq = dynamic_cast<castor::stager::StgFilesDeleted*>(req);
        // and store it
        storeGCRequest(gcreq, client, gcreq->nsHost());
      }
      break;
    default:
      castor::exception::Internal ex;
      ex.getMessage() << "Unsupported request type : " << req->type() << std::endl;
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    // catch SQL exceptions and deal with them
    handleException(e);
    // Deal more specifically with application specific errors
    if (e.getErrorCode() == 20121) {
      // permission denied
      castor::exception::PermissionDenied ex;
      throw ex;
    } else if (e.getErrorCode() == 20113) {
      // invalid service class
      castor::exception::InvalidArgument ex;
      // extract the original message sent by the PL/SQL code
      // from the surrounding oracle additions
      std::string msg = e.what();
      ex.getMessage() << msg.substr(11,msg.find('\n')-11) << "\n";
      throw ex;
    } else {
      // unexpected exception
      castor::exception::Internal ex;
      std::string msg = e.what();
      ex.getMessage() << "unexpected ORACLE exception caught while inserting request : "
                      << e.what();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// storeSimpleRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeSimpleRequest (const castor::stager::Request* req,
                                                    const castor::rh::Client* client,
                                                    const u_signed64 subReqId,
                                                    const u_signed64 fileId,
                                                    const std::string nsHost)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeSimpleRequestStatement) {
    m_storeSimpleRequestStatement =
      createStatement(s_storeSimpleRequestStatementString);
    m_storeSimpleRequestStatement->setAutoCommit(true);
  }
  // execute the statement and see whether we found something
  m_storeSimpleRequestStatement->setString(1, req->machine());
  m_storeSimpleRequestStatement->setInt(2, req->euid());
  m_storeSimpleRequestStatement->setInt(3, req->egid());
  m_storeSimpleRequestStatement->setInt(4, req->pid());
  m_storeSimpleRequestStatement->setString(5, req->userName());
  m_storeSimpleRequestStatement->setString(6, req->svcClassName());
  m_storeSimpleRequestStatement->setString(7, req->reqId());
  m_storeSimpleRequestStatement->setInt(8, req->type());
  m_storeSimpleRequestStatement->setInt(9, client->ipAddress());
  m_storeSimpleRequestStatement->setInt(10, client->port());
  m_storeSimpleRequestStatement->setInt(11, client->version());
  m_storeSimpleRequestStatement->setInt(12, client->secure());
  m_storeSimpleRequestStatement->setDouble(13, subReqId);
  m_storeSimpleRequestStatement->setDouble(14, fileId);
  m_storeSimpleRequestStatement->setString(15, nsHost);
  unsigned int rc = m_storeSimpleRequestStatement->executeUpdate();
  if (0 == rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to store simple request";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// storeFileRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeFileRequest (castor::stager::FileRequest* req,
                                                  const castor::rh::Client* client,
                                                  const std::string freeStrParam,
                                                  const float freeNumParam)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  try {
    // Check whether the statement is ok
    if (0 == m_storeFileRequestStatement) {
      m_storeFileRequestStatement =
        createStatement(s_storeFileRequestStatementString);
      m_storeFileRequestStatement->setAutoCommit(true);
    }
  } catch (oracle::occi::SQLException e) {
    // Deal with exception (in particular see whether we should reconnect)
    handleException(e);
    // Send generic errors
    castor::exception::Internal ex;
    ex.getMessage() << "Error caught in storeFileRequest."
                    << std::endl << e.what();
    throw ex;
  }
  // easier access to the subRequests list
  const std::vector<castor::stager::SubRequest*> &subRequests = req->subRequests();
  // Allocate ORACLE buffers for string arguments lengths (fileName and protocol)
  ub2 *lensFileName = (ub2*) calloc(subRequests.size(), sizeof(ub2));
  if (0 == lensFileName) {
    castor::exception::OutOfMemory e; throw e;
  };
  ub2 *lensProtocol = (ub2*) calloc(subRequests.size(), sizeof(ub2));
  if (0 == lensProtocol) {
    free(lensFileName);
    castor::exception::OutOfMemory e; throw e;
  };
  // Fill the buffers for string arguments lengths, and compute the max lengths
  // Note the non zero maximum length so that buffers are not empty. This would not
  // be to the taste of ORACLE....
  unsigned int maxFileNameLen = 1;
  unsigned int maxProtocolLen = 1;
  unsigned int counter = 0;
  for (std::vector<castor::stager::SubRequest*>::const_iterator it = subRequests.begin();
       it != subRequests.end();
       it++) {
    lensFileName[counter] = (*it)->fileName().size();
    lensProtocol[counter] = (*it)->protocol().size();
    if ((*it)->fileName().size() > maxFileNameLen) maxFileNameLen = (*it)->fileName().size();
    if ((*it)->protocol().size() > maxProtocolLen) maxProtocolLen = (*it)->protocol().size();
    counter++;
  }
  // Allocate ORACLE buffers for the string parameters
  unsigned int fileNameCellSize = maxFileNameLen * sizeof(char);
  char *bufferFileName = (char*) calloc(subRequests.size(), fileNameCellSize);
  if (0 == bufferFileName) {
    free(lensFileName);free(lensProtocol);
    castor::exception::OutOfMemory e;
    throw e;
  };
  unsigned int protocolCellSize = maxProtocolLen * sizeof(char);
  char *bufferProtocol = (char*) calloc(subRequests.size(), protocolCellSize);
  if (0 == bufferProtocol) {
    free(lensFileName);free(lensProtocol);free(bufferFileName);
    castor::exception::OutOfMemory e;
    throw e;
  };
  // Allocate ORACLE buffer for the number parameters (xsize, flags and
  // modeBits) and for their lengths
  ub2 *lensXsize = (ub2*) calloc(subRequests.size(), sizeof(ub2));
  if (0 == lensXsize) {
    free(lensFileName);free(lensProtocol);
    free(bufferFileName);free(bufferProtocol);
    castor::exception::OutOfMemory e; throw e;
  };
  ub2 *lensFlags = (ub2*) calloc(subRequests.size(), sizeof(ub2));
  if (0 == lensFlags) {
    free(lensFileName);free(lensProtocol);free(bufferFileName);
    free(bufferProtocol);free(lensXsize);
    castor::exception::OutOfMemory e; throw e;
  };
  ub2 *lensModeBits = (ub2*) calloc(subRequests.size(), sizeof(ub2));
  if (0 == lensModeBits) {
    free(lensFileName);free(lensProtocol);free(bufferFileName);
    free(bufferProtocol);free(lensXsize);free(lensFlags);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferXsize)[21] =
    (unsigned char(*)[21]) calloc(subRequests.size(), 21*sizeof(unsigned char));
  if (0 == bufferXsize) {
    free(lensFileName);free(lensProtocol);free(bufferFileName);
    free(bufferProtocol);free(lensXsize);free(lensFlags);free(lensModeBits);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferFlags)[21] =
    (unsigned char(*)[21]) calloc(subRequests.size(), 21*sizeof(unsigned char));
  if (0 == bufferFlags) {
    free(lensFileName);free(lensProtocol);free(bufferFileName);
    free(bufferProtocol);free(lensXsize);free(lensFlags);
    free(lensModeBits);free(bufferXsize);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferModeBits)[21] =
    (unsigned char(*)[21]) calloc(subRequests.size(), 21*sizeof(unsigned char));
  if (0 == bufferModeBits) {
    free(lensFileName);free(lensProtocol);free(bufferFileName);
    free(bufferProtocol);free(lensXsize);free(lensFlags);
    free(lensModeBits);free(bufferXsize);free(bufferFlags);
    castor::exception::OutOfMemory e; throw e;
  };
  // Fill all buffers
  counter = 0;
  for (std::vector<castor::stager::SubRequest*>::const_iterator it = subRequests.begin();
       it != subRequests.end();
       it++) {
    strncpy(bufferFileName+(counter*fileNameCellSize), (*it)->fileName().c_str(), lensFileName[counter]);
    strncpy(bufferProtocol+(counter*protocolCellSize), (*it)->protocol().c_str(), lensProtocol[counter]);
    oracle::occi::Number oraXsize = (double)(*it)->xsize();
    oracle::occi::Bytes bxsize = oraXsize.toBytes();
    bxsize.getBytes(bufferXsize[counter],bxsize.length());
    lensXsize[counter] = bxsize.length();
    oracle::occi::Number oraFlags = (*it)->flags();
    oracle::occi::Bytes bflags = oraFlags.toBytes();
    bflags.getBytes(bufferFlags[counter],bflags.length());
    lensFlags[counter] = bflags.length();
    oracle::occi::Number oraModeBits = (*it)->modeBits();
    oracle::occi::Bytes bmodeBits = oraModeBits.toBytes();
    bmodeBits.getBytes(bufferModeBits[counter],bmodeBits.length());
    lensModeBits[counter] = bmodeBits.length();
    counter++;
  }
  try {
    // link all input parameters to the statement
    m_storeFileRequestStatement->setString(1, req->userTag());
    m_storeFileRequestStatement->setString(2, req->machine());
    m_storeFileRequestStatement->setInt(3, req->euid());
    m_storeFileRequestStatement->setInt(4, req->egid());
    m_storeFileRequestStatement->setInt(5, req->pid());
    m_storeFileRequestStatement->setInt(6, req->mask());
    m_storeFileRequestStatement->setString(7, req->userName());
    m_storeFileRequestStatement->setInt(8, req->flags());
    m_storeFileRequestStatement->setString(9, req->svcClassName());
    m_storeFileRequestStatement->setString(10, req->reqId());
    m_storeFileRequestStatement->setInt(11, req->type());
    m_storeFileRequestStatement->setInt(12, client->ipAddress());
    m_storeFileRequestStatement->setInt(13, client->port());
    m_storeFileRequestStatement->setInt(14, client->version());
    m_storeFileRequestStatement->setInt(15, client->secure());
    m_storeFileRequestStatement->setString(16, freeStrParam);
    m_storeFileRequestStatement->setDouble(17, freeNumParam);
    ub4 nbItems = subRequests.size();
    m_storeFileRequestStatement->setDataBufferArray
      (18, bufferFileName, oracle::occi::OCCI_SQLT_CHR,
       nbItems, &nbItems, maxFileNameLen, lensFileName);
    m_storeFileRequestStatement->setDataBufferArray
      (19, bufferProtocol, oracle::occi::OCCI_SQLT_CHR,
       nbItems, &nbItems, maxProtocolLen, lensProtocol);
    m_storeFileRequestStatement->setDataBufferArray
      (20, bufferXsize, oracle::occi::OCCI_SQLT_NUM,
       nbItems, &nbItems, 21, lensXsize);
    m_storeFileRequestStatement->setDataBufferArray
      (21, bufferFlags, oracle::occi::OCCI_SQLT_NUM,
       nbItems, &nbItems, 21, lensFlags);
    m_storeFileRequestStatement->setDataBufferArray
      (22, bufferModeBits, oracle::occi::OCCI_SQLT_NUM,
       nbItems, &nbItems, 21, lensModeBits);
    // execute the statement
    unsigned int rc = m_storeFileRequestStatement->executeUpdate();
    //cleanup memory 
    free(lensFileName);free(lensProtocol);free(bufferFileName);
    free(bufferProtocol);free(lensXsize);free(lensFlags);
    free(lensModeBits);free(bufferXsize);free(bufferFlags);
    free(bufferModeBits);
    // check whether the statement was successful
    if (0 == rc) {
      // throw exception
      castor::exception::Internal ex;
      ex.getMessage() << "storeFileRequest : unable to store file request";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    // cleanup memory 
    free(lensFileName);free(lensProtocol);free(bufferFileName);
    free(bufferProtocol);free(lensXsize);free(lensFlags);
    free(lensModeBits);free(bufferXsize);free(bufferFlags);
    free(bufferModeBits);
    // rethrow, the handling is done one level up
    throw e;
  }
}

//------------------------------------------------------------------------------
// storeStartRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeStartRequest (castor::stager::StartRequest* req,
                                                   const castor::rh::Client* client)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeStartRequestStatement) {
    m_storeStartRequestStatement =
      createStatement(s_storeStartRequestStatementString);
    m_storeStartRequestStatement->setAutoCommit(true);
  }
  // execute the statement and see whether we found something
  m_storeStartRequestStatement->setString(1, req->machine());
  m_storeStartRequestStatement->setInt(2, req->euid());
  m_storeStartRequestStatement->setInt(3, req->egid());
  m_storeStartRequestStatement->setInt(4, req->pid());
  m_storeStartRequestStatement->setString(5, req->userName());
  m_storeStartRequestStatement->setString(6, req->svcClassName());
  m_storeStartRequestStatement->setString(7, req->reqId());
  m_storeStartRequestStatement->setInt(8, req->type());
  m_storeStartRequestStatement->setInt(9, client->ipAddress());
  m_storeStartRequestStatement->setInt(10, client->port());
  m_storeStartRequestStatement->setInt(11, client->version());
  m_storeStartRequestStatement->setInt(12, client->secure());
  m_storeStartRequestStatement->setDouble(13, req->subreqId());
  m_storeStartRequestStatement->setString(14, req->diskServer());
  m_storeStartRequestStatement->setString(15, req->fileSystem());
  m_storeStartRequestStatement->setDouble(16, req->fileId());
  m_storeStartRequestStatement->setString(17, req->nsHost());
  unsigned int rc = m_storeStartRequestStatement->executeUpdate();
  if (0 == rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to store start request";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// storeD2dRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeD2dRequest (castor::stager::Request* req,
                                                 const castor::rh::Client* client,
                                                 const u_signed64 diskCopyId,
                                                 const u_signed64 srcDiskCopyId,
                                                 const std::string diskServer,
                                                 const std::string mountPoint,
                                                 const u_signed64 size,
                                                 const u_signed64 fileId,
                                                 const std::string nsHost)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeD2dRequestStatement) {
    m_storeD2dRequestStatement =
      createStatement(s_storeD2dRequestStatementString);
    m_storeD2dRequestStatement->setAutoCommit(true);
  }
  // execute the statement and see whether we found something
  m_storeD2dRequestStatement->setString(1, req->machine());
  m_storeD2dRequestStatement->setInt(2, req->euid());
  m_storeD2dRequestStatement->setInt(3, req->egid());
  m_storeD2dRequestStatement->setInt(4, req->pid());
  m_storeD2dRequestStatement->setString(5, req->userName());
  m_storeD2dRequestStatement->setString(6, req->svcClassName());
  m_storeD2dRequestStatement->setString(7, req->reqId());
  m_storeD2dRequestStatement->setInt(8, req->type());
  m_storeD2dRequestStatement->setInt(9, client->ipAddress());
  m_storeD2dRequestStatement->setInt(10, client->port());
  m_storeD2dRequestStatement->setInt(11, client->version());
  m_storeD2dRequestStatement->setInt(12, client->secure());
  m_storeD2dRequestStatement->setDouble(13, diskCopyId);
  m_storeD2dRequestStatement->setDouble(14, srcDiskCopyId);
  m_storeD2dRequestStatement->setDouble(15, size);
  m_storeD2dRequestStatement->setString(16, diskServer);
  m_storeD2dRequestStatement->setString(17, mountPoint);
  m_storeD2dRequestStatement->setDouble(18, fileId);
  m_storeD2dRequestStatement->setString(19, nsHost);
  unsigned int rc = m_storeD2dRequestStatement->executeUpdate();
  if (0 == rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to store d2d request";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// storeVersionQueryRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeVersionQueryRequest (castor::query::VersionQuery* req,
                                                          const castor::rh::Client* client)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeVersionQueryStatement) {
    m_storeVersionQueryStatement =
      createStatement(s_storeVersionQueryStatementString);
    m_storeVersionQueryStatement->setAutoCommit(true);
  }
  // execute the statement and see whether we found something
  m_storeVersionQueryStatement->setString(1, req->machine());
  m_storeVersionQueryStatement->setInt(2, req->euid());
  m_storeVersionQueryStatement->setInt(3, req->egid());
  m_storeVersionQueryStatement->setInt(4, req->pid());
  m_storeVersionQueryStatement->setString(5, req->userName());
  m_storeVersionQueryStatement->setString(6, req->svcClassName());
  m_storeVersionQueryStatement->setString(7, req->reqId());
  m_storeVersionQueryStatement->setInt(8, req->type());
  m_storeVersionQueryStatement->setInt(9, client->ipAddress());
  m_storeVersionQueryStatement->setInt(10, client->port());
  m_storeVersionQueryStatement->setInt(11, client->version());
  m_storeVersionQueryStatement->setInt(12, client->secure());
  unsigned int rc = m_storeVersionQueryStatement->executeUpdate();
  if (0 == rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to store VersionQuery request";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// storeStageFileQueryRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeStageFileQueryRequest (castor::stager::StageFileQueryRequest* req,
                                                            const castor::rh::Client* client)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeStageFileQueryRequestStatement) {
    m_storeStageFileQueryRequestStatement =
      createStatement(s_storeStageFileQueryRequestStatementString);
    m_storeStageFileQueryRequestStatement->setAutoCommit(true);
  }
  // easier access to the parameters list
  const std::vector<castor::stager::QueryParameter*> &parameters = req->parameters();
  // Allocate ORACLE buffers for string arguments lengths (value)
  ub2 *lensValue = (ub2*) calloc(parameters.size(), sizeof(ub2));
  if (0 == lensValue) {
    castor::exception::OutOfMemory e; throw e;
  };
  // Fill the buffers for string arguments lengths, and compute the max lengths
  // Note the non zero maximum length so that buffers are not empty. This would not
  // be to the taste of ORACLE....
  unsigned int maxValueLen = 1;
  unsigned int counter = 0;
  for (std::vector<castor::stager::QueryParameter*>::const_iterator it = parameters.begin();
       it != parameters.end();
       it++) {
    lensValue[counter] = (*it)->value().size();
    if ((*it)->value().size() > maxValueLen) maxValueLen = (*it)->value().size();
    counter++;
  }
  // Allocate ORACLE buffers for the string parameters
  unsigned int valueCellSize = maxValueLen * sizeof(char);
  char *bufferValue = (char*) calloc(parameters.size(), valueCellSize);
  if (0 == bufferValue) {
    free(lensValue);
    castor::exception::OutOfMemory e;
    throw e;
  };
  // Allocate ORACLE buffer for the number parameters (queryType) and for their lengths
  ub2 *lensQueryType = (ub2*) calloc(parameters.size(), sizeof(ub2));
  if (0 == lensQueryType) {
    free(lensValue);free(bufferValue);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferQueryType)[21] =
    (unsigned char(*)[21]) calloc(parameters.size(), 21*sizeof(unsigned char));
  if (0 == bufferQueryType) {
    free(lensQueryType);free(lensValue);free(bufferValue);
    castor::exception::OutOfMemory e; throw e;
  };
  // Fill all buffers
  counter = 0;
  for (std::vector<castor::stager::QueryParameter*>::const_iterator it = parameters.begin();
       it != parameters.end();
       it++) {
    strncpy(bufferValue+(counter*valueCellSize), (*it)->value().c_str(), lensValue[counter]);
    oracle::occi::Number oraQueryType = (double)(*it)->queryType();
    oracle::occi::Bytes bqueryType = oraQueryType.toBytes();
    bqueryType.getBytes(bufferQueryType[counter],bqueryType.length());
    lensQueryType[counter] = bqueryType.length();
    counter++;
  }
  try {
    // execute the statement and see whether we found something
    m_storeStageFileQueryRequestStatement->setString(1, req->machine());
    m_storeStageFileQueryRequestStatement->setInt(2, req->euid());
    m_storeStageFileQueryRequestStatement->setInt(3, req->egid());
    m_storeStageFileQueryRequestStatement->setInt(4, req->pid());
    m_storeStageFileQueryRequestStatement->setString(5, req->userName());
    m_storeStageFileQueryRequestStatement->setString(6, req->svcClassName());
    m_storeStageFileQueryRequestStatement->setString(7, req->reqId());
    m_storeStageFileQueryRequestStatement->setInt(8, req->type());
    m_storeStageFileQueryRequestStatement->setInt(9, client->ipAddress());
    m_storeStageFileQueryRequestStatement->setInt(10, client->port());
    m_storeStageFileQueryRequestStatement->setInt(11, client->version());
    m_storeStageFileQueryRequestStatement->setInt(12, client->secure());
    m_storeStageFileQueryRequestStatement->setString(13, req->fileName());
    ub4 nbItems = parameters.size();
    m_storeStageFileQueryRequestStatement->setDataBufferArray
      (14, bufferValue, oracle::occi::OCCI_SQLT_CHR,
       nbItems, &nbItems, maxValueLen, lensValue);
    m_storeStageFileQueryRequestStatement->setDataBufferArray
      (15, bufferQueryType, oracle::occi::OCCI_SQLT_NUM,
       nbItems, &nbItems, 21, lensQueryType);
    // execute the statement
    unsigned int rc = m_storeStageFileQueryRequestStatement->executeUpdate();
    // cleanup memory 
    free(lensQueryType);free(lensValue);free(bufferValue);free(bufferQueryType);
    // check whether the statement was successful
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to store FileQuery request";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    // cleanup memory 
    free(lensQueryType);free(lensValue);free(bufferValue);free(bufferQueryType);
    // rethrow, the handling is done one level up
    throw e;
  }
}

//------------------------------------------------------------------------------
// storeDiskPoolQueryRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeDiskPoolQueryRequest (castor::query::DiskPoolQuery* req,
                                                           const castor::rh::Client* client)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeDiskPoolQueryStatement) {
    m_storeDiskPoolQueryStatement =
      createStatement(s_storeDiskPoolQueryStatementString);
    m_storeDiskPoolQueryStatement->setAutoCommit(true);
  }
  // execute the statement and see whether we found something
  m_storeDiskPoolQueryStatement->setString(1, req->machine());
  m_storeDiskPoolQueryStatement->setInt(2, req->euid());
  m_storeDiskPoolQueryStatement->setInt(3, req->egid());
  m_storeDiskPoolQueryStatement->setInt(4, req->pid());
  m_storeDiskPoolQueryStatement->setString(5, req->userName());
  m_storeDiskPoolQueryStatement->setString(6, req->svcClassName());
  m_storeDiskPoolQueryStatement->setString(7, req->reqId());
  m_storeDiskPoolQueryStatement->setInt(8, req->type());
  m_storeDiskPoolQueryStatement->setInt(9, client->ipAddress());
  m_storeDiskPoolQueryStatement->setInt(10, client->port());
  m_storeDiskPoolQueryStatement->setInt(11, client->version());
  m_storeDiskPoolQueryStatement->setInt(12, client->secure());
  m_storeDiskPoolQueryStatement->setString(13, req->diskPoolName());
  m_storeDiskPoolQueryStatement->setInt(14, req->queryType());
  unsigned int rc = m_storeDiskPoolQueryStatement->executeUpdate();
  if (0 == rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to store DiskPoolQuery request";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// storeMoverCloseRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeMoverCloseRequest (castor::stager::MoverCloseRequest* req,
                                                        const castor::rh::Client* client)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeMoverCloseRequestStatement) {
    m_storeMoverCloseRequestStatement =
      createStatement(s_storeMoverCloseRequestStatementString);
    m_storeMoverCloseRequestStatement->setAutoCommit(true);
  }
  // execute the statement and see whether we found something
  m_storeMoverCloseRequestStatement->setString(1, req->machine());
  m_storeMoverCloseRequestStatement->setInt(2, req->euid());
  m_storeMoverCloseRequestStatement->setInt(3, req->egid());
  m_storeMoverCloseRequestStatement->setInt(4, req->pid());
  m_storeMoverCloseRequestStatement->setString(5, req->userName());
  m_storeMoverCloseRequestStatement->setString(6, req->svcClassName());
  m_storeMoverCloseRequestStatement->setString(7, req->reqId());
  m_storeMoverCloseRequestStatement->setInt(8, req->type());
  m_storeMoverCloseRequestStatement->setInt(9, client->ipAddress());
  m_storeMoverCloseRequestStatement->setInt(10, client->port());
  m_storeMoverCloseRequestStatement->setInt(11, client->version());
  m_storeMoverCloseRequestStatement->setInt(12, client->secure());
  m_storeMoverCloseRequestStatement->setDouble(13, req->subReqId());
  m_storeMoverCloseRequestStatement->setDouble(14, req->fileId());
  m_storeMoverCloseRequestStatement->setString(15, req->nsHost());
  m_storeMoverCloseRequestStatement->setDouble(16, req->fileSize());
  m_storeMoverCloseRequestStatement->setDouble(17, req->timeStamp());
  m_storeMoverCloseRequestStatement->setString(18, req->csumType());
  m_storeMoverCloseRequestStatement->setString(19, req->csumValue());
  unsigned int rc = m_storeMoverCloseRequestStatement->executeUpdate();
  if (0 == rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to store MoverClose request";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// storeFiles2DeleteRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeFiles2DeleteRequest (castor::stager::Files2Delete* req,
                                                          const castor::rh::Client* client)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeFiles2DeleteStatement) {
    m_storeFiles2DeleteStatement =
      createStatement(s_storeFiles2DeleteStatementString);
    m_storeFiles2DeleteStatement->setAutoCommit(true);
  }
  // execute the statement and see whether we found something
  m_storeFiles2DeleteStatement->setString(1, req->machine());
  m_storeFiles2DeleteStatement->setInt(2, req->euid());
  m_storeFiles2DeleteStatement->setInt(3, req->egid());
  m_storeFiles2DeleteStatement->setInt(4, req->pid());
  m_storeFiles2DeleteStatement->setString(5, req->userName());
  m_storeFiles2DeleteStatement->setString(6, req->svcClassName());
  m_storeFiles2DeleteStatement->setString(7, req->reqId());
  m_storeFiles2DeleteStatement->setInt(8, req->type());
  m_storeFiles2DeleteStatement->setInt(9, client->ipAddress());
  m_storeFiles2DeleteStatement->setInt(10, client->port());
  m_storeFiles2DeleteStatement->setInt(11, client->version());
  m_storeFiles2DeleteStatement->setInt(12, client->secure());
  m_storeFiles2DeleteStatement->setString(13, req->diskServer());
  unsigned int rc = m_storeFiles2DeleteStatement->executeUpdate();
  if (0 == rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to store Files2Delete request";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// storeListPrivilegesRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeListPrivilegesRequest (castor::bwlist::ListPrivileges* req,
                                                            const castor::rh::Client* client)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeListPrivilegesStatement) {
    m_storeListPrivilegesStatement =
      createStatement(s_storeListPrivilegesStatementString);
    m_storeListPrivilegesStatement->setAutoCommit(true);
  }
  // execute the statement and see whether we found something
  m_storeListPrivilegesStatement->setString(1, req->machine());
  m_storeListPrivilegesStatement->setInt(2, req->euid());
  m_storeListPrivilegesStatement->setInt(3, req->egid());
  m_storeListPrivilegesStatement->setInt(4, req->pid());
  m_storeListPrivilegesStatement->setString(5, req->userName());
  m_storeListPrivilegesStatement->setString(6, req->svcClassName());
  m_storeListPrivilegesStatement->setString(7, req->reqId());
  m_storeListPrivilegesStatement->setInt(8, req->type());
  m_storeListPrivilegesStatement->setInt(9, client->ipAddress());
  m_storeListPrivilegesStatement->setInt(10, client->port());
  m_storeListPrivilegesStatement->setInt(11, client->version());
  m_storeListPrivilegesStatement->setInt(12, client->secure());
  m_storeListPrivilegesStatement->setInt(13, req->userId());
  m_storeListPrivilegesStatement->setInt(14, req->groupId());
  m_storeListPrivilegesStatement->setInt(15, req->requestType());
  unsigned int rc = m_storeListPrivilegesStatement->executeUpdate();
  if (0 == rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to store ListPrivileges request";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// storeStageAbortRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeStageAbortRequest (castor::stager::StageAbortRequest* req,
                                                        const castor::rh::Client* client)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeStageAbortRequestStatement) {
    m_storeStageAbortRequestStatement =
      createStatement(s_storeStageAbortRequestStatementString);
    m_storeStageAbortRequestStatement->setAutoCommit(true);
  }
  // easier access to the fileid list
  const std::vector<castor::stager::NsFileId*> &fileids = req->files();
  // actual size of the sent buffer (cannot be 0 thanks to Oracle....)
  unsigned int buffersLen = fileids.size();
  if (buffersLen == 0) buffersLen++;
  // Allocate ORACLE buffers for string arguments lengths (nsHost)
  ub2 *lensNsHost = (ub2*) calloc(buffersLen, sizeof(ub2));
  if (0 == lensNsHost) {
    castor::exception::OutOfMemory e; throw e;
  };
  // Fill the buffers for string arguments lengths, and compute the max lengths
  // Note the non zero maximum length so that buffers are not empty. This would not
  // be to the taste of ORACLE....
  unsigned int maxNsHostLen = 1;
  unsigned int counter = 0;
  if (0 == fileids.size()) lensNsHost[0] = 0;
  for (std::vector<castor::stager::NsFileId*>::const_iterator it = fileids.begin();
       it != fileids.end();
       it++) {
    lensNsHost[counter] = (*it)->nsHost().size();
    if ((*it)->nsHost().size() > maxNsHostLen) maxNsHostLen = (*it)->nsHost().size();
    counter++;
  }
  // Allocate ORACLE buffers for the string arguments
  unsigned int nsHostCellSize = maxNsHostLen * sizeof(char);
  char *bufferNsHost = (char*) calloc(buffersLen, nsHostCellSize);
  if (0 == bufferNsHost) {
    free(lensNsHost);
    castor::exception::OutOfMemory e;
    throw e;
  };
  // Allocate ORACLE buffer for the number arguments (fileId) and for their lengths
  ub2 *lensFileId = (ub2*) calloc(buffersLen, sizeof(ub2));
  if (0 == lensFileId) {
    free(lensNsHost);free(bufferNsHost);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferFileId)[21] =
    (unsigned char(*)[21]) calloc(buffersLen, 21*sizeof(unsigned char));
  if (0 == bufferFileId) {
    free(lensFileId);free(lensNsHost);free(bufferNsHost);
    castor::exception::OutOfMemory e; throw e;
  };
  // Fill all buffers
  counter = 0;
  if (0 == fileids.size()) {
    oracle::occi::Number oraFileId = 0;
    oracle::occi::Bytes bfileId = oraFileId.toBytes();
    bfileId.getBytes(bufferFileId[0],bfileId.length());
    lensFileId[0] = bfileId.length();
  }
  for (std::vector<castor::stager::NsFileId*>::const_iterator it = fileids.begin();
       it != fileids.end();
       it++) {
    strncpy(bufferNsHost+(counter*nsHostCellSize), (*it)->nsHost().c_str(), lensNsHost[counter]);
    oracle::occi::Number oraFileId = (double)(*it)->fileid();
    oracle::occi::Bytes bfileId = oraFileId.toBytes();
    bfileId.getBytes(bufferFileId[counter],bfileId.length());
    lensFileId[counter] = bfileId.length();
    counter++;
  }
  try {
    // execute the statement and see whether we found something
    m_storeStageAbortRequestStatement->setString(1, req->machine());
    m_storeStageAbortRequestStatement->setInt(2, req->euid());
    m_storeStageAbortRequestStatement->setInt(3, req->egid());
    m_storeStageAbortRequestStatement->setInt(4, req->pid());
    m_storeStageAbortRequestStatement->setString(5, req->userName());
    m_storeStageAbortRequestStatement->setString(6, req->svcClassName());
    m_storeStageAbortRequestStatement->setString(7, req->reqId());
    m_storeStageAbortRequestStatement->setInt(8, req->type());
    m_storeStageAbortRequestStatement->setInt(9, client->ipAddress());
    m_storeStageAbortRequestStatement->setInt(10, client->port());
    m_storeStageAbortRequestStatement->setInt(11, client->version());
    m_storeStageAbortRequestStatement->setInt(12, client->secure());
    m_storeStageAbortRequestStatement->setString(13, req->parentUuid());
    ub4 nbItems = buffersLen;
    m_storeStageAbortRequestStatement->setDataBufferArray
      (14, bufferFileId, oracle::occi::OCCI_SQLT_NUM,
       nbItems, &nbItems, 21, lensFileId);
    m_storeStageAbortRequestStatement->setDataBufferArray
      (15, bufferNsHost, oracle::occi::OCCI_SQLT_CHR,
       nbItems, &nbItems, maxNsHostLen, lensNsHost);
    // execute the statement
    unsigned int rc = m_storeStageAbortRequestStatement->executeUpdate();
    // cleanup memory 
    free(lensFileId);free(lensNsHost);free(bufferNsHost);free(bufferFileId);
    // check whether the statement was successful
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to store Abort request";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    // cleanup memory 
    free(lensFileId);free(lensNsHost);free(bufferNsHost);free(bufferFileId);
    // rethrow, the handling is done one level up
    throw e;
  }
}

//------------------------------------------------------------------------------
// storeChangePrivilegeRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeChangePrivilegeRequest (castor::bwlist::ChangePrivilege* req,
                                                             const castor::rh::Client* client)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeChangePrivilegeStatement) {
    m_storeChangePrivilegeStatement =
      createStatement(s_storeChangePrivilegeStatementString);
    m_storeChangePrivilegeStatement->setAutoCommit(true);
  }
  // easier access to the users and requestTypes list
  const std::vector<castor::bwlist::BWUser*> &users = req->users();
  const std::vector<castor::bwlist::RequestType*> &requestTypes = req->requestTypes();
  // Allocate ORACLE buffer for the number arguments (uids, gids and reqTypes) and for their lengths
  ub2 *lensUid = (ub2*) calloc(users.size(), sizeof(ub2));
  if (0 == lensUid) {
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferUid)[21] =
    (unsigned char(*)[21]) calloc(users.size(), 21*sizeof(unsigned char));
  if (0 == bufferUid) {
    free(lensUid);
    castor::exception::OutOfMemory e; throw e;
  };
  ub2 *lensGid = (ub2*) calloc(users.size(), sizeof(ub2));
  if (0 == lensGid) {
    free(lensUid);free(bufferUid);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferGid)[21] =
    (unsigned char(*)[21]) calloc(users.size(), 21*sizeof(unsigned char));
  if (0 == bufferGid) {
    free(lensUid);free(bufferUid);free(lensGid);
    castor::exception::OutOfMemory e; throw e;
  };
  ub2 *lensReqType = (ub2*) calloc(requestTypes.size(), sizeof(ub2));
  if (0 == lensReqType) {
    free(lensUid);free(bufferUid);free(lensGid);free(bufferGid);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferReqType)[21] =
    (unsigned char(*)[21]) calloc(requestTypes.size(), 21*sizeof(unsigned char));
  if (0 == bufferReqType) {
    free(lensUid);free(bufferUid);free(lensGid);free(bufferGid);free(lensReqType);
    castor::exception::OutOfMemory e; throw e;
  };
  // Fill all buffers
  unsigned int counter = 0;
  for (std::vector<castor::bwlist::BWUser*>::const_iterator it = users.begin();
       it != users.end();
       it++) {
    oracle::occi::Number oraUid = (double)(*it)->euid();
    oracle::occi::Bytes bUid = oraUid.toBytes();
    bUid.getBytes(bufferUid[counter],bUid.length());
    lensUid[counter] = bUid.length();
    oracle::occi::Number oraGid = (double)(*it)->egid();
    oracle::occi::Bytes bGid = oraGid.toBytes();
    bGid.getBytes(bufferGid[counter],bGid.length());
    lensGid[counter] = bGid.length();
    counter++;
  }
  counter = 0;
  for (std::vector<castor::bwlist::RequestType*>::const_iterator it = requestTypes.begin();
       it != requestTypes.end();
       it++) {
    oracle::occi::Number oraReqType = (double)(*it)->reqType();
    oracle::occi::Bytes bReqType = oraReqType.toBytes();
    bReqType.getBytes(bufferReqType[counter],bReqType.length());
    lensReqType[counter] = bReqType.length();
    counter++;
  }
  try {
    // execute the statement and see whether we found something
    m_storeChangePrivilegeStatement->setString(1, req->machine());
    m_storeChangePrivilegeStatement->setInt(2, req->euid());
    m_storeChangePrivilegeStatement->setInt(3, req->egid());
    m_storeChangePrivilegeStatement->setInt(4, req->pid());
    m_storeChangePrivilegeStatement->setString(5, req->userName());
    m_storeChangePrivilegeStatement->setString(6, req->svcClassName());
    m_storeChangePrivilegeStatement->setString(7, req->reqId());
    m_storeChangePrivilegeStatement->setInt(8, req->type());
    m_storeChangePrivilegeStatement->setInt(9, client->ipAddress());
    m_storeChangePrivilegeStatement->setInt(10, client->port());
    m_storeChangePrivilegeStatement->setInt(11, client->version());
    m_storeChangePrivilegeStatement->setInt(12, client->secure());
    m_storeChangePrivilegeStatement->setInt(13, req->isGranted());
    ub4 nbUsers = users.size();
    m_storeChangePrivilegeStatement->setDataBufferArray
      (14, bufferUid, oracle::occi::OCCI_SQLT_NUM,
       nbUsers, &nbUsers, 21, lensUid);
    m_storeChangePrivilegeStatement->setDataBufferArray
      (14, bufferGid, oracle::occi::OCCI_SQLT_NUM,
       nbUsers, &nbUsers, 21, lensGid);
    ub4 nbReqTypes = users.size();
    m_storeChangePrivilegeStatement->setDataBufferArray
      (14, bufferReqType, oracle::occi::OCCI_SQLT_NUM,
       nbReqTypes, &nbReqTypes, 21, lensReqType);
    // execute the statement
    unsigned int rc = m_storeChangePrivilegeStatement->executeUpdate();
    // cleanup memory 
    free(lensUid);free(bufferUid);free(lensGid);free(bufferGid);free(lensReqType);free(bufferReqType);
    // check whether the statement was successful
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to store ChangePrivilege request";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    // cleanup memory 
    free(lensUid);free(bufferUid);free(lensGid);free(bufferGid);free(lensReqType);free(bufferReqType);
    // rethrow, the handling is done one level up
    throw e;
  }
}

//------------------------------------------------------------------------------
// storeGCRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::storeGCRequest (castor::stager::GCFileList* req,
                                                const castor::rh::Client *client,
                                                const std::string nsHost)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check whether the statement is ok
  if (0 == m_storeGCStatement) {
    m_storeGCStatement =
      createStatement(s_storeGCStatementString);
    m_storeGCStatement->setAutoCommit(true);
  }
  // easier access to the users and requestTypes list
  const std::vector<castor::stager::GCFile*> &files = req->files();
  // Allocate ORACLE buffer for the number arguments (diskCopyId) and for their lengths
  ub2 *lensDiskCopyId = (ub2*) calloc(files.size(), sizeof(ub2));
  if (0 == lensDiskCopyId) {
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferDiskCopyId)[21] =
    (unsigned char(*)[21]) calloc(files.size(), 21*sizeof(unsigned char));
  if (0 == bufferDiskCopyId) {
    free(lensDiskCopyId);
    castor::exception::OutOfMemory e; throw e;
  };
  // Fill all buffers
  unsigned int counter = 0;
  for (std::vector<castor::stager::GCFile*>::const_iterator it = files.begin();
       it != files.end();
       it++) {
    oracle::occi::Number oraDiskCopyId = (double)(*it)->diskCopyId();
    oracle::occi::Bytes bDiskCopyId = oraDiskCopyId.toBytes();
    bDiskCopyId.getBytes(bufferDiskCopyId[counter],bDiskCopyId.length());
    lensDiskCopyId[counter] = bDiskCopyId.length();
    counter++;
  }
  try {
    // execute the statement and see whether we found something
    m_storeGCStatement->setString(1, req->machine());
    m_storeGCStatement->setInt(2, req->euid());
    m_storeGCStatement->setInt(3, req->egid());
    m_storeGCStatement->setInt(4, req->pid());
    m_storeGCStatement->setString(5, req->userName());
    m_storeGCStatement->setString(6, req->svcClassName());
    m_storeGCStatement->setString(7, req->reqId());
    m_storeGCStatement->setInt(8, req->type());
    m_storeGCStatement->setInt(9, client->ipAddress());
    m_storeGCStatement->setInt(10, client->port());
    m_storeGCStatement->setInt(11, client->version());
    m_storeGCStatement->setInt(12, client->secure());
    m_storeGCStatement->setString(13, nsHost);
    ub4 nbDiskCopyIds = files.size();
    m_storeGCStatement->setDataBufferArray
      (14, bufferDiskCopyId, oracle::occi::OCCI_SQLT_NUM,
       nbDiskCopyIds, &nbDiskCopyIds, 21, lensDiskCopyId);
    // execute the statement
    unsigned int rc = m_storeGCStatement->executeUpdate();
    //cleanup memory 
    free(lensDiskCopyId);free(bufferDiskCopyId);
    // check whether the statement was successful
    if (0 == rc) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to store GC request";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    // cleanup memory 
    free(lensDiskCopyId);free(bufferDiskCopyId);
    // rethrow, the handling is done one level up
    throw e;
  }
}

//------------------------------------------------------------------------------
// handleChangePrivilegeTypeLoop
//------------------------------------------------------------------------------
void handleChangePrivilegeTypeLoop
(std::vector<castor::bwlist::RequestType*> &requestTypes,
 oracle::occi::Statement *stmt)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // loop on the request types
  if (requestTypes.size() > 0) {
    for (std::vector<castor::bwlist::RequestType*>::const_iterator typeIt =
           requestTypes.begin();
         typeIt != requestTypes.end();
         typeIt++) {
      if ((*typeIt)->reqType() > 0) {
        stmt->setInt(4, (*typeIt)->reqType());
      } else {
        stmt->setNull(4, oracle::occi::OCCINUMBER);
      }
      stmt->executeUpdate();
    }
  } else {
    // no request type given, that means all of them are targeted
    stmt->setNull(4, oracle::occi::OCCINUMBER);
    stmt->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// changePrivilege
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::changePrivilege
(const std::string svcClassName,
 std::vector<castor::bwlist::BWUser*> users,
 std::vector<castor::bwlist::RequestType*> requestTypes,
 bool isAdd)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (isAdd){
      if (0 == m_addPrivilegeStatement) {
        m_addPrivilegeStatement =
          createStatement(s_addPrivilegeStatementString);
      }
    } else {
      if (0 == m_removePrivilegeStatement) {
        m_removePrivilegeStatement =
          createStatement(s_removePrivilegeStatementString);
      }
    }
    // deal with the type of change
    oracle::occi::Statement *stmt =
      isAdd ? m_addPrivilegeStatement : m_removePrivilegeStatement;
    // deal with the service class if any
    if (svcClassName != "*") {
      stmt->setString(1, svcClassName);
    } else {
      stmt->setNull(1, oracle::occi::OCCISTRING);
    }
    // loop over users if any
    if (users.size() > 0) {
      for (std::vector<castor::bwlist::BWUser*>::const_iterator userIt =
             users.begin();
           userIt != users.end();
           userIt++) {
        if ((*userIt)->euid() >= 0) {
          stmt->setInt(2, (*userIt)->euid());
        } else {
          stmt->setNull(2, oracle::occi::OCCINUMBER);
        }
        if ((*userIt)->egid() >= 0) {
          stmt->setInt(3, (*userIt)->egid());
        } else {
          stmt->setNull(3, oracle::occi::OCCINUMBER);
        }
        // loop on the request types if any and call DB
        handleChangePrivilegeTypeLoop(requestTypes, stmt);
      }
    } else {
      // empty user list, that means all uids and all gids
      stmt->setNull(2, oracle::occi::OCCINUMBER);
      stmt->setNull(3, oracle::occi::OCCINUMBER);
      // loop on the request types if any and call DB
      handleChangePrivilegeTypeLoop(requestTypes, stmt);
    }
    commit();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    if (e.getErrorCode() == 20108) {
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "The operation you are requesting cannot be expressed "
                      << "in the privilege table.\nThis means "
                      << "that you are trying to grant only part of a privilege "
                      << "that is currently denied.\nConsider granting all of it "
                      << "and denying the complement afterward";
      throw ex;
    } else if (e.getErrorCode() == 20113) {
      std::string msg = e.what();
      castor::exception::InvalidArgument ex;
      // extract the original message sent by the PL/SQL code
      // from the surrounding oracle additions
      ex.getMessage() << msg.substr(11,msg.find('\n')-11) << "\n";
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage()
        << "Error caught in changePrivilege."
        << std::endl << e.what();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// listPrivileges
//------------------------------------------------------------------------------
std::vector<castor::bwlist::Privilege*>
castor::db::ora::OraRHSvc::listPrivileges
(const std::string svcClassName, const int user,
 const int group, const int requestType)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_listPrivilegesStatement) {
      m_listPrivilegesStatement =
        createStatement(s_listPrivilegesStatementString);
      m_listPrivilegesStatement->registerOutParam
        (5, oracle::occi::OCCICURSOR);
    }
    // deal with the service class, user, group and type
    if (svcClassName != "*") {
      m_listPrivilegesStatement->setString(1, svcClassName);
    } else {
      m_listPrivilegesStatement->setNull(1, oracle::occi::OCCISTRING);
    }
    m_listPrivilegesStatement->setInt(2, user);
    m_listPrivilegesStatement->setInt(3, group);
    m_listPrivilegesStatement->setInt(4, requestType );
    // Call DB
    m_listPrivilegesStatement->executeUpdate();
    // Extract list of privileges
    std::vector<castor::bwlist::Privilege*> result;
    oracle::occi::ResultSet *prs =
      m_listPrivilegesStatement->getCursor(5);
    oracle::occi::ResultSet::Status status = prs->next();
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::bwlist::Privilege* p = new castor::bwlist::Privilege();
      p->setServiceClass(prs->getString(1));
      p->setEuid(prs->getInt(2));
      if (prs->isNull(2)) p->setEuid(-1);
      p->setEgid(prs->getInt(3));
      if (prs->isNull(3)) p->setEgid(-1);
      p->setRequestType(prs->getInt(4));
      p->setGranted(prs->getInt(5) != 0);
      result.push_back(p);
      status = prs->next();
    }
    // return result
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in listPrivileges."
      << std::endl << e.what();
    throw ex;
  }
}

