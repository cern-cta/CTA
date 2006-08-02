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
 * @(#)$RCSfile: OraStagerSvc.cpp,v $ $Revision: 1.185 $ $Release$ $Date: 2006/08/02 14:04:21 $ $Author: gtaur $
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
#include "castor/stager/Stream.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskPool.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/stager/FileClass.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/Files2Delete.hpp"
#include "castor/stager/FilesDeleted.hpp"
#include "castor/stager/FilesDeletionFailed.hpp"
#include "castor/stager/GetUpdateDone.hpp"
#include "castor/stager/GetUpdateFailed.hpp"
#include "castor/stager/PutFailed.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "castor/stager/GCFile.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/TapeCopyForMigration.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Busy.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/stager/TapeStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/BaseAddress.hpp"
#include "OraStagerSvc.hpp"
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

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraStagerSvc>* s_factoryOraStagerSvc =
  new castor::SvcFactory<castor::db::ora::OraStagerSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for subRequestFailedToDo
const std::string castor::db::ora::OraStagerSvc::s_subRequestFailedToDoStatementString =
  // 10 = SUBREQUEST_FAILED_ANSWERING, 7 = SUBREQUEST_FAILED
  "UPDATE SubRequest SET status = 10 WHERE decode(status,7,status,null) = 7 AND ROWNUM < 2 RETURNING id, retryCounter, fileName, protocol, xsize, priority,  status, modeBits, flags, repackVid INTO :1, :2, :3, :4, :5 ,:6 , :7, :8, :9, :10";

/// SQL statement for isSubRequestToSchedule
const std::string castor::db::ora::OraStagerSvc::s_isSubRequestToScheduleStatementString =
  "BEGIN isSubRequestToSchedule(:1, :2, :3); END;";

/// SQL statement for selectCastorFile
const std::string castor::db::ora::OraStagerSvc::s_selectCastorFileStatementString =
  "BEGIN selectCastorFile(:1, :2, :3, :4, :5, :6, :7, :8); END;";

/// SQL statement for updateAndCheckSubRequest
const std::string castor::db::ora::OraStagerSvc::s_updateAndCheckSubRequestStatementString =
  "BEGIN updateAndCheckSubRequest(:1, :2, :3); END;";

/// SQL statement for recreateCastorFile
const std::string castor::db::ora::OraStagerSvc::s_recreateCastorFileStatementString =
  "BEGIN recreateCastorFile(:1, :2, :3, :4, :5, :6); END;";

/// SQL statement for bestFileSystemForJob
const std::string castor::db::ora::OraStagerSvc::s_bestFileSystemForJobStatementString =
  "BEGIN bestFileSystemForJob(:1, :2, :3, :4, :5); END;";

/// SQL statement for updateFileSystemForJob
const std::string castor::db::ora::OraStagerSvc::s_updateFileSystemForJobStatementString =
  "BEGIN updateFileSystemForJob(:1, :2, :3); END;";

/// SQL statement for archiveSubReq
const std::string castor::db::ora::OraStagerSvc::s_archiveSubReqStatementString =
  "BEGIN archiveSubReq(:1); END;";

/// SQL statement for stageRelease
const std::string castor::db::ora::OraStagerSvc::s_stageReleaseStatementString =
  "BEGIN stageRelease(:1, :2, :3); END;";

/// SQL statement for stageRm
const std::string castor::db::ora::OraStagerSvc::s_stageRmStatementString =
  "BEGIN stageRm(:1, :2, :3); END;";

/// SQL statement for setFileGCWeight
const std::string castor::db::ora::OraStagerSvc::s_setFileGCWeightStatementString =
  "UPDATE DiskCopy SET gcWeight = :1 WHERE castorfile = (SELECT id FROM castorfile WHERE fileId = :2 and nsHost = :3)";

// -----------------------------------------------------------------------
// OraStagerSvc
// -----------------------------------------------------------------------
castor::db::ora::OraStagerSvc::OraStagerSvc(const std::string name) :
  OraCommonSvc(name),
  m_subRequestToDoStatement(0),
  m_subRequestFailedToDoStatement(0),
  m_isSubRequestToScheduleStatement(0),
  m_selectCastorFileStatement(0),
  m_updateAndCheckSubRequestStatement(0),
  m_recreateCastorFileStatement(0),
  m_bestFileSystemForJobStatement(0),
  m_updateFileSystemForJobStatement(0),
  m_archiveSubReqStatement(0),
  m_stageReleaseStatement(0),
  m_stageRmStatement(0),
  m_setFileGCWeightStatement(0) {
}

// -----------------------------------------------------------------------
// ~OraStagerSvc
// -----------------------------------------------------------------------
castor::db::ora::OraStagerSvc::~OraStagerSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraStagerSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraStagerSvc::ID() {
  return castor::SVC_ORASTAGERSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    deleteStatement(m_subRequestToDoStatement);
    deleteStatement(m_subRequestFailedToDoStatement);
    deleteStatement(m_isSubRequestToScheduleStatement);
    deleteStatement(m_selectCastorFileStatement);
    deleteStatement(m_updateAndCheckSubRequestStatement);
    deleteStatement(m_recreateCastorFileStatement);
    deleteStatement(m_bestFileSystemForJobStatement);
    deleteStatement(m_updateFileSystemForJobStatement);
    deleteStatement(m_archiveSubReqStatement);
    deleteStatement(m_stageReleaseStatement);
    deleteStatement(m_stageRmStatement);
    deleteStatement(m_setFileGCWeightStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_subRequestToDoStatement = 0;
  m_subRequestFailedToDoStatement = 0;
  m_isSubRequestToScheduleStatement = 0;
  m_selectCastorFileStatement = 0;
  m_updateAndCheckSubRequestStatement = 0;
  m_recreateCastorFileStatement = 0;
  m_bestFileSystemForJobStatement = 0;
  m_updateFileSystemForJobStatement = 0;
  m_archiveSubReqStatement = 0;
  m_stageReleaseStatement = 0;
  m_stageRmStatement = 0;
  m_setFileGCWeightStatement = 0;
}


// -----------------------------------------------------------------------
// subRequestToDo
// -----------------------------------------------------------------------
castor::stager::SubRequest*
castor::db::ora::OraStagerSvc::subRequestToDo
(std::vector<ObjectsIds> &types)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_subRequestToDoStatement) {
      std::ostringstream stmtString;
      stmtString
        << "UPDATE SubRequest SET status = 3 " // SUBREQUEST_WAITSCHED
        // Here use I_SubRequest_status index to retrieve SubRequests
        // in START, RESTART and RETRY status
        << " WHERE (decode(status,0,status,1,status,2,status,NULL)) < 3"
        << " AND ROWNUM < 2 AND "
        << "(SELECT type FROM Id2Type WHERE id = SubRequest.request)"
        << " IN (";
      for (std::vector<ObjectsIds>::const_iterator it = types.begin();
           it!= types.end();
           it++) {
        if (types.begin() != it) stmtString << ", ";
        stmtString << *it;
      }
      stmtString
        << ") RETURNING id, retryCounter, fileName, "
        << "protocol, xsize, priority, status, modeBits, flags, repackVid"
        << " INTO :1, :2, :3, :4, :5 ,:6 , :7, :8, :9, :10";

      m_subRequestToDoStatement =
        createStatement(stmtString.str());
      m_subRequestToDoStatement->registerOutParam
        (1, oracle::occi::OCCIDOUBLE);
      m_subRequestToDoStatement->registerOutParam
        (2, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (3, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_subRequestToDoStatement->registerOutParam
        (5, oracle::occi::OCCIDOUBLE);
      m_subRequestToDoStatement->registerOutParam
        (6, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (8, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (9, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (10, oracle::occi::OCCISTRING, 10);
      m_subRequestToDoStatement->setAutoCommit(true);
    }
    // build the list of
    // execute the statement and see whether we found something
    unsigned int nb =
      m_subRequestToDoStatement->executeUpdate();
    if (0 == nb) {
      // Found no SubRequest to handle
      return 0;
    }
    // Create result
    castor::stager::SubRequest* result =
      new castor::stager::SubRequest();
    result->setId((u_signed64)m_subRequestToDoStatement->getDouble(1));
    result->setRetryCounter(m_subRequestToDoStatement->getInt(2));
    result->setFileName(m_subRequestToDoStatement->getString(3));
    result->setProtocol(m_subRequestToDoStatement->getString(4));
    result->setXsize((u_signed64)m_subRequestToDoStatement->getDouble(5));
    result->setPriority(m_subRequestToDoStatement->getInt(6));
    result->setStatus
      ((enum castor::stager::SubRequestStatusCodes)
       m_subRequestToDoStatement->getInt(7));
    result->setModeBits(m_subRequestToDoStatement->getInt(8));
    result->setFlags(m_subRequestToDoStatement->getInt(9));
    result->setRepackVid(m_subRequestToDoStatement->getString(10));
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in subRequestToDo."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// subRequestFailedToDo
// -----------------------------------------------------------------------
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
        (2, oracle::occi::OCCIINT);
      m_subRequestFailedToDoStatement->registerOutParam
        (3, oracle::occi::OCCISTRING, 2048);
      m_subRequestFailedToDoStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_subRequestFailedToDoStatement->registerOutParam
        (5, oracle::occi::OCCIDOUBLE);
      m_subRequestFailedToDoStatement->registerOutParam
        (6, oracle::occi::OCCIINT);
      m_subRequestFailedToDoStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
      m_subRequestFailedToDoStatement->registerOutParam
        (8, oracle::occi::OCCIINT);
      m_subRequestFailedToDoStatement->registerOutParam
        (9, oracle::occi::OCCIINT);
      m_subRequestFailedToDoStatement->registerOutParam
        (10, oracle::occi::OCCISTRING, 2048);
      m_subRequestFailedToDoStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    unsigned int nb =
      m_subRequestFailedToDoStatement->executeUpdate();
    if (0 == nb) {
      // Found no SubRequest to handle
      return 0;
    }
    // Create result
    castor::stager::SubRequest* result =
      new castor::stager::SubRequest();
    result->setId((u_signed64)m_subRequestFailedToDoStatement->getDouble(1));
    result->setRetryCounter(m_subRequestFailedToDoStatement->getInt(2));
    result->setFileName(m_subRequestFailedToDoStatement->getString(3));
    result->setProtocol(m_subRequestFailedToDoStatement->getString(4));
    result->setXsize((u_signed64)m_subRequestFailedToDoStatement->getDouble(5));
    result->setPriority(m_subRequestFailedToDoStatement->getInt(6));
    result->setStatus
      ((enum castor::stager::SubRequestStatusCodes)
       m_subRequestFailedToDoStatement->getInt(7));
    result->setModeBits(m_subRequestFailedToDoStatement->getInt(8));
    result->setFlags(m_subRequestFailedToDoStatement->getInt(9));
    result->setRepackVid(m_subRequestFailedToDoStatement->getString(10));
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in subRequestFailedToDo."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// isSubRequestToSchedule
// -----------------------------------------------------------------------
bool castor::db::ora::OraStagerSvc::isSubRequestToSchedule
(castor::stager::SubRequest* subreq,
 std::list<castor::stager::DiskCopyForRecall*>& sources)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_isSubRequestToScheduleStatement) {
      m_isSubRequestToScheduleStatement =
        createStatement(s_isSubRequestToScheduleStatementString);
      m_isSubRequestToScheduleStatement->registerOutParam
        (2, oracle::occi::OCCIINT);
      m_isSubRequestToScheduleStatement->registerOutParam
        (3, oracle::occi::OCCICURSOR);
    }
    // execute the statement and see whether we found something
    m_isSubRequestToScheduleStatement->setDouble(1, subreq->id());
    unsigned int nb =
      m_isSubRequestToScheduleStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "isSubRequestToSchedule : "
        << "unable to know whether SubRequest should be scheduled.";
      throw ex;
    }
    // Get result and return
    unsigned int rc =
      m_isSubRequestToScheduleStatement->getInt(2);
    if (1 == rc) {
      // status 1 means diskcopies are available, list them
      try {
        oracle::occi::ResultSet *rs =
          m_isSubRequestToScheduleStatement->getCursor(3);
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
      } catch (oracle::occi::SQLException e) {
        cnvSvc()->handleException(e);
        if (e.getErrorCode() != 24338) {
          // if not "statement handle not executed"
          // it's really wrong, else, it's normal
          throw e;
        }
      }
    }
    return 0 != rc;
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in isSubRequestToSchedule."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// selectCastorFile
// -----------------------------------------------------------------------
castor::stager::CastorFile*
castor::db::ora::OraStagerSvc::selectCastorFile
(const u_signed64 fileId, const std::string nsHost,
 u_signed64 svcClass, u_signed64 fileClass,
 u_signed64 fileSize, std::string fileName)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectCastorFileStatement) {
    m_selectCastorFileStatement =
      createStatement(s_selectCastorFileStatementString);
    m_selectCastorFileStatement->registerOutParam
      (7, oracle::occi::OCCIDOUBLE);
    m_selectCastorFileStatement->registerOutParam
      (8, oracle::occi::OCCIDOUBLE);
    m_selectCastorFileStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectCastorFileStatement->setDouble(1, fileId);
    m_selectCastorFileStatement->setString(2, nsHost);
    m_selectCastorFileStatement->setDouble(3, svcClass);
    m_selectCastorFileStatement->setDouble(4, fileClass);
    m_selectCastorFileStatement->setDouble(5, fileSize);
    m_selectCastorFileStatement->setString(6, fileName);
   
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
    result->setId((u_signed64)m_selectCastorFileStatement->getDouble(7));
    result->setFileId(fileId);
    result->setNsHost(nsHost);
    result->setFileSize((u_signed64)m_selectCastorFileStatement->getDouble(8));
    return result;
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select castorFile by fileId :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// updateAndCheckSubRequest
// -----------------------------------------------------------------------
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
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateAndCheckSubRequest."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// recreateCastorFile
// -----------------------------------------------------------------------
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
      m_recreateCastorFileStatement->setAutoCommit(true);
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
    // case of No CastorFile, return 0
    if (0 == id) return 0;
    // Otherwise, build a DiskCopyForRecall
    castor::stager::DiskCopyForRecall *result =
      new castor::stager::DiskCopyForRecall();
    result->setId(id);
    result->setStatus((castor::stager::DiskCopyStatusCodes)
                      m_recreateCastorFileStatement->getInt(4));
    result->setMountPoint(m_recreateCastorFileStatement->getString(5));
    result->setDiskServer(m_recreateCastorFileStatement->getString(6));
    return result;
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in recreateCastorFile."
      << std::endl << e.what();
    throw ex;
  }  
}

// -----------------------------------------------------------------------
// bestFileSystemForJob
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::bestFileSystemForJob
(char** fileSystems,
 char** machines,
 u_signed64* minFree,
 unsigned int fileSystemsNb,
 std::string* mountPoint,
 std::string* diskServer)
  throw (castor::exception::Exception) {
  // check that filesystem is null if machines is null
  if (machines == 0 && fileSystems != 0) {
    castor::exception::InvalidArgument ex;
    ex.getMessage()
      << "bestFileSystemForJob caught with no machines but filesystems."
      << std::endl;
    throw ex;
  }
  try {
    // Check whether the statements are ok
    if (0 == m_bestFileSystemForJobStatement) {
      m_bestFileSystemForJobStatement =
        createStatement(s_bestFileSystemForJobStatementString);
      m_bestFileSystemForJobStatement->setAutoCommit(true);
      m_bestFileSystemForJobStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_bestFileSystemForJobStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048);
    }
    // lengths of the different arrays
    unsigned int fileSystemsL = fileSystems == 0 ? 0 : fileSystemsNb;
    unsigned int machinesL = machines == 0 ? 0 : fileSystemsNb;
    // Compute actual length of the buffers : this
    // may be different from the needed one, since
    // Oracle does not like 0 length arrays....
    unsigned int fileSystemsLa = fileSystemsL == 0 ? 1 : fileSystemsL;
    unsigned int machinesLa = machinesL == 0 ? 1 : machinesL;
    // Find max length of the input parameters
    ub2 lensFS[fileSystemsLa], lensM[machinesLa];
    unsigned int maxFS = 0;
    unsigned int maxM = 0;
    // in case fileSystemsLa != fileSystemsL
    lensFS[fileSystemsLa-1]= 0;
    // in case machinesLa != machinesL
    lensM[machinesLa-1]= 0;    
    for (int i = 0; i < fileSystemsL; i++) {
      lensFS[i] = strlen(fileSystems[i]);
      if (lensFS[i] > maxFS) maxFS = lensFS[i];
    }
    for (int i = 0; i < machinesL; i++) { 
      lensM[i] = strlen(machines[i]);
      if (lensM[i] > maxM) maxM = lensM[i];
    }
    // Allocate buffer for giving the parameters to ORACLE
    char bufferFS[fileSystemsLa][maxFS];
    char bufferM[machinesLa][maxM];
    // Copy inputs into the buffer
    for (int i = 0; i < fileSystemsL; i++) {
      strncpy(bufferFS[i], fileSystems[i], lensFS[i]);
    }
    for (int i = 0; i < machinesL; i++) { 
      strncpy(bufferM[i], machines[i], lensM[i]);
    }
    // Deal with the special case of u_signed64 parameters
    unsigned int fsNbFree = fileSystems != 0 ? fileSystemsNb : 1;
    ub2 lensMF[fsNbFree];
    unsigned char bufferMF[fsNbFree][21];
    memset(bufferMF, 0, fsNbFree * 21);
    for (int i = 0; i < fsNbFree; i++) {
      oracle::occi::Number n = (double)minFree[i];
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferMF[i],b.length());
      lensMF[i] = b.length();
    }
    // execute the statement and see whether we found something
    ub4 unusedFS = fileSystemsL;    
    ub4 unusedM = machinesL;    
    ub4 unusedMF = fsNbFree;
    m_bestFileSystemForJobStatement->setDataBufferArray
      (1, bufferFS, oracle::occi::OCCI_SQLT_CHR,
       fileSystemsLa, &unusedFS, maxFS, lensFS);
    m_bestFileSystemForJobStatement->setDataBufferArray
      (2, bufferM, oracle::occi::OCCI_SQLT_CHR,
       machinesLa, &unusedM, maxM, lensM);
    m_bestFileSystemForJobStatement->setDataBufferArray
      (3, bufferMF, oracle::occi::OCCI_SQLT_NUM,
       fsNbFree, &unusedMF, 21, lensMF);
    unsigned int nb = m_bestFileSystemForJobStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "bestFileSystemForJob did not return any result.";
      throw ex;
    }
    // collect output
    *mountPoint = m_bestFileSystemForJobStatement->getString(4);
    *diskServer = m_bestFileSystemForJobStatement->getString(5);
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in bestFileSystemForJob."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// updateFileSystemForJob
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::updateFileSystemForJob
(std::string fileSystem,
 std::string diskServer,
 u_signed64 fileSize)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_updateFileSystemForJobStatement) {
      m_updateFileSystemForJobStatement =
        createStatement(s_updateFileSystemForJobStatementString);
      m_updateFileSystemForJobStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_updateFileSystemForJobStatement->setString(1, fileSystem);
    m_updateFileSystemForJobStatement->setString(2, diskServer);
    m_updateFileSystemForJobStatement->setDouble(3, fileSize);
    unsigned int nb = m_updateFileSystemForJobStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "updateFileSystemForJob did not do anything.";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateFileSystemForJob."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// archiveSubReq
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::archiveSubReq
(u_signed64 subReqId)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_archiveSubReqStatement) {
    m_archiveSubReqStatement =
      createStatement(s_archiveSubReqStatementString);
    m_archiveSubReqStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_archiveSubReqStatement->setDouble(1, subReqId);
    m_archiveSubReqStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to archive subRequest :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// stageRelease
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::stageRelease
(const u_signed64 fileId, const std::string nsHost)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_stageReleaseStatement) {
      m_stageReleaseStatement =
        createStatement(s_stageReleaseStatementString);
      m_stageReleaseStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_stageReleaseStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_stageReleaseStatement->setDouble(1, fileId);
    m_stageReleaseStatement->setString(2, nsHost);
    unsigned int nb = m_stageReleaseStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "stageRelease : No return code after PL/SQL call.";
      throw ex;
    }
    // In case of EBUSY, throw exception
    int returnCode = m_stageReleaseStatement->getInt(3);
    if (returnCode != 0) {
      castor::exception::Busy e;
      if (returnCode == 1) {
        e.getMessage() << "The file is being migrated.";
      } else {
        e.getMessage() << "There is/are ongoing request(s) on this file.";
      }
      throw e;
    }
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in stageRelease."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// stageRm
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::stageRm
(const u_signed64 fileId, const std::string nsHost)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_stageRmStatement) {
      m_stageRmStatement =
        createStatement(s_stageRmStatementString);
      m_stageRmStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_stageRmStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_stageRmStatement->setDouble(1, fileId);
    m_stageRmStatement->setString(2, nsHost);
    unsigned int nb = m_stageRmStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "stageRm : No return code after PL/SQL call.";
      throw ex;
    }
    // In case of EBUSY, throw exception
    int returnCode = m_stageRmStatement->getInt(3);
    if (returnCode != 0) {
      castor::exception::Busy e;
      if (returnCode == 1) {
        e.getMessage() << "The file is not yet migrated.";
      } else if (returnCode == 2) {
        e.getMessage() << "The file is being replicated.";
      } else {
        e.getMessage() << "The file is being recalled from Tape.";
      }
      throw e;
    }
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in stageRm."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// setFileGCWeight
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::setFileGCWeight
(const u_signed64 fileId, const std::string nsHost, const float weight)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_setFileGCWeightStatement) {
      m_setFileGCWeightStatement =
        createStatement(s_setFileGCWeightStatementString);
      m_setFileGCWeightStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_setFileGCWeightStatement->setFloat(1, weight);
    m_setFileGCWeightStatement->setDouble(2, fileId);
    m_setFileGCWeightStatement->setString(3, nsHost);
    unsigned int nb = m_setFileGCWeightStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "setFileGCWeightStatement : No return code after PL/SQL call.";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in setFileGCWeight."
      << std::endl << e.what();
    throw ex;
  }
}
