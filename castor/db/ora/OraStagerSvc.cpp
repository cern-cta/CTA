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
 * @(#)$RCSfile: OraStagerSvc.cpp,v $ $Revision: 1.197 $ $Release$ $Date: 2007/04/04 13:47:49 $ $Author: sponcec3 $
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
  "UPDATE SubRequest SET status = 10 WHERE decode(status,7,status,null) = 7 AND ROWNUM < 2 RETURNING id, retryCounter, fileName, protocol, xsize, priority,  status, modeBits, flags INTO :1, :2, :3, :4, :5 ,:6 , :7, :8, :9";

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

/// SQL statement for selectDiskPool
const std::string castor::db::ora::OraStagerSvc::s_selectDiskPoolStatementString =
  "SELECT id FROM DiskPool WHERE name = :1";

/// SQL statement for selectTapePool
const std::string castor::db::ora::OraStagerSvc::s_selectTapePoolStatementString =
  "SELECT id FROM TapePool WHERE name = :1";

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
  m_updateFileSystemForJobStatement(0),
  m_archiveSubReqStatement(0),
  m_stageReleaseStatement(0),
  m_stageRmStatement(0),
  m_setFileGCWeightStatement(0),
  m_selectDiskPoolStatement(0),
  m_selectTapePoolStatement(0) {
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
    deleteStatement(m_updateFileSystemForJobStatement);
    deleteStatement(m_archiveSubReqStatement);
    deleteStatement(m_stageReleaseStatement);
    deleteStatement(m_stageRmStatement);
    deleteStatement(m_setFileGCWeightStatement);
    deleteStatement(m_selectDiskPoolStatement);
    deleteStatement(m_selectTapePoolStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_subRequestToDoStatement = 0;
  m_subRequestFailedToDoStatement = 0;
  m_isSubRequestToScheduleStatement = 0;
  m_selectCastorFileStatement = 0;
  m_updateAndCheckSubRequestStatement = 0;
  m_recreateCastorFileStatement = 0;
  m_updateFileSystemForJobStatement = 0;
  m_archiveSubReqStatement = 0;
  m_stageReleaseStatement = 0;
  m_stageRmStatement = 0;
  m_setFileGCWeightStatement = 0;
  m_selectDiskPoolStatement = 0;
  m_selectTapePoolStatement = 0;
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
        << "protocol, xsize, priority, status, modeBits, flags, subReqId"
        << " INTO :1, :2, :3, :4, :5 ,:6 , :7, :8, :9, :10 ";

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
        (10, oracle::occi::OCCISTRING, 2048);
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
    result->setSubreqId(m_subRequestToDoStatement->getString(10));
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

// -----------------------------------------------------------------------
// isSubRequestToSchedule
// -----------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::isSubRequestToSchedule
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

    if (1 == rc) { /* Reschedule without real Disk2Disk copy */
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
        handleException(e);
        if (e.getErrorCode() != 24338) {
          // if not "statement handle not executed"
          // it's really wrong, else, it's normal
          throw e;
        }
      }
    }
    else if(3 == rc) rc = 1; /* do a real Disk2DiskCopy, 
                              * there are no candidates in this context
                              * (but in another)
                              */

    return rc; /* 2: TapeRecall 
                * 0: no scheduling
                */ 

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in isSubRequestToSchedule."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// createRecallCandidate
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::createRecallCandidate
(castor::stager::SubRequest* subreq,
 unsigned long euid,
 unsigned long egid)
  throw (castor::exception::Exception) {

  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  ad.setTarget(subreq->id());
  castor::stager::DiskCopy* dc = NULL;
 
  try {
      castor::stager::CastorFile* cf = subreq->castorFile();
      // create DiskCopy
      dc = new castor::stager::DiskCopy();
      dc->setCastorFile(cf);
      
      /* we need to create this object now, so we have the DB id for
       * the following operation
       */
      cnvSvc()->createRep(&ad, dc, false);
      cnvSvc()->fillRep(&ad, dc, OBJ_CastorFile, false);
      // build the path, since we now have the DB id from the DiskCopy;
      // note that this line duplicates the buildPathFromFileId PL/SQL
      // procedure, but we don't want to call Oracle only for it
      std::ostringstream spath;
      if ( cf->fileId() % 100 < 10 )
        spath << "0";
      spath << cf->fileId() % 100 << "/" << cf->fileId()
            << "@" << cf->nsHost() << "." << dc->id();

      // set the diskcopy to WAITTAPERECALL and the creationtime 
      dc->setPath(spath.str());
      dc->setStatus(castor::stager::DISKCOPY_WAITTAPERECALL);
      dc->setCreationTime(time(NULL));
      cnvSvc()->updateRep(&ad, dc, false);

      // we link the subreq with the diskcopy
      subreq->setDiskcopy(dc);
      cnvSvc()->fillRep(&ad, subreq, OBJ_DiskCopy, false);
      
      // set SubRequest to WAITTAPERECALL
      subreq->setStatus(castor::stager::SUBREQUEST_WAITTAPERECALL);
      cnvSvc()->updateRep(&ad, subreq, false); 
      
      // create needed TapeCopy(ies) and Segment(s)
    
      createTapeCopySegmentsForRecall(cf, euid, egid);
      cnvSvc()->commit();
      subreq->setDiskcopy(NULL);
      delete dc; 
    }catch (castor::exception::Exception forward){
        // no valid tape copy found, in such a case, we rollback the created DiskCopy
        // and set the subrequest to failed. The original Message is forwarded
    try{
  
	cnvSvc()->rollback();
	subreq->setDiskcopy(0);
	delete dc;
        subreq->setStatus(castor::stager::SUBREQUEST_FAILED);
        cnvSvc()->updateRep(&ad, subreq, true);
    }catch(castor::exception::Exception forward2){
        castor::exception::Internal ex2;
        ex2.getMessage() << "Exception in try-catch of  createRecallCandidate"
        << std::endl << forward2.getMessage().str();
        throw ex2;
    }
  
    castor::exception::Internal ex;
    ex.getMessage() << "Exception in createRecallCandidate"
    << std::endl << forward.getMessage().str();
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
    handleException(e);
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
    handleException(e);
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
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in recreateCastorFile."
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
    handleException(e);
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
  try {
    m_archiveSubReqStatement->setDouble(1, subReqId);
    m_archiveSubReqStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
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
    handleException(e);
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
    handleException(e);
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
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in setFileGCWeight."
      << std::endl << e.what();
    throw ex;
  }
}


// -----------------------------------------------------------------------
// validNsSegment
// -----------------------------------------------------------------------
int validNsSegment(struct Cns_segattrs *nsSegment) {
  if ((0 == nsSegment) || (*nsSegment->vid == '\0') ||
      (nsSegment->s_status != '-')) {
    return 0;
  }
  struct vmgr_tape_info vmgrTapeInfo;
  int rc = vmgr_querytape(nsSegment->vid,nsSegment->side,&vmgrTapeInfo,0);
  if (-1 == rc) return 0;
  if (((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
      ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ||
      ((vmgrTapeInfo.status & EXPORTED) == EXPORTED)) {
    return 0;
  }
  return 1;
}

// -----------------------------------------------------------------------
// invalidateAllSegmentsForCopy
// -----------------------------------------------------------------------
void invalidateAllSegmentsForCopy(int copyNb,
                                  struct Cns_segattrs * nsSegmentArray,
                                  int nbNsSegments) {
  if ((copyNb < 0) || (nbNsSegments <= 0) || (nsSegmentArray == 0)) {
    return;
  }
  for (int i = 0; i < nbNsSegments; i++) {
    if (nsSegmentArray[i].copyno == copyNb) {
      nsSegmentArray[i].s_status = NS_SEGMENT_NOTOK;
    }
  }
  return;
}

// -----------------------------------------------------------------------
// compareSegments
// -----------------------------------------------------------------------
// Helper method to compare segments
extern "C" {
  int compareSegments
  (const void* arg1, const void* arg2) {
    struct Cns_segattrs *segAttr1 = (struct Cns_segattrs *)arg1;
    struct Cns_segattrs *segAttr2 = (struct Cns_segattrs *)arg2;
    int rc = 0;
    if ( segAttr1->fsec < segAttr2->fsec ) rc = -1;
    if ( segAttr1->fsec > segAttr2->fsec ) rc = 1;
    return rc;
  }
}

// -----------------------------------------------------------------------
// createTapeCopySegmentsForRecall (private)
// -----------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::createTapeCopySegmentsForRecall
(castor::stager::CastorFile *castorFile,
 unsigned long euid,
 unsigned long egid)
  throw (castor::exception::Exception) {
  // check argument
  if (0 == castorFile) {
    castor::exception::Internal e;
    e.getMessage() << "createTapeCopySegmentsForRecall "
                   << "called with null argument";
    throw e;
  }
  // check nsHost name length
  if (castorFile->nsHost().length() > CA_MAXHOSTNAMELEN) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "createTapeCopySegmentsForRecall "
                   << "name server host has too long name";
    throw e;
  }
  // Prepare access to name server : avoid log and set uid
  char cns_error_buffer[512];  /* Cns error buffer */
  if (Cns_seterrbuf(cns_error_buffer,sizeof(cns_error_buffer)) != 0) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "createTapeCopySegmentsForRecall : Cns_seterrbuf failed.";
    throw ex;
  }
  if (Cns_setid(euid,egid) != 0) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "createTapeCopySegmentsForRecall : Cns_setid failed :"
      << std::endl << cns_error_buffer;
    throw ex;
  }
  // Get segments for castorFile
  struct Cns_fileid fileid;
  fileid.fileid = castorFile->fileId();
  strncpy(fileid.server,
          castorFile->nsHost().c_str(),
          CA_MAXHOSTNAMELEN);
  struct Cns_segattrs *nsSegmentAttrs = 0;
  int nbNsSegments=0;

  int rc = Cns_getsegattrs
    (0, &fileid, &nbNsSegments, &nsSegmentAttrs);
  if (-1 == rc) {
    castor::exception::Exception e(serrno);
    e.getMessage() << "createTapeCopySegmentsForRecall : "
                   << "Cns_getsegattrs failed";
    throw e;
  }
  // Sort segments
  qsort((void *)nsSegmentAttrs,
        (size_t)nbNsSegments,
        sizeof(struct Cns_segattrs),
        compareSegments);
  // Find a valid copy
  int useCopyNb = -1;
  for (int i = 0; i < nbNsSegments; i++) {
    if (validNsSegment(&nsSegmentAttrs[i]) == 0) {
      invalidateAllSegmentsForCopy(nsSegmentAttrs[i].copyno,
                                   nsSegmentAttrs,
                                   nbNsSegments);
      continue;
    }
    /*
     * This segment is valid. Before we can decide to use
     * it we must check all other segments for the same copy
     */
    useCopyNb = nsSegmentAttrs[i].copyno;
    for (int j = 0; j < nbNsSegments; j++) {
      if (j == i) continue;
      if (nsSegmentAttrs[j].copyno != useCopyNb) continue;
      if (0 == validNsSegment(&nsSegmentAttrs[j])) {
        // This copy was no good
        invalidateAllSegmentsForCopy(nsSegmentAttrs[i].copyno,
                                     nsSegmentAttrs,
                                     nbNsSegments);
        useCopyNb = -1;
        break;
      }
    }
  }
  if (useCopyNb == -1) {  //something went bad.
    
    if (nsSegmentAttrs != NULL ) free(nsSegmentAttrs); 

    // No valid copy found
    //	return -1;
    castor::exception::Internal e;
    e.getMessage() << "createTapeCopySegmentsForRecall : "
                   << "No valid copy found";
    throw e;
  }

  // DB address
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  // create TapeCopy

  castor::stager::TapeCopy tapeCopy;
  tapeCopy.setCopyNb(useCopyNb);
  tapeCopy.setStatus(castor::stager::TAPECOPY_TOBERECALLED);
  tapeCopy.setCastorFile(castorFile);
  castorFile->addTapeCopies(&tapeCopy);
  cnvSvc()->fillRep(&ad, castorFile,
                    castor::OBJ_TapeCopy, false);
  // Go through Segments
  u_signed64 totalSize = 0;
  for (int i = 0; i < nbNsSegments; i++) {
    // Only deal with segments of the copy we choose
    if (nsSegmentAttrs[i].copyno != useCopyNb) continue;
    // create Segment
    castor::stager::Segment *segment =
      new castor::stager::Segment();
    segment->setBlockId0(nsSegmentAttrs[i].blockid[0]);
    segment->setBlockId1(nsSegmentAttrs[i].blockid[1]);
    segment->setBlockId2(nsSegmentAttrs[i].blockid[2]);
    segment->setBlockId3(nsSegmentAttrs[i].blockid[3]);
    segment->setFseq(nsSegmentAttrs[i].fseq);
    segment->setOffset(totalSize);
    segment->setStatus(castor::stager::SEGMENT_UNPROCESSED);
    totalSize += nsSegmentAttrs[i].segsize;
    // get tape for this segment
    castor::stager::Tape *tape =
      selectTape(nsSegmentAttrs[i].vid,
                 nsSegmentAttrs[i].side,
                 WRITE_DISABLE);
    switch (tape->status()) {
    case castor::stager::TAPE_UNUSED:
    case castor::stager::TAPE_FINISHED:
    case castor::stager::TAPE_FAILED:
    case castor::stager::TAPE_UNKNOWN:
      tape->setStatus(castor::stager::TAPE_PENDING);
      break;
    default:
      break;
    }
    cnvSvc()->updateRep(&ad, tape, false);
    // Link Tape with Segment
    segment->setTape(tape);
    tape->addSegments(segment);
    // Link Segment with TapeCopy
    segment->setCopy(&tapeCopy);
    tapeCopy.addSegments(segment);
  }
  // create Segments in DataBase
  cnvSvc()->fillRep(&ad, &tapeCopy, castor::OBJ_Segment, false);
  // Fill Segment to Tape link
  for (unsigned i = 0; i < tapeCopy.segments().size(); i++) {
    castor::stager::Segment* seg = tapeCopy.segments()[i];
    cnvSvc()->fillRep(&ad, seg, castor::OBJ_Tape, false);
  }
  // cleanup

  for(unsigned i = 0;i<tapeCopy.segments().size();i++)
    delete tapeCopy.segments()[i]->tape(); 
 
  if (nsSegmentAttrs != NULL) free(nsSegmentAttrs);

  return 0;
}

// -----------------------------------------------------------------------
// selectDiskPool
// -----------------------------------------------------------------------
castor::stager::DiskPool*
castor::db::ora::OraStagerSvc::selectDiskPool
(const std::string name)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectDiskPoolStatement) {
    m_selectDiskPoolStatement =
      createStatement(s_selectDiskPoolStatementString);
  }
  // Execute statement and get result
  try {
    m_selectDiskPoolStatement->setString(1, name);
    oracle::occi::ResultSet *rset = m_selectDiskPoolStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectDiskPoolStatement->closeResultSet(rset);
      return 0;
    }
    // Found the DiskPool, so create it in memory
    castor::stager::DiskPool* result =
      new castor::stager::DiskPool();
    result->setId((u_signed64)rset->getDouble(1));
    m_selectDiskPoolStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select DiskPool by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// selectTapePool
// -----------------------------------------------------------------------
castor::stager::TapePool*
castor::db::ora::OraStagerSvc::selectTapePool
(const std::string name)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectTapePoolStatement) {
    m_selectTapePoolStatement =
      createStatement(s_selectTapePoolStatementString);
  }
  // Execute statement and get result
  try {
    m_selectTapePoolStatement->setString(1, name);
    oracle::occi::ResultSet *rset = m_selectTapePoolStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectTapePoolStatement->closeResultSet(rset);
      return 0;
    }
    // Found the TapePool, so create it in memory
    castor::stager::TapePool* result =
      new castor::stager::TapePool();
    result->setId((u_signed64)rset->getDouble(1));
    m_selectTapePoolStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select TapePool by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}
