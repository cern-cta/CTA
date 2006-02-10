/******************************************************************************
 *                      OraJobSvc.cpp
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
 * @(#)$RCSfile: OraJobSvc.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2006/02/10 15:08:13 $ $Author: sponcec3 $
 *
 * Implementation of the IJobSvc for Oracle
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
#include "castor/db/ora/OraJobSvc.hpp"
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
static castor::SvcFactory<castor::db::ora::OraJobSvc>* s_factoryOraJobSvc =
  new castor::SvcFactory<castor::db::ora::OraJobSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL statement for getUpdateStart
const std::string castor::db::ora::OraJobSvc::s_getUpdateStartStatementString =
  "BEGIN getUpdateStart(:1, :2, :3, :4, :5, :6, :7, :8); END;";

/// SQL statement for putStart
const std::string castor::db::ora::OraJobSvc::s_putStartStatementString =
  "BEGIN putStart(:1, :2, :3, :4, :5); END;";

/// SQL statement for putDoneStart
const std::string castor::db::ora::OraJobSvc::s_putDoneStartStatementString =
  "BEGIN putDoneStartProc(:1, :2, :3, :4); END;";

/// SQL statement for disk2DiskCopyDone
const std::string castor::db::ora::OraJobSvc::s_disk2DiskCopyDoneStatementString =
  "BEGIN disk2DiskCopyDone(:1, :2); END;";

/// SQL statement for prepareForMigration
const std::string castor::db::ora::OraJobSvc::s_prepareForMigrationStatementString =
  "BEGIN prepareForMigration(:1, :2, :3, :4, :5, :6); END;";

/// SQL statement for getUpdateDone
const std::string castor::db::ora::OraJobSvc::s_getUpdateDoneStatementString =
  "BEGIN getUpdateDoneProc(:1); END;";

/// SQL statement for getUpdateFailed
const std::string castor::db::ora::OraJobSvc::s_getUpdateFailedStatementString =
  "BEGIN getUpdateFailedProc(:1); END;";

/// SQL statement for putFailed
const std::string castor::db::ora::OraJobSvc::s_putFailedStatementString =
  "BEGIN putFailedProc(:1); END;";

/// SQL statement for requestToDo
const std::string castor::db::ora::OraJobSvc::s_requestToDoStatementString =
  "BEGIN :1 := 0; DELETE FROM newRequests WHERE type IN (60, 64, 65, 67, 78, 79, 80, 93) AND ROWNUM < 2 RETURNING id INTO :1; END;";

// -----------------------------------------------------------------------
// OraJobSvc
// -----------------------------------------------------------------------
castor::db::ora::OraJobSvc::OraJobSvc(const std::string name) :
  OraCommonSvc(name),
  m_getUpdateStartStatement(0),
  m_putStartStatement(0),
  m_putDoneStartStatement(0),
  m_disk2DiskCopyDoneStatement(0),
  m_prepareForMigrationStatement(0),
  m_getUpdateDoneStatement(0),
  m_getUpdateFailedStatement(0),
  m_putFailedStatement(0),
  m_requestToDoStatement(0) {
}

// -----------------------------------------------------------------------
// ~OraJobSvc
// -----------------------------------------------------------------------
castor::db::ora::OraJobSvc::~OraJobSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraJobSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraJobSvc::ID() {
  return castor::SVC_ORAJOBSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraJobSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    deleteStatement(m_getUpdateStartStatement);
    deleteStatement(m_putStartStatement);
    deleteStatement(m_putDoneStartStatement);
    deleteStatement(m_disk2DiskCopyDoneStatement);
    deleteStatement(m_prepareForMigrationStatement);
    deleteStatement(m_getUpdateDoneStatement);
    deleteStatement(m_getUpdateFailedStatement);
    deleteStatement(m_putFailedStatement);
    deleteStatement(m_requestToDoStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_getUpdateStartStatement = 0;
  m_putStartStatement = 0;
  m_putDoneStartStatement = 0;
  m_disk2DiskCopyDoneStatement = 0;
  m_prepareForMigrationStatement = 0;
  m_getUpdateDoneStatement = 0;
  m_getUpdateFailedStatement = 0;
  m_putFailedStatement = 0;
  m_requestToDoStatement = 0;
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
// getUpdateStart
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::db::ora::OraJobSvc::getUpdateStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem,
 std::list<castor::stager::DiskCopyForRecall*>& sources,
 bool* emptyFile)
  throw (castor::exception::Exception) {
  *emptyFile = false;
  try {
    // Check whether the statements are ok
    if (0 == m_getUpdateStartStatement) {
      m_getUpdateStartStatement =
        createStatement(s_getUpdateStartStatementString);
      m_getUpdateStartStatement->registerOutParam
        (3, oracle::occi::OCCIDOUBLE);
      m_getUpdateStartStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_getUpdateStartStatement->registerOutParam
        (5, oracle::occi::OCCIINT);
      m_getUpdateStartStatement->registerOutParam
        (6, oracle::occi::OCCICURSOR);
      m_getUpdateStartStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
      m_getUpdateStartStatement->registerOutParam
        (8, oracle::occi::OCCIINT);
    }
    // execute the statement and see whether we found something
    m_getUpdateStartStatement->setDouble(1, subreq->id());
    m_getUpdateStartStatement->setDouble(2, fileSystem->id());
    unsigned int nb =
      m_getUpdateStartStatement->executeUpdate();
    if (0 == nb) {
      rollback();
      castor::exception::Internal ex;
      ex.getMessage()
        << "getUpdateStart : unable to schedule SubRequest.";
      throw ex;
    }
    unsigned long euid = m_getUpdateStartStatement->getInt(7);
    unsigned long egid = m_getUpdateStartStatement->getInt(8);
    // Check result
    u_signed64 id
      = (u_signed64)m_getUpdateStartStatement->getDouble(3);
    // If no DiskCopy returned, return
    if (0 == id) {
      cnvSvc()->commit();
      return 0;
    }
    // In case a diskcopy was created in WAITTAPERECALL
    // status, create the associated TapeCopy and Segments
    unsigned int status =
      m_getUpdateStartStatement->getInt(5);
    if (status == 99) {
      // First get the DiskCopy
      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      ad.setTarget(id);
      castor::IObject* iobj = cnvSvc()->createObj(&ad);
      castor::stager::DiskCopy *dc =
        dynamic_cast<castor::stager::DiskCopy*>(iobj);
      if (0 == dc) {
        rollback();
        castor::exception::Internal e;
        e.getMessage() << "Dynamic cast to DiskCopy failed "
                       << "in getUpdateStart";
        delete iobj;
        throw e;
      }
      // Then get the associated CastorFile
      cnvSvc()->fillObj(&ad, iobj, castor::OBJ_CastorFile);
      // create needed TapeCopy(ies) and Segment(s)
      int crSegFailed = createTapeCopySegmentsForRecall(dc->castorFile(), euid, egid);
      if(crSegFailed == -1) {      
          // no valid copy found, set the diskcopy status to failed
          dc->setStatus(castor::stager::DISKCOPY_FAILED);
          subreq->setStatus(castor::stager::SUBREQUEST_FAILED);
          cnvSvc()->updateRep(&ad, dc, false);
          cnvSvc()->updateRep(&ad, subreq, false);
      }          
      // cleanup
      delete dc->castorFile();
      delete dc;
      // commit and return
      cnvSvc()->commit();

      if(crSegFailed == 0) return 0;
      // else throw the exception to the stager_job_service
      castor::exception::Internal e;
      e.getMessage() << "getUpdateStart : no valid copy found on tape";
      throw e;
    }
    // Else create a resulting DiskCopy
    castor::stager::DiskCopy* result =
      new castor::stager::DiskCopy();
    result->setId(id);
    result->setPath(m_getUpdateStartStatement->getString(4));
    if (98 == status) {
      result->setStatus(castor::stager::DISKCOPY_WAITDISK2DISKCOPY);
      *emptyFile = true;
    } else {
      result->setStatus
        ((enum castor::stager::DiskCopyStatusCodes) status);
    }
    if (status == castor::stager::DISKCOPY_WAITDISK2DISKCOPY) {
      try {
        oracle::occi::ResultSet *rs =
          m_getUpdateStartStatement->getCursor(6);
        // Run through the cursor
        oracle::occi::ResultSet::Status status = rs->next();
        while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
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
        rollback();
        if (e.getErrorCode() != 24338) {
          // if not "statement handle not executed"
          // it's really wrong, else, it's normal
          throw e;
        }
      }
    }
    // return
    cnvSvc()->commit();
    return result;
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getUpdateStart."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// putStart
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::db::ora::OraJobSvc::putStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem)
  throw (castor::exception::Exception) {
  castor::IObject *iobj = 0;
  castor::stager::DiskCopy* result = 0;
  try {
    // Check whether the statements are ok
    if (0 == m_putStartStatement) {
      m_putStartStatement =
        createStatement(s_putStartStatementString);
      m_putStartStatement->registerOutParam
        (3, oracle::occi::OCCIDOUBLE);
      m_putStartStatement->registerOutParam
        (4, oracle::occi::OCCIINT);
      m_putStartStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048);
    }
    // execute the statement and see whether we found something
    m_putStartStatement->setDouble(1, subreq->id());
    m_putStartStatement->setDouble(2, fileSystem->id());
    unsigned int nb = m_putStartStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "putStart : unable to schedule SubRequest.";
      throw ex;
    }
    // Get the result
    result = new castor::stager::DiskCopy();
    result->setId((u_signed64)m_putStartStatement->getDouble(3));
    result->setStatus
      ((enum castor::stager::DiskCopyStatusCodes)
       m_putStartStatement->getInt(4));
    result->setPath(m_putStartStatement->getString(5));
    // return
    cnvSvc()->commit();
    return result;
  } catch (oracle::occi::SQLException e) {
    if (0 != result) {
      delete result;
    }
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in putStart."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// putDoneStart
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::db::ora::OraJobSvc::putDoneStart(u_signed64 subreqId)
  throw (castor::exception::Exception) {
  castor::IObject *iobj = 0;
  castor::stager::DiskCopy* result = 0;
  try {
    // Check whether the statements are ok
    if (0 == m_putDoneStartStatement) {
      m_putDoneStartStatement =
        createStatement(s_putDoneStartStatementString);
      m_putDoneStartStatement->registerOutParam
        (2, oracle::occi::OCCIDOUBLE);
      m_putDoneStartStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_putDoneStartStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
    }
    // execute the statement and see whether we found something
    m_putDoneStartStatement->setDouble(1, subreqId);
    unsigned int nb = m_putDoneStartStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "putDoneStart : unable to execute putDoneStart in DB.\n"
        << "Subrequest Id was " << subreqId;
      throw ex;
    }
    // Get the result
    result = new castor::stager::DiskCopy();
    result->setId((u_signed64)m_putDoneStartStatement->getDouble(2));
    result->setStatus
      ((enum castor::stager::DiskCopyStatusCodes)
       m_putDoneStartStatement->getInt(3));
    result->setPath(m_putDoneStartStatement->getString(4));
    // return
    cnvSvc()->commit();
    return result;
  } catch (oracle::occi::SQLException e) {
    if (0 != result) {
      delete result;
    }
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in putDoneStart."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// disk2DiskCopyDone
// -----------------------------------------------------------------------
void castor::db::ora::OraJobSvc::disk2DiskCopyDone
(u_signed64 diskCopyId,
 castor::stager::DiskCopyStatusCodes status)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_disk2DiskCopyDoneStatement) {
      m_disk2DiskCopyDoneStatement =
        createStatement(s_disk2DiskCopyDoneStatementString);
      m_disk2DiskCopyDoneStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_disk2DiskCopyDoneStatement->setDouble(1, diskCopyId);
    m_disk2DiskCopyDoneStatement->setDouble(2, status);
    m_disk2DiskCopyDoneStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in disk2DiskCopyDone."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// prepareForMigration
// -----------------------------------------------------------------------
void castor::db::ora::OraJobSvc::prepareForMigration
(castor::stager::SubRequest *subreq,
 u_signed64 fileSize)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_prepareForMigrationStatement) {
      m_prepareForMigrationStatement =
        createStatement(s_prepareForMigrationStatementString);
      m_prepareForMigrationStatement->setAutoCommit(true);
      m_prepareForMigrationStatement->registerOutParam
        (3, oracle::occi::OCCIDOUBLE);
      m_prepareForMigrationStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_prepareForMigrationStatement->registerOutParam
        (5, oracle::occi::OCCIINT);
      m_prepareForMigrationStatement->registerOutParam
        (6, oracle::occi::OCCIINT);
    }
    // execute the statement and see whether we found something
    m_prepareForMigrationStatement->setDouble(1, subreq->id());
    m_prepareForMigrationStatement->setDouble(2, fileSize);
    unsigned int nb = m_prepareForMigrationStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "prepareForMigration did not return any result.";
      throw ex;
    }
    // collect output
    struct Cns_fileid fileid;
    fileid.fileid =
      (u_signed64)m_prepareForMigrationStatement->getDouble(3);
    std::string nsHost =
      m_prepareForMigrationStatement->getString(4);
    strncpy(fileid.server,
            nsHost.c_str(),
            CA_MAXHOSTNAMELEN);
    unsigned long euid =
      m_prepareForMigrationStatement->getInt(5);
    unsigned long egid =
      m_prepareForMigrationStatement->getInt(6);
    // Update name server
    char cns_error_buffer[512];  /* Cns error buffer */
    if (Cns_seterrbuf(cns_error_buffer,sizeof(cns_error_buffer)) != 0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage()
        << "prepareForMigration : Cns_seterrbuf failed.";
      throw ex;
    }
    if (Cns_setid(euid,egid) != 0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage()
        << "prepareForMigration : Cns_setid failed :"
        << std::endl << cns_error_buffer;
      throw ex;
    }
    if (Cns_setfsize(0, &fileid, fileSize) != 0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage()
        << "prepareForMigration : Cns_setfsize failed :"
        << std::endl << cns_error_buffer;
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in prepareForMigration."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// getUpdateDone
// -----------------------------------------------------------------------
void castor::db::ora::OraJobSvc::getUpdateDone
(u_signed64 subReqId)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_getUpdateDoneStatement) {
    m_getUpdateDoneStatement =
      createStatement(s_getUpdateDoneStatementString);
    m_getUpdateDoneStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_getUpdateDoneStatement->setDouble(1, subReqId);
    m_getUpdateDoneStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to clean Get/Update subRequest :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// getUpdateFailed
// -----------------------------------------------------------------------
void castor::db::ora::OraJobSvc::getUpdateFailed
(u_signed64 subReqId)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_getUpdateFailedStatement) {
    m_getUpdateFailedStatement =
      createStatement(s_getUpdateFailedStatementString);
    m_getUpdateFailedStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_getUpdateFailedStatement->setDouble(1, subReqId);
    m_getUpdateFailedStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to mark subRequest as FAILED :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// putFailed
// -----------------------------------------------------------------------
void castor::db::ora::OraJobSvc::putFailed
(u_signed64 subReqId)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_putFailedStatement) {
    m_putFailedStatement =
      createStatement(s_putFailedStatementString);
    m_putFailedStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_putFailedStatement->setDouble(1, subReqId);
    m_putFailedStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to clean Put subRequest :"
      << std::endl << e.getMessage();
    throw ex;
  }
}


// -----------------------------------------------------------------------
// createTapeCopySegmentsForRecall (private)
// -----------------------------------------------------------------------
int castor::db::ora::OraJobSvc::createTapeCopySegmentsForRecall
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
  int nbNsSegments;
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
  if (useCopyNb == -1) {
    // No valid copy found
	return -1;
    //castor::exception::Internal e;
    //e.getMessage() << "createTapeCopySegmentsForRecall : "
    //               << "No valid copy found";
    //throw e;
  }
  // DB address
  castor::BaseAddress ad;
  ad.setCnvSvcName("OracnvSvc");
  ad.setCnvSvcType(castor::SVC_ORACNV);
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
  // Fill Segment to Tape link and Cleanup
  for (unsigned int i = 0; i < tapeCopy.segments().size(); i++) {
    castor::stager::Segment* seg = tapeCopy.segments()[i];
    cnvSvc()->fillRep(&ad, seg, castor::OBJ_Tape, false);
  }
  while (tapeCopy.segments().size() > 0) {
    delete tapeCopy.segments()[0]->tape();
  }
  cnvSvc()->commit();
  return 0;
}

// -----------------------------------------------------------------------
// requestToDo
// -----------------------------------------------------------------------
castor::stager::Request*
castor::db::ora::OraJobSvc::requestToDo()
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_requestToDoStatement) {
      m_requestToDoStatement =
        createStatement(s_requestToDoStatementString);
      m_requestToDoStatement->registerOutParam
        (1, oracle::occi::OCCIDOUBLE);
      m_requestToDoStatement->setAutoCommit(true);
    }
    // execute the statement
    m_requestToDoStatement->executeUpdate();
    // see whether we've found something
    u_signed64 id = (u_signed64)m_requestToDoStatement->getDouble(1);
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
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in requestToDo."
      << std::endl << e.what();
    throw ex;
  }
}
