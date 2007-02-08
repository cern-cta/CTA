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
 * @(#)$RCSfile: OraJobSvc.cpp,v $ $Revision: 1.18 $ $Release$ $Date: 2007/02/08 07:31:05 $ $Author: gtaur $
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
#include "castor/exception/SegmentNotAccessible.hpp"
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
  "BEGIN prepareForMigration(:1, :2, :3, :4, :5, :6,:7); END;";

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
    // If no DiskCopy returned, we have to wait, hence return
    if (0 == id) {
      cnvSvc()->commit();
      return 0;
    }
    
    unsigned int status =
      m_getUpdateStartStatement->getInt(5);
    castor::stager::DiskCopy* result =
      new castor::stager::DiskCopy();
    result->setId(id);
    result->setPath(m_getUpdateStartStatement->getString(4));
    if (98 == status) {
      result->setStatus(castor::stager::DISKCOPY_WAITDISK2DISKCOPY);
      *emptyFile = true;
    } else {
    result->setStatus((enum castor::stager::DiskCopyStatusCodes) status);
    }

    // A diskcopy was created in WAITDISK2DISKCOPY
    // create a resulting DiskCopy
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
    handleException(e);
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
    handleException(e);
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
    handleException(e);
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
    handleException(e);
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
 u_signed64 fileSize, u_signed64 timeStamp)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_prepareForMigrationStatement) {
      m_prepareForMigrationStatement =
        createStatement(s_prepareForMigrationStatementString);
      m_prepareForMigrationStatement->setAutoCommit(true);
      m_prepareForMigrationStatement->registerOutParam
        (4, oracle::occi::OCCIDOUBLE);
      m_prepareForMigrationStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048);
      m_prepareForMigrationStatement->registerOutParam
        (6, oracle::occi::OCCIINT);
      m_prepareForMigrationStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
    }
    // execute the statement and see whether we found something
    m_prepareForMigrationStatement->setDouble(1, subreq->id());
    m_prepareForMigrationStatement->setDouble(2, fileSize);
    m_prepareForMigrationStatement->setDouble(3, timeStamp);
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
      (u_signed64)m_prepareForMigrationStatement->getDouble(4);
    std::string nsHost =
      m_prepareForMigrationStatement->getString(5);
    strncpy(fileid.server,
            nsHost.c_str(),
            CA_MAXHOSTNAMELEN);
    unsigned long euid =
      m_prepareForMigrationStatement->getInt(6);
    unsigned long egid =
      m_prepareForMigrationStatement->getInt(7);
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
    handleException(e);
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
    handleException(e);
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
    handleException(e);
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
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to clean Put subRequest :"
      << std::endl << e.getMessage();
    throw ex;
  }
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
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in requestToDo."
      << std::endl << e.what();
    throw ex;
  }
}
