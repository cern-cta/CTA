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
#include "castor/stager/Request.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/FileClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/SubRequest.hpp"
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
#include "castor/db/ora/OraJobSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Busy.hpp"
#include "castor/exception/RequestCanceled.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/dlf/Dlf.hpp"
#include "occi.h"
#include "getconfent.h"
#include <Cuuid.h>
#include <string>
#include <sstream>
#include <vector>
#include <Cns_api.h>
#include <vmgr_api.h>
#include <Ctape_api.h>
#include <serrno.h>
#include <string.h>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraJobSvc>
  s_factoryOraJobSvc;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL statement for getStart
const std::string castor::db::ora::OraJobSvc::s_getStartStatementString =
  "BEGIN getStart(:1, :2, :3, :4, :5); END;";

/// SQL statement for putStart
const std::string castor::db::ora::OraJobSvc::s_putStartStatementString =
  "BEGIN putStart(:1, :2, :3, :4); END;";

/// SQL statement for getEnded
const std::string castor::db::ora::OraJobSvc::s_getEndedStatementString =
  "BEGIN getEnded(:1, :2, :3); END;";

/// SQL statement for putEnded
const std::string castor::db::ora::OraJobSvc::s_putEndedStatementString =
  "BEGIN putEnded(:1, :2, :3, :4, :5, :6, :7); END;";

/// SQL statement for firstByteWritten
const std::string castor::db::ora::OraJobSvc::s_firstByteWrittenStatementString =
  "BEGIN firstByteWrittenProc(:1); END;";

// -----------------------------------------------------------------------------
// OraJobSvc
//------------------------------------------------------------------------------
castor::db::ora::OraJobSvc::OraJobSvc(const std::string name) :
  OraCommonSvc(name),
  m_getStartStatement(0),
  m_putStartStatement(0),
  m_getEndedStatement(0),
  m_putEndedStatement(0),
  m_firstByteWrittenStatement(0) {
}

//------------------------------------------------------------------------------
// ~OraJobSvc
//------------------------------------------------------------------------------
castor::db::ora::OraJobSvc::~OraJobSvc() throw() {
  reset();
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraJobSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
unsigned int castor::db::ora::OraJobSvc::ID() {
  return castor::SVC_ORAJOBSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraJobSvc::reset() throw() {
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if (m_getStartStatement) deleteStatement(m_getStartStatement);
    if (m_putStartStatement) deleteStatement(m_putStartStatement);
    if (m_getEndedStatement) deleteStatement(m_getEndedStatement);
    if (m_putEndedStatement) deleteStatement(m_putEndedStatement);
    if (m_firstByteWrittenStatement) deleteStatement(m_firstByteWrittenStatement);
  } catch (castor::exception::Exception& ignored) {};
  // Now reset all pointers to 0
  m_getStartStatement = 0;
  m_putStartStatement = 0;
  m_getEndedStatement = 0;
  m_putEndedStatement = 0;
  m_firstByteWrittenStatement = 0;
}

//------------------------------------------------------------------------------
// getUpdateStart
//------------------------------------------------------------------------------
std::string
castor::db::ora::OraJobSvc::getUpdateStart
(castor::stager::SubRequest* subreq,
 std::string diskServerName,
 std::string mountPoint,
 bool* emptyFile,
 u_signed64,
 const std::string)
  throw (castor::exception::Exception) {
  *emptyFile = false;
  try {
    // Check whether the statements are ok
    if (0 == m_getStartStatement) {
      m_getStartStatement =
        createStatement(s_getStartStatementString);
      m_getStartStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_getStartStatement->registerOutParam
        (5, oracle::occi::OCCIINT);
      m_getStartStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_getStartStatement->setDouble(1, subreq->id());
    m_getStartStatement->setString(2, diskServerName);
    m_getStartStatement->setString(3, mountPoint);
    unsigned int nb = m_getStartStatement->executeUpdate();
    if (0 == nb) {
      rollback();
      castor::exception::Exception ex;
      ex.getMessage()
        << "getUpdateStart : unable to schedule SubRequest.";
      throw ex;
    }

    // get results and return
    *emptyFile = m_getStartStatement->getInt(5) == 1;
    return m_getStartStatement->getString(4);
  } catch (oracle::occi::SQLException e) {
    // Application specific errors
    if (e.getErrorCode() == 20114) {
      castor::exception::RequestCanceled ex;
      std::string error = e.what();
      ex.getMessage() << error.substr(error.find("ORA-") + 11,
                                      error.find("ORA-", 4) - 12);
      throw ex;
    }
    handleException(e);
    castor::exception::Exception ex;
    ex.getMessage()
      << "Error caught in getUpdateStart."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// putStart
//------------------------------------------------------------------------------
std::string
castor::db::ora::OraJobSvc::putStart
(castor::stager::SubRequest* subreq,
 std::string diskServerName,
 std::string mountPoint,
 u_signed64,
 const std::string)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_putStartStatement) {
      m_putStartStatement =
        createStatement(s_putStartStatementString);
      m_putStartStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_putStartStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_putStartStatement->setDouble(1, subreq->id());
    m_putStartStatement->setString(2, diskServerName);
    m_putStartStatement->setString(3, mountPoint);
    unsigned int nb = m_putStartStatement->executeUpdate();
    if (0 == nb) {
      rollback();
      castor::exception::Exception ex;
      ex.getMessage()
        << "putStart : unable to schedule SubRequest.";
      throw ex;
    }
    // return diskCopy Path
    return m_putStartStatement->getString(4);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    // Application specific errors
    if (e.getErrorCode() == 20104) {
      castor::exception::RequestCanceled ex;
      std::string error = e.what();
      ex.getMessage() << error.substr(error.find("ORA-") + 11,
                                      error.find("ORA-", 4) - 12);
      throw ex;
    }
    castor::exception::Exception ex;
    ex.getMessage()
      << "Error caught in putStart."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// prepareForMigration
//------------------------------------------------------------------------------
void castor::db::ora::OraJobSvc::prepareForMigration
(u_signed64 subReqId,
 u_signed64 fileSize,
 u_signed64 timeStamp,
 u_signed64,
 const std::string,
 const std::string csumtype,
 const std::string csumvalue)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_putEndedStatement) {
      m_putEndedStatement =
        createStatement(s_putEndedStatementString);
      m_putEndedStatement->registerOutParam
        (6, oracle::occi::OCCIINT);
      m_putEndedStatement->registerOutParam
        (7, oracle::occi::OCCISTRING, 2048);
      m_putEndedStatement->setAutoCommit(true);
    }
    // Execute the statement
    // all error handling and logging is done in PL/SQL
    m_putEndedStatement->setDouble(1, subReqId);
    m_putEndedStatement->setDouble(2, fileSize);
    m_putEndedStatement->setDouble(3, timeStamp);
    m_putEndedStatement->setString(4, csumtype);
    m_putEndedStatement->setString(5, csumvalue);
    m_putEndedStatement->setInt(6, 0);
    //m_putEndedStatement->setString(7, "");
    m_putEndedStatement->executeUpdate();

    // Check for errors
    int returnCode = m_putEndedStatement->getInt(6);
    if (returnCode) {
      castor::exception::Exception e(returnCode);
      e.getMessage() << "prepareForMigration failed: "
                     << m_putEndedStatement->getString(7);
      throw e;
    }

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Exception ex;
    ex.getMessage()
      << "Error caught in prepareForMigration."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getUpdateDone
//------------------------------------------------------------------------------
void castor::db::ora::OraJobSvc::getUpdateDone
(u_signed64 subReqId,
 u_signed64,
 const std::string)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_getEndedStatement) {
    m_getEndedStatement =
      createStatement(s_getEndedStatementString);
    m_getEndedStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  try {
    m_getEndedStatement->setDouble(1, subReqId);
    m_getEndedStatement->setInt(2, 0);
    m_getEndedStatement->setString(3, "");
    m_getEndedStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Exception ex;
    ex.getMessage()
      << "Unable to clean Get/Update subRequest :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getUpdateFailed
//------------------------------------------------------------------------------
void castor::db::ora::OraJobSvc::getUpdateFailed
(u_signed64 subReqId,
 u_signed64,
 const std::string)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_getEndedStatement) {
    m_getEndedStatement =
      createStatement(s_getEndedStatementString);
    m_getEndedStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  try {
    m_getEndedStatement->setDouble(1, subReqId);
    m_getEndedStatement->setInt(2, 1015);
    m_getEndedStatement->setString(3, "Job terminated with failure");
    m_getEndedStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Exception ex;
    ex.getMessage()
      << "Unable to mark subRequest as FAILED :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// putEnded
//------------------------------------------------------------------------------
void castor::db::ora::OraJobSvc::putFailed
(u_signed64 subReqId,
 u_signed64,
 const std::string)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_putEndedStatement) {
    m_putEndedStatement =
      createStatement(s_putEndedStatementString);
    m_putEndedStatement->registerOutParam
      (6, oracle::occi::OCCIINT);
    m_putEndedStatement->registerOutParam
      (7, oracle::occi::OCCISTRING, 2048);
    m_putEndedStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  try {
    m_putEndedStatement->setDouble(1, subReqId);
    m_putEndedStatement->setDouble(2, 0);
    m_putEndedStatement->setDouble(3, 0);
    m_putEndedStatement->setString(4, "");
    m_putEndedStatement->setString(5, "");
    m_putEndedStatement->setInt(6, 1015);
    m_putEndedStatement->setString(7, "Job terminated with failure");
    m_putEndedStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Exception ex;
    ex.getMessage()
      << "Unable to clean Put subRequest :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// firstByteWritten
//------------------------------------------------------------------------------
void castor::db::ora::OraJobSvc::firstByteWritten
(u_signed64 subRequestId,
 u_signed64,
 const std::string)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_firstByteWrittenStatement) {
      m_firstByteWrittenStatement =
        createStatement(s_firstByteWrittenStatementString);
      m_firstByteWrittenStatement->setAutoCommit(true);
    }
    // execute the statement
    m_firstByteWrittenStatement->setDouble(1, subRequestId);
    m_firstByteWrittenStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    // Application specific errors
    if (e.getErrorCode() == 20106) {
      castor::exception::Busy ex;
      std::string error = e.what();
      ex.getMessage() << error.substr(error.find("ORA-") + 11,
                                      error.find("ORA-", 4) - 12);
      throw ex;
    }
    castor::exception::Exception ex;
    ex.getMessage()
      << "Error caught in firstByteWritten."
      << std::endl << e.what();
    throw ex;
  }
}
