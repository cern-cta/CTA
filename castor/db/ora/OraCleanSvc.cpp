/******************************************************************************
 *                      OraCleanSvc.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: OraCleanSvc.cpp,v $ $Author: waldron $
 *
 * @author Giulia Taurelli
 *****************************************************************************/

// Include Files
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/IClient.hpp"
#include "castor/db/ora/OraCleanSvc.hpp"
#include "castor/exception/InvalidArgument.hpp"
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
#include <serrno.h>


// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraCleanSvc>* s_factoryOraCleanSvc =
  new castor::SvcFactory<castor::db::ora::OraCleanSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL statement for function removeOutOfDateRequests
const std::string castor::db::ora::OraCleanSvc::s_removeOutOfDateRequestsString=
  "BEGIN deleteOutOfDateRequests(:1); END;";

/// SQL statement for removeArchivedRequests function
const std::string castor::db::ora::OraCleanSvc::s_removeArchivedRequestsString=
  "BEGIN deleteArchivedRequests(:1); END;";

/// SQL statement for function removeFailedDiskCopies
const std::string castor::db::ora::OraCleanSvc::s_removeFailedDiskCopiesString=
  "BEGIN deleteFailedDiskCopies(:1); END;";

/// SQL statement for function removeOutOfDateStageOutDCs
const std::string castor::db::ora::OraCleanSvc::s_removeOutOfDateStageOutDCsString=
  "BEGIN deleteOutOfDateStageOutDCs(:1, :2, :3); END;";

/// SQL statement for function removeOutOfDateRecallDCs
const std::string castor::db::ora::OraCleanSvc::s_removeOutOfDateRecallDCsString=
  "BEGIN deleteOutOfDateRecallDCs(:1, :2); END;";

// -----------------------------------------------------------------------
// OraCleanSvc
// -----------------------------------------------------------------------

castor::db::ora::OraCleanSvc::OraCleanSvc(const std::string name) :
  OraCommonSvc(name),
  m_removeOutOfDateRequestsStatement(0),
  m_removeArchivedRequestsStatement(0),
  m_removeFailedDiskCopiesStatement(0),
  m_removeOutOfDateStageOutDCsStatement(0),
  m_removeOutOfDateRecallDCsStatement(0) {}

// -----------------------------------------------------------------------
// ~OraCleanSvc
// -----------------------------------------------------------------------
castor::db::ora::OraCleanSvc::~OraCleanSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraCleanSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraCleanSvc::ID() {
  return castor::SVC_ORACLEANSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraCleanSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if (m_removeOutOfDateRequestsStatement) deleteStatement(m_removeOutOfDateRequestsStatement);
    if (m_removeArchivedRequestsStatement) deleteStatement(m_removeArchivedRequestsStatement);
    if (m_removeFailedDiskCopiesStatement) deleteStatement(m_removeFailedDiskCopiesStatement);
    if (m_removeOutOfDateStageOutDCsStatement) deleteStatement(m_removeOutOfDateStageOutDCsStatement);
    if (m_removeOutOfDateRecallDCsStatement) deleteStatement(m_removeOutOfDateRecallDCsStatement);
  } catch (oracle::occi::SQLException e) {
    // "Cleaning Service not available"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_ORACLELIB + 7, 1, params);
  };
  // Now reset all pointers to 0
  m_removeOutOfDateRequestsStatement = 0;
  m_removeArchivedRequestsStatement = 0;
  m_removeFailedDiskCopiesStatement=0;
  m_removeOutOfDateStageOutDCsStatement=0;
  m_removeOutOfDateRecallDCsStatement=0;
}

// -----------------------------------------------------------------------
// removeOutOfDateRequests
// -----------------------------------------------------------------------
void castor::db::ora::OraCleanSvc::removeOutOfDateRequests(int timeout)
  throw (castor::exception::Exception) {
  try {
    if (0 == m_removeOutOfDateRequestsStatement) {
      m_removeOutOfDateRequestsStatement =
        createStatement(s_removeOutOfDateRequestsString);
      m_removeOutOfDateRequestsStatement->setAutoCommit(true);
    }
    // execute the statement
    m_removeOutOfDateRequestsStatement->setInt(1, timeout);
    unsigned int nb = m_removeOutOfDateRequestsStatement->executeUpdate();
    if (nb == 0) {
      rollback();
      castor::exception::NoEntry e;
      e.getMessage() << "deleteOutOfDateRequests function not found";
      throw e;
    }
    // "Cleaning of out of date requests done"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, DLF_BASE_ORACLELIB + 1);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    // "Cleaning of out of date requests failed"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message",e.getMessage())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_ORACLELIB + 4, 1, params);
    throw ex;
  }
}

// -----------------------------------------------------------------------
// removeArchivedRequests
// -----------------------------------------------------------------------
void castor::db::ora::OraCleanSvc::removeArchivedRequests(int timeout)
  throw (castor::exception::Exception) {
  try {
    if (0 == m_removeArchivedRequestsStatement) {
      m_removeArchivedRequestsStatement =
        createStatement(s_removeArchivedRequestsString);
      m_removeArchivedRequestsStatement->setAutoCommit(true);
    }
    // execute the statement
    m_removeArchivedRequestsStatement->setInt(1, timeout);
    unsigned int nb = m_removeArchivedRequestsStatement->executeUpdate();
    if (nb == 0) {
      rollback();
      castor::exception::NoEntry e;
      e.getMessage() << "deleteArchivedRequests function not found";
      throw e;
    }
    // "Cleaning of archived requests done"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, DLF_BASE_ORACLELIB + 3);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    // "Cleaning of archived requests failed"
    castor::dlf::Param params[] =
      {castor::dlf::Param(castor::dlf::Param("Message",e.getMessage()))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_ORACLELIB + 6, 1, params);
    throw ex;
  }
}

// -----------------------------------------------------------------------
// removeFailedDiskCopies
// -----------------------------------------------------------------------
void castor::db::ora::OraCleanSvc::removeFailedDiskCopies(int timeout)
  throw (castor::exception::Exception) {
  try {
    if (0 == m_removeFailedDiskCopiesStatement) {
      m_removeFailedDiskCopiesStatement =
        createStatement(s_removeFailedDiskCopiesString);
      m_removeFailedDiskCopiesStatement->setAutoCommit(true);
    }
    // execute the statement
    m_removeFailedDiskCopiesStatement->setInt(1, timeout);
    unsigned int nb = m_removeFailedDiskCopiesStatement->executeUpdate();
    if (nb == 0) {
      rollback();
      castor::exception::NoEntry e;
      e.getMessage() << "deleteFailedDiskCopies function not found";
      throw e;
    }
    // "Cleaning of failed diskCopies done"
    castor::dlf::Param logParams[] =
      {castor::dlf::Param("Message", "Removed out of date request.")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, DLF_BASE_ORACLELIB + 2, 1, logParams);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    // "Cleaning of failed diskCopies failed"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message",e.getMessage())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_ORACLELIB + 5, 1, params);
    throw ex;
  }
}

// -----------------------------------------------------------------------
// removeOutOfDateStageOutDCs
// -----------------------------------------------------------------------
void castor::db::ora::OraCleanSvc::removeOutOfDateStageOutDCs(int timeout)
  throw (castor::exception::Exception) {
  try {
    if (0 == m_removeOutOfDateStageOutDCsStatement) {
      m_removeOutOfDateStageOutDCsStatement =
        createStatement(s_removeOutOfDateStageOutDCsString);
      m_removeOutOfDateStageOutDCsStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR);
      m_removeOutOfDateStageOutDCsStatement->registerOutParam
        (3, oracle::occi::OCCICURSOR);
    }
    // execute the statement
    m_removeOutOfDateStageOutDCsStatement->setInt(1, timeout);
    unsigned int nb = m_removeOutOfDateStageOutDCsStatement->executeUpdate();
    if (nb == 0) {
      rollback();
      castor::exception::NoEntry e;
      e.getMessage() << "deleteOutOfDateStageOutDCs function not found";
      throw e;
    }
    try {
      // Run through the files that were dropped
      struct Cns_fileid fileid;
      memset(&fileid, '\0', sizeof(Cns_fileid));
      oracle::occi::ResultSet *rs =
        m_removeOutOfDateStageOutDCsStatement->getCursor(2);
      oracle::occi::ResultSet::Status status = rs->next();
      while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
        // Just log it
        fileid.fileid = (u_signed64)rs->getDouble(1);
        strncpy(fileid.server, rs->getString(2).c_str(), CA_MAXHOSTNAMELEN);
        // "File was dropped by Cleaning Daemon"
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, DLF_BASE_ORACLELIB + 12, 0, 0, &fileid);
        status = rs->next();
      }
      m_removeOutOfDateStageOutDCsStatement->closeResultSet(rs);
      // Run through the files for which a putDone was issued
      oracle::occi::ResultSet *rs2 =
        m_removeOutOfDateStageOutDCsStatement->getCursor(3);
      oracle::occi::ResultSet::Status status2 = rs2->next();
      while (status2 == oracle::occi::ResultSet::DATA_AVAILABLE) {
        // Just log it
        fileid.fileid = (u_signed64)rs2->getDouble(1);
        strncpy(fileid.server, rs2->getString(2).c_str(), CA_MAXHOSTNAMELEN);
        // "PutDone enforced by Cleaning Daemon"
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, DLF_BASE_ORACLELIB + 13, 0, 0, &fileid);
        status2 = rs2->next();
      }
      m_removeOutOfDateStageOutDCsStatement->closeResultSet(rs2);
    } catch (oracle::occi::SQLException e) {
      handleException(e);
      if (e.getErrorCode() != 24338) {
        // if not "statement handle not executed"
        // it's really wrong, else, it's normal
        throw e;
      }
    }
    commit();
    // "Cleaning of out of date STAGEOUT diskCopies done"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, DLF_BASE_ORACLELIB + 8);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    // "Cleaning of out of date STAGEOUT diskCopies done"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message",e.getMessage())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_ORACLELIB + 10, 1, params);
    throw ex;
  }
}

// -----------------------------------------------------------------------
// removeOutOfDateRecallDCs
// -----------------------------------------------------------------------
void castor::db::ora::OraCleanSvc::removeOutOfDateRecallDCs(int timeout)
  throw (castor::exception::Exception) {
  try {
    if (0 == m_removeOutOfDateRecallDCsStatement) {
      m_removeOutOfDateRecallDCsStatement =
        createStatement(s_removeOutOfDateRecallDCsString);
      m_removeOutOfDateRecallDCsStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR);
    }
    // execute the statement
    m_removeOutOfDateRecallDCsStatement->setInt(1, timeout);
    unsigned int nb = m_removeOutOfDateRecallDCsStatement->executeUpdate();
    if (nb == 0) {
      rollback();
      castor::exception::NoEntry e;
      e.getMessage() << "deleteOutOfDateRecallDCs function not found";
      throw e;
    }
    try {
      // Run through the recalls that were dropped
      struct Cns_fileid fileid;
      memset(&fileid, '\0', sizeof(Cns_fileid));
      oracle::occi::ResultSet *rs =
        m_removeOutOfDateRecallDCsStatement->getCursor(2);
      oracle::occi::ResultSet::Status status = rs->next();
      while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
        // Just log it
        fileid.fileid = (u_signed64)rs->getDouble(1);
        strncpy(fileid.server, rs->getString(2).c_str(), CA_MAXHOSTNAMELEN);
        // "Recall canceled by Cleaning Daemon"
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, DLF_BASE_ORACLELIB + 14, 0, 0, &fileid);
        status = rs->next();
      }
      m_removeOutOfDateRecallDCsStatement->closeResultSet(rs);
    } catch (oracle::occi::SQLException e) {
      handleException(e);
      if (e.getErrorCode() != 24338) {
        // if not "statement handle not executed"
        // it's really wrong, else, it's normal
        throw e;
      }
    }
    commit();
    // "Cleaning of out of date recalls done"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, DLF_BASE_ORACLELIB + 9);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    // "Cleaning of out of date recalls failed"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message",e.getMessage())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_ORACLELIB + 11, 1, params);
    throw ex;
  }
}
