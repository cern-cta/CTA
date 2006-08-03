/******************************************************************************
 *                      OraFSSvc.cpp
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
 * @(#)$RCSfile: OraFSSvc.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2006/08/03 10:03:16 $ $Author: gtaur $
 *
 * Implementation of the IFSSvc for Oracle
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
#include "castor/db/ora/OraFSSvc.hpp"
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
static castor::SvcFactory<castor::db::ora::OraFSSvc>* s_factoryOraFSSvc =
  new castor::SvcFactory<castor::db::ora::OraFSSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for selectDiskPool
const std::string castor::db::ora::OraFSSvc::s_selectDiskPoolStatementString =
  "SELECT id FROM DiskPool WHERE name = :1";

/// SQL statement for selectTapePool
const std::string castor::db::ora::OraFSSvc::s_selectTapePoolStatementString =
  "SELECT id FROM TapePool WHERE name = :1";

/// SQL statement for selectDiskServer
const std::string castor::db::ora::OraFSSvc::s_selectDiskServerStatementString =
  "SELECT id, status FROM DiskServer WHERE name = :1";

// -----------------------------------------------------------------------
// OraFSSvc
// -----------------------------------------------------------------------
castor::db::ora::OraFSSvc::OraFSSvc(const std::string name) :
  OraCommonSvc(name),
  m_selectDiskPoolStatement(0),
  m_selectTapePoolStatement(0),
  m_selectDiskServerStatement(0) {
}

// -----------------------------------------------------------------------
// ~OraFSSvc
// -----------------------------------------------------------------------
castor::db::ora::OraFSSvc::~OraFSSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraFSSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraFSSvc::ID() {
  return castor::SVC_ORAFSSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraFSSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    deleteStatement(m_selectDiskPoolStatement);
    deleteStatement(m_selectTapePoolStatement);
    deleteStatement(m_selectDiskServerStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_selectDiskPoolStatement = 0;
  m_selectTapePoolStatement = 0;
  m_selectDiskServerStatement = 0;
}

// -----------------------------------------------------------------------
// selectDiskPool
// -----------------------------------------------------------------------
castor::stager::DiskPool*
castor::db::ora::OraFSSvc::selectDiskPool
(const std::string name)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectDiskPoolStatement) {
    m_selectDiskPoolStatement =
      createStatement(s_selectDiskPoolStatementString);
  }
  // Execute statement and get result
  unsigned long id;
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
castor::db::ora::OraFSSvc::selectTapePool
(const std::string name)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectTapePoolStatement) {
    m_selectTapePoolStatement =
      createStatement(s_selectTapePoolStatementString);
  }
  // Execute statement and get result
  unsigned long id;
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

// -----------------------------------------------------------------------
// selectDiskServer
// -----------------------------------------------------------------------
castor::stager::DiskServer*
castor::db::ora::OraFSSvc::selectDiskServer
(const std::string name)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectDiskServerStatement) {
    m_selectDiskServerStatement =
      createStatement(s_selectDiskServerStatementString);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectDiskServerStatement->setString(1, name);
    oracle::occi::ResultSet *rset = m_selectDiskServerStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectDiskServerStatement->closeResultSet(rset);
      return 0;
    }
    // Found the DiskServer, so create it in memory
    castor::stager::DiskServer* result =
      new castor::stager::DiskServer();
    result->setId((u_signed64)rset->getDouble(1));
    result->setStatus
      ((enum castor::stager::DiskServerStatusCode)
       rset->getInt(2));
    result->setName(name);
    m_selectDiskServerStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select DiskServer by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

