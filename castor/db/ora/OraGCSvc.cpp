/******************************************************************************
 *                      OraGCSvc.cpp
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
 * @(#)$RCSfile: OraGCSvc.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/09/05 12:53:42 $ $Author: sponcec3 $
 *
 * Implementation of the IGCSvc for Oracle
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
#include "castor/db/ora/OraGCSvc.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
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
static castor::SvcFactory<castor::db::ora::OraGCSvc> s_factoryOraGCSvc;
const castor::IFactory<castor::IService>&
OraGCSvcFactory = s_factoryOraGCSvc;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for selectFiles2Delete
const std::string castor::db::ora::OraGCSvc::s_selectFiles2DeleteStatementString =
  "BEGIN selectFiles2Delete(:1, :2); END;";

/// SQL statement for filesDeleted
const std::string castor::db::ora::OraGCSvc::s_filesDeletedStatementString =
  "BEGIN filesDeletedProc(:1); END;";

/// SQL statement for filesDeleted
const std::string castor::db::ora::OraGCSvc::s_filesDeletionFailedStatementString =
  "BEGIN filesDeletionFailedProc(:1); END;";

/// SQL statement for requestToDo
const std::string castor::db::ora::OraGCSvc::s_requestToDoStatementString =
  "BEGIN :1 := 0; DELETE FROM newRequests WHERE type IN (73, 74, 83) AND ROWNUM < 2 RETURNING id INTO :1; END;";

// -----------------------------------------------------------------------
// OraGCSvc
// -----------------------------------------------------------------------
castor::db::ora::OraGCSvc::OraGCSvc(const std::string name) :
  OraCommonSvc(name),
  m_selectFiles2DeleteStatement(0),
  m_filesDeletedStatement(0),
  m_filesDeletionFailedStatement(0),
  m_requestToDoStatement(0) {
}

// -----------------------------------------------------------------------
// ~OraGCSvc
// -----------------------------------------------------------------------
castor::db::ora::OraGCSvc::~OraGCSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraGCSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraGCSvc::ID() {
  return castor::SVC_ORAGCSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraGCSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    deleteStatement(m_selectFiles2DeleteStatement);
    deleteStatement(m_filesDeletedStatement);
    deleteStatement(m_filesDeletionFailedStatement);
    deleteStatement(m_requestToDoStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_selectFiles2DeleteStatement = 0;
  m_filesDeletedStatement = 0;
  m_filesDeletionFailedStatement = 0;
  m_requestToDoStatement = 0;
}


// -----------------------------------------------------------------------
// selectFiles2Delete
// -----------------------------------------------------------------------
std::vector<castor::stager::GCLocalFile*>*
castor::db::ora::OraGCSvc::selectFiles2Delete
(std::string diskServer)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectFiles2DeleteStatement) {
    m_selectFiles2DeleteStatement =
      createStatement(s_selectFiles2DeleteStatementString);
    m_selectFiles2DeleteStatement->registerOutParam
      (2, oracle::occi::OCCICURSOR);
    m_selectFiles2DeleteStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectFiles2DeleteStatement->setString(1, diskServer);
    unsigned int nb = m_selectFiles2DeleteStatement->executeUpdate();
    if (0 == nb) {
      rollback();
      castor::exception::Internal ex;
      ex.getMessage()
        << "selectFiles2Delete : Unable to select files to delete.";
      throw ex;
    }
    // create result
    std::vector<castor::stager::GCLocalFile*>* result =
      new std::vector<castor::stager::GCLocalFile*>;
    // Run through the cursor
    oracle::occi::ResultSet *rs =
      m_selectFiles2DeleteStatement->getCursor(2);
    oracle::occi::ResultSet::Status status = rs->next();
    while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      // Fill result
      castor::stager::GCLocalFile* f = new castor::stager::GCLocalFile();
      f->setFileName(rs->getString(1));
      f->setDiskCopyId((u_signed64)rs->getDouble(2));
      result->push_back(f);
      status = rs->next();
    }
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select files to delete :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// filesDeleted
// -----------------------------------------------------------------------
void castor::db::ora::OraGCSvc::filesDeleted
(std::vector<u_signed64*>& diskCopyIds)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_filesDeletedStatement) {
    m_filesDeletedStatement =
      createStatement(s_filesDeletedStatementString);
    m_filesDeletedStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    // Deal with the list of diskcopy ids
    unsigned int nb = diskCopyIds.size();
    // Compute actual length of the buffers : this
    // may be different from the needed one, since
    // Oracle does not like 0 length arrays....
    unsigned int nba = nb == 0 ? 1 : nb;
    ub2 lens[nb];
    unsigned char buffer[nba][21];
    memset(buffer, 0, nba * 21);
    for (int i = 0; i < nb; i++) {
      oracle::occi::Number n = (double)(*(diskCopyIds[i]));
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(buffer[i],b.length());
      lens[i] = b.length();
    }
    ub4 unused = nb;
    m_filesDeletedStatement->setDataBufferArray
      (1, buffer, oracle::occi::OCCI_SQLT_NUM,
       nba, &unused, 21, lens);
    // execute the statement
    m_filesDeletedStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to remove deleted files :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// filesDeletionFailed
// -----------------------------------------------------------------------
void castor::db::ora::OraGCSvc::filesDeletionFailed
(std::vector<u_signed64*>& diskCopyIds)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_filesDeletionFailedStatement) {
    m_filesDeletionFailedStatement =
      createStatement(s_filesDeletionFailedStatementString);
    m_filesDeletionFailedStatement->setAutoCommit(true);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    // Deal with the list of diskcopy ids
    unsigned int nb = diskCopyIds.size();
    // Compute actual length of the buffers : this
    // may be different from the needed one, since
    // Oracle does not like 0 length arrays....
    unsigned int nba = nb == 0 ? 1 : nb;
    ub2 lens[nb];
    unsigned char buffer[nba][21];
    memset(buffer, 0, nba * 21);
    for (int i = 0; i < nb; i++) {
      oracle::occi::Number n = (double)(*(diskCopyIds[i]));
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(buffer[i],b.length());
      lens[i] = b.length();
    }
    ub4 unused = nb;
    m_filesDeletionFailedStatement->setDataBufferArray
      (1, buffer, oracle::occi::OCCI_SQLT_NUM,
       nba, &unused, 21, lens);
    // execute the statement
    m_filesDeletionFailedStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to remove deleted files :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// requestToDo
// -----------------------------------------------------------------------
castor::stager::Request*
castor::db::ora::OraGCSvc::requestToDo()
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
