/******************************************************************************
 *                      OraQuerySvc.cpp
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
 * @(#)$RCSfile: OraQuerySvc.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2005/01/10 17:26:07 $ $Author: sponcec3 $
 *
 * Implementation of the IQuerySvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/db/ora/OraQuerySvc.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/exception/Internal.hpp"
#include <string>

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraQuerySvc> s_factoryOraQuerySvc;
const castor::IFactory<castor::IService>&
OraQuerySvcFactory = s_factoryOraQuerySvc;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for tapesToDo
const std::string castor::db::ora::OraQuerySvc::s_diskCopies4FileStatementString =
  "SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, FileSystem.mountPoint, FileSystem.weight, DiskServer.name FROM CastorFile, DiskCopy, FileSystem, DiskServer WHERE CastorFile.fileId = :1 AND CastorFile.nsHost = :2 AND DiskCopy.castorFile = Castorfile.id AND DiskCopy.fileSystem = FileSystem.id and FileSystem.diskserver = DiskServer.id";

// -----------------------------------------------------------------------
// OraQuerySvc
// -----------------------------------------------------------------------
castor::db::ora::OraQuerySvc::OraQuerySvc(const std::string name) :
  BaseSvc(name), OraBaseObj(0),
  m_diskCopies4FileStatement(0) {}

// -----------------------------------------------------------------------
// ~OraQuerySvc
// -----------------------------------------------------------------------
castor::db::ora::OraQuerySvc::~OraQuerySvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraQuerySvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraQuerySvc::ID() {
  return castor::SVC_ORAQUERYSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraQuerySvc::reset() throw() {
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_diskCopies4FileStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_diskCopies4FileStatement = 0;
}

// -----------------------------------------------------------------------
// diskCopies4File
// -----------------------------------------------------------------------
std::list<castor::stager::DiskCopyForRecall*>
castor::db::ora::OraQuerySvc::diskCopies4File
(std::string fileId, std::string nsHost)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_diskCopies4FileStatement) {
      m_diskCopies4FileStatement =
        createStatement(s_diskCopies4FileStatementString);
    }
    // execute the statement and see whether we found something
    m_diskCopies4FileStatement->setString(1, fileId);
    m_diskCopies4FileStatement->setString(2, nsHost);
    oracle::occi::ResultSet *rset =
      m_diskCopies4FileStatement->executeQuery();
    // Gather the results
    std::list<castor::stager::DiskCopyForRecall*> result;
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      castor::stager::DiskCopyForRecall* item =
        new castor::stager::DiskCopyForRecall();
      item->setId(rset->getInt(1));
      item->setPath(rset->getString(2));
      item->setStatus((castor::stager::DiskCopyStatusCodes)rset->getInt(3));
      item->setMountPoint(rset->getString(4));
      item->setFsWeight(rset->getDouble(5));
      item->setDiskServer(rset->getString(6));
      result.push_back(item);
    }
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage() << "Error caught in diskCopies4File."
                    << std::endl << e.what();
    throw ex;
  }
}
