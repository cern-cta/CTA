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
 * @(#)$RCSfile: OraQuerySvc.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2005/02/02 18:10:11 $ $Author: bcouturi $
 *
 * Implementation of the IQuerySvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/db/ora/OraQuerySvc.hpp"
#include "castor/stager/DiskCopyInfo.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"

#include <string>
#include <list>

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
/*const std::string castor::db::ora::OraQuerySvc::s_diskCopies4FileStatementString =
"SELECT DiskCopy.id, DiskCopy.path, CastorFile.filesize, DiskCopy.status as dstatus, tapecopy.status as tstatus, NULL FROM CastorFile, DiskCopy, tapecopy WHERE CastorFile.fileId =  :1   AND CastorFile.nsHost = :2 AND DiskCopy.status in (0,2,4,5,6)  AND DiskCopy.castorFile = Castorfile.id   AND castorfile.id = tapecopy.castorfile (+) UNION SELECT DiskCopy.id, DiskCopy.path, CastorFile.filesize, DiskCopy.status, tapecopy.status, segment.status  FROM CastorFile, DiskCopy, tapecopy, segment    WHERE CastorFile.fileId =  :1   AND CastorFile.nsHost = :2 AND DiskCopy.status in (0,2,4,5,6)  AND DiskCopy.castorFile = Castorfile.id   AND castorfile.id = tapecopy.castorfile AND segment.copy = tapecopy.id (+)";
*/

const std::string castor::db::ora::OraQuerySvc::s_diskCopies4FileStatementString =
"SELECT  DiskCopy.id,  DiskCopy.path,  CastorFile.filesize,  nvl(DiskCopy.status, -1), nvl(tpseg.tstatus, -1),  nvl(tpseg.sstatus, -1) FROM CastorFile, DiskCopy, (SELECT tapecopy.castorfile, tapecopy.id, tapecopy.status as tstatus, segment.status as sstatus  FROM tapecopy, segment WHERE tapecopy.id = segment.copy (+)) tpseg WHERE DiskCopy.castorFile = Castorfile.id AND CastorFile.fileId = :1 AND CastorFile.nsHost = :2 AND castorfile.id = tpseg.castorfile (+) ";


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
std::list<castor::stager::DiskCopyInfo*>
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
    std::list<castor::stager::DiskCopyInfo*> result;
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      castor::stager::DiskCopyInfo* item =
        new castor::stager::DiskCopyInfo();
       item->setId(rset->getInt(1));
       item->setDiskCopyPath(rset->getString(2));
      item->setSize(rset->getInt(3));
      item->setDiskCopyStatus((castor::stager::DiskCopyStatusCodes)
                              rset->getInt(4));
      item->setTapeCopyStatus((castor::stager::TapeCopyStatusCodes)
                              rset->getInt(5));
      item->setSegmentStatus((castor::stager::SegmentStatusCodes)
                              rset->getInt(6));
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
