/******************************************************************************
 *                      DbCommonSvc.cpp
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
 * @(#)$RCSfile: DbCommonSvc.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2005/09/19 14:41:50 $ $Author: sponcec3 $
 *
 * Implementation of the ICommonSvc for CDBC
 *
 * @author Giuseppe Lo Presti
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
#include "castor/db/DbCommonSvc.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/db/IDbResultSet.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Busy.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/exception/SQLError.hpp"
#include "castor/stager/TapeStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/BaseAddress.hpp"
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
static castor::SvcFactory<castor::db::DbCommonSvc> s_factoryDbCommonSvc;
const castor::IFactory<castor::IService>&
DbCommonSvcFactory = s_factoryDbCommonSvc;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for selectTape
const std::string castor::db::DbCommonSvc::s_selectTapeStatementString =
  "SELECT id FROM Tape WHERE vid = :1 AND side = :2 AND tpmode = :3 FOR UPDATE";

/// SQL statement for selectSvcClass
const std::string castor::db::DbCommonSvc::s_selectSvcClassStatementString =
  "SELECT id, nbDrives, defaultFileSize, maxReplicaNb, replicationPolicy, gcPolicy, migratorPolicy, recallerPolicy FROM SvcClass WHERE name = :1";

/// SQL statement for selectFileClass
const std::string castor::db::DbCommonSvc::s_selectFileClassStatementString =
  "SELECT id, minFileSize, maxFileSize, nbCopies FROM FileClass WHERE name = :1";

  /// SQL statement for selectFileSystem
const std::string castor::db::DbCommonSvc::s_selectFileSystemStatementString =
  "SELECT DiskServer.id, DiskServer.status, FileSystem.id, FileSystem.free, FileSystem.weight, FileSystem.fsDeviation, FileSystem.status, FileSystem.minFreeSpace, FileSystem.maxFreeSpace, FileSystem.reservedSpace, FileSystem.spaceToBeFreed, FileSystem.totalSize FROM FileSystem, DiskServer WHERE DiskServer.name = :1 AND FileSystem.mountPoint = :2 AND FileSystem.diskserver = DiskServer.id FOR UPDATE";

  
// -----------------------------------------------------------------------
// DbCommonSvc
// -----------------------------------------------------------------------
castor::db::DbCommonSvc::DbCommonSvc(const std::string name) :
  BaseSvc(name), DbBaseObj(0),
  m_selectTapeStatement(0),
  m_selectSvcClassStatement(0),
  m_selectFileClassStatement(0),
  m_selectFileSystemStatement(0) {
}

// -----------------------------------------------------------------------
// ~DbCommonSvc
// -----------------------------------------------------------------------
castor::db::DbCommonSvc::~DbCommonSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::DbCommonSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::DbCommonSvc::ID() {
  return castor::SVC_DBCOMMONSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::DbCommonSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    delete m_selectTapeStatement;
    delete m_selectSvcClassStatement;
    delete m_selectFileClassStatement;
    delete m_selectFileSystemStatement;
  } catch (castor::exception::SQLError ignored) {};
  // Now reset all pointers to 0
  m_selectTapeStatement = 0;
  m_selectSvcClassStatement = 0;
  m_selectFileClassStatement = 0;
  m_selectFileSystemStatement = 0;
}

// -----------------------------------------------------------------------
// selectTape
// -----------------------------------------------------------------------
castor::stager::Tape*
castor::db::DbCommonSvc::selectTape(const std::string vid,
                                          const int side,
                                          const int tpmode)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectTapeStatement) {
    m_selectTapeStatement = createStatement(s_selectTapeStatementString);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectTapeStatement->setString(1, vid);
    m_selectTapeStatement->setInt(2, side);
    m_selectTapeStatement->setInt(3, tpmode);
    castor::db::IDbResultSet *rset = m_selectTapeStatement->executeQuery();
    if (!rset->next()) {
      delete rset;
      // we found nothing, so let's create the tape
      castor::stager::Tape* tape = new castor::stager::Tape();
      tape->setVid(vid);
      tape->setSide(side);
      tape->setTpmode(tpmode);
      tape->setStatus(castor::stager::TAPE_UNUSED);
      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      try {
        cnvSvc()->createRep(&ad, tape, false);
        return tape;
      } catch (castor::exception::SQLError e) {
        delete tape;
        if (1 == e.getSQLErrorCode()) {
          // if violation of unique constraint, ie means that
          // some other thread was quicker than us on the insertion
          // So let's select what was inserted
          rset = m_selectTapeStatement->executeQuery();
          if (!rset->next()) {
            // Still nothing ! Here it's a real error
            delete rset;
            castor::exception::Internal ex;
            ex.getMessage()
              << "Unable to select tape while inserting "
              << "violated unique constraint :"
              << std::endl << e.getMessage();
            throw ex;
          }
        }
        delete rset;
        // Else, "standard" error, throw exception
        castor::exception::Internal ex;
        ex.getMessage()
          << "Exception while inserting new tape in the DB :"
          << std::endl << e.getMessage();
        throw ex;
      }
    }
    // If we reach this point, then we selected successfully
    // a tape and its id is in rset
    id = rset->getInt64(1);
    delete rset;
  } catch (castor::exception::SQLError e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape by vid, side and tpmode :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // Now get the tape from its id
  try {
    castor::BaseAddress ad;
    ad.setTarget(id);
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    castor::IObject* obj = cnvSvc()->createObj(&ad);
    castor::stager::Tape* tape =
      dynamic_cast<castor::stager::Tape*> (obj);
    if (0 == tape) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << id;
      delete obj;
      throw e;
    }
    return tape;
  } catch (castor::exception::SQLError e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point
}

// -----------------------------------------------------------------------
// selectSvcClass
// -----------------------------------------------------------------------
castor::stager::SvcClass*
castor::db::DbCommonSvc::selectSvcClass
(const std::string name)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectSvcClassStatement) {
    m_selectSvcClassStatement =
      createStatement(s_selectSvcClassStatementString);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectSvcClassStatement->setString(1, name);
    castor::db::IDbResultSet *rset = m_selectSvcClassStatement->executeQuery();
    if (!rset->next()) {
      // Nothing found, return 0
      delete rset;
      return 0;
    }
    // Found the SvcClass, so create it in memory
    castor::stager::SvcClass* result =
      new castor::stager::SvcClass();
    result->setId(rset->getInt64(1));
    result->setNbDrives(rset->getInt(2));
    result->setDefaultFileSize(rset->getInt64(3));
    result->setMaxReplicaNb(rset->getInt(4));
    result->setReplicationPolicy(rset->getString(5));
    result->setGcPolicy(rset->getString(6));
    result->setMigratorPolicy(rset->getString(7));
    result->setRecallerPolicy(rset->getString(8));
    result->setName(name);
    delete rset;
    return result;
  } catch (castor::exception::SQLError e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select SvcClass by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// selectFileClass
// -----------------------------------------------------------------------
castor::stager::FileClass*
castor::db::DbCommonSvc::selectFileClass
(const std::string name)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectFileClassStatement) {
    m_selectFileClassStatement =
      createStatement(s_selectFileClassStatementString);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectFileClassStatement->setString(1, name);
    castor::db::IDbResultSet *rset = m_selectFileClassStatement->executeQuery();
    if (!rset->next()) {
      // Nothing found, return 0
      delete rset;
      return 0;
    }
    // Found the FileClass, so create it in memory
    castor::stager::FileClass* result =
      new castor::stager::FileClass();
    result->setId(rset->getInt64(1));
    result->setMinFileSize(rset->getInt64(2));
    result->setMaxFileSize(rset->getInt64(3));
    result->setNbCopies(rset->getInt(4));
    result->setName(name);
    delete rset;
    return result;
  } catch (castor::exception::SQLError e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select FileClass by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// selectFileSystem
// -----------------------------------------------------------------------
castor::stager::FileSystem*
castor::db::DbCommonSvc::selectFileSystem
(const std::string mountPoint,
 const std::string diskServer)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectFileSystemStatement) {
    m_selectFileSystemStatement =
      createStatement(s_selectFileSystemStatementString);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectFileSystemStatement->setString(1, diskServer);
    m_selectFileSystemStatement->setString(2, mountPoint);
    castor::db::IDbResultSet *rset = m_selectFileSystemStatement->executeQuery();
    if (!rset->next()) {
      // Nothing found, return 0
      delete rset;
      return 0;
    }
    // Found the FileSystem and the DiskServer,
    // create them in memory
    castor::stager::DiskServer* ds =
      new castor::stager::DiskServer();
    ds->setId(rset->getInt64(1));
    ds->setStatus
      ((enum castor::stager::DiskServerStatusCode)rset->getInt(2));
    ds->setName(diskServer);
    castor::stager::FileSystem* result =
      new castor::stager::FileSystem();
    result->setId(rset->getInt64(3));
    result->setFree(rset->getInt64(4));
    result->setWeight(rset->getDouble(5));
    result->setFsDeviation(rset->getDouble(6));
    result->setStatus
      ((enum castor::stager::FileSystemStatusCodes)rset->getInt(7));
    result->setMinFreeSpace(rset->getFloat(8));
    result->setMaxFreeSpace(rset->getFloat(9));
    result->setReservedSpace(rset->getInt64(10));
    result->setSpaceToBeFreed(rset->getInt64(11));
    result->setTotalSize(rset->getInt64(12));
    result->setMountPoint(mountPoint);
    result->setDiskserver(ds);
    ds->addFileSystems(result);
    delete rset;
    return result;
  } catch (castor::exception::SQLError e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select FileSystem by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}


// -----------------------------------------------------------------------
// commit
// -----------------------------------------------------------------------
void
castor::db::DbCommonSvc::commit() {
	try {
	  cnvSvc()->commit();
	} catch (castor::exception::Exception) {
	  // commit failed, let's rollback for security
	  rollback();
	}
}


// -----------------------------------------------------------------------
// rollback
// -----------------------------------------------------------------------
void
castor::db::DbCommonSvc::rollback() {
  try {
    cnvSvc()->rollback();
  } catch (castor::exception::Exception) {
    // rollback failed, let's drop the connection for security
    cnvSvc()->dropConnection();
  }
}
