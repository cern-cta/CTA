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
 * @(#)$RCSfile: OraStagerSvc.cpp,v $ $Revision: 1.72 $ $Release$ $Date: 2004/12/03 14:03:46 $ $Author: sponcec3 $
 *
 *
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
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/FileClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/TapeCopyForMigration.hpp"
#include "castor/db/ora/OraStagerSvc.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/InvalidArgument.hpp"
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

#define NS_SEGMENT_NOTOK (' ')

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraStagerSvc> s_factoryOraStagerSvc;
const castor::IFactory<castor::IService>&
OraStagerSvcFactory = s_factoryOraStagerSvc;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for tapesToDo
const std::string castor::db::ora::OraStagerSvc::s_tapesToDoStatementString =
  "SELECT id FROM Tape WHERE status = :1";

/// SQL statement for streamsToDo
const std::string castor::db::ora::OraStagerSvc::s_streamsToDoStatementString =
  "SELECT id FROM Stream WHERE status = :1";

/// SQL statement for selectTape
const std::string castor::db::ora::OraStagerSvc::s_selectTapeStatementString =
  "SELECT id FROM Tape WHERE vid = :1 AND side = :2 AND tpmode = :3 FOR UPDATE";

/// SQL statement for anyTapeCopyForStream
const std::string castor::db::ora::OraStagerSvc::s_anyTapeCopyForStreamStatementString =
  "SELECT id FROM TapeCopy, Stream2TapeCopy WHERE status = :1 and child = id and ROWNUM < 2";

/// SQL statement for bestTapeCopyForStream
const std::string castor::db::ora::OraStagerSvc::s_bestTapeCopyForStreamStatementString =
  "BEGIN bestTapeCopyForStream(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13); END;";

/// SQL statement for bestFileSystemForSegment
const std::string castor::db::ora::OraStagerSvc::s_bestFileSystemForSegmentStatementString =
  "BEGIN bestFileSystemForSegment(:1, :2, :3, :4, :5); END;";

/// SQL statement for fileRecalled
const std::string castor::db::ora::OraStagerSvc::s_fileRecalledStatementString =
  "BEGIN fileRecalled(:1, :2, :3, :4); END;";

/// SQL statement for isSubRequestToSchedule
const std::string castor::db::ora::OraStagerSvc::s_isSubRequestToScheduleStatementString =
  "BEGIN isSubRequestToSchedule(:1, :2); END;";

/// SQL statement for getUpdateStart
const std::string castor::db::ora::OraStagerSvc::s_getUpdateStartStatementString =
  "BEGIN getUpdateStart(:1, :2, :3, :4, :5, :6, :7, :8); END;";

/// SQL statement for putStart
const std::string castor::db::ora::OraStagerSvc::s_putStartStatementString =
  "BEGIN putStart(:1, :2, :3 :4 :5 :6 :7); END;";

/// SQL statement for selectSvcClass
const std::string castor::db::ora::OraStagerSvc::s_selectSvcClassStatementString =
  "SELECT id, policy, nbDrives FROM SvcClass WHERE name = :1";

/// SQL statement for selectFileClass
const std::string castor::db::ora::OraStagerSvc::s_selectFileClassStatementString =
  "SELECT id, minFileSize, maxFileSize, nbCopies FROM FileClass WHERE name = :1";

/// SQL statement for selectCastorFile
const std::string castor::db::ora::OraStagerSvc::s_selectCastorFileStatementString =
  "SELECT id, fileSize FROM CastorFile WHERE fileId = :1 AND nsHost = :2";

/// SQL statement for selectFileSystem
const std::string castor::db::ora::OraStagerSvc::s_selectFileSystemStatementString =
  "SELECT DiskServer.id, DiskServer.status, FileSystem.id, FileSystem.free, FileSystem.weight, FileSystem.fsDeviation, FileSystem.status FROM FileSystem, DiskServer WHERE DiskServer.name = :1 AND FileSystem.mountPoint = :2 AND FileSystem.diskserver = DiskServer.id";

/// SQL statement for selectDiskPool
const std::string castor::db::ora::OraStagerSvc::s_selectDiskPoolStatementString =
  "SELECT id FROM DiskPool WHERE name = :1";

/// SQL statement for selectDiskServer
const std::string castor::db::ora::OraStagerSvc::s_selectDiskServerStatementString =
  "SELECT id, status FROM DiskServer WHERE name = :1";

/// SQL statement for selectTapeCopiesForMigration
const std::string castor::db::ora::OraStagerSvc::s_selectTapeCopiesForMigrationStatementString =
  "SELECT id FROM TapeCopy, CastorFile WHERE TapeCopy.castorFile = CastorFile.id AND CastorFile.svcClass = :1 AND TapeCopy.status IN (0, 1)";

/// SQL statement for updateAndCheckSubRequest
const std::string castor::db::ora::OraStagerSvc::s_updateAndCheckSubRequestStatementString =
  "BEGIN updateAndCheckSubRequest(:1, :2, :3); END;";

/// SQL statement for recreateCastorFile
const std::string castor::db::ora::OraStagerSvc::s_recreateCastorFileStatementString =
  "BEGIN recreateCastorFile(:1, :2); END;";

/// SQL statement for prepareForMigration
const std::string castor::db::ora::OraStagerSvc::s_prepareForMigrationStatementString =
  "BEGIN prepareForMigration(:1, :2); END;";

// -----------------------------------------------------------------------
// OraStagerSvc
// -----------------------------------------------------------------------
castor::db::ora::OraStagerSvc::OraStagerSvc(const std::string name) :
  BaseSvc(name), OraBaseObj(0),
  m_tapesToDoStatement(0),
  m_streamsToDoStatement(0),
  m_selectTapeStatement(0),
  m_anyTapeCopyForStreamStatement(0),
  m_bestTapeCopyForStreamStatement(0),
  m_bestFileSystemForSegmentStatement(0),
  m_fileRecalledStatement(0),
  m_subRequestToDoStatement(0),
  m_requestToDoStatement(0),
  m_isSubRequestToScheduleStatement(0),
  m_getUpdateStartStatement(0),
  m_putStartStatement(0),
  m_selectSvcClassStatement(0),
  m_selectFileClassStatement(0),
  m_selectCastorFileStatement(0),
  m_selectFileSystemStatement(0),
  m_selectDiskPoolStatement(0),
  m_selectDiskServerStatement(0),
  m_selectTapeCopiesForMigrationStatement(0),
  m_updateAndCheckSubRequestStatement(0),
  m_recreateCastorFileStatement(0),
  m_prepareForMigrationStatement(0) {
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
  try {
    deleteStatement(m_tapesToDoStatement);
    deleteStatement(m_streamsToDoStatement);
    deleteStatement(m_selectTapeStatement);
    deleteStatement(m_anyTapeCopyForStreamStatement);
    deleteStatement(m_bestTapeCopyForStreamStatement);
    deleteStatement(m_bestFileSystemForSegmentStatement);
    deleteStatement(m_fileRecalledStatement);
    deleteStatement(m_subRequestToDoStatement);
    deleteStatement(m_requestToDoStatement);
    deleteStatement(m_isSubRequestToScheduleStatement);
    deleteStatement(m_getUpdateStartStatement);
    deleteStatement(m_putStartStatement);
    deleteStatement(m_selectSvcClassStatement);
    deleteStatement(m_selectFileClassStatement);
    deleteStatement(m_selectFileSystemStatement);
    deleteStatement(m_selectCastorFileStatement);
    deleteStatement(m_selectDiskPoolStatement);
    deleteStatement(m_selectDiskServerStatement);
    deleteStatement(m_selectTapeCopiesForMigrationStatement);
    deleteStatement(m_updateAndCheckSubRequestStatement);
    deleteStatement(m_recreateCastorFileStatement);
    deleteStatement(m_prepareForMigrationStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_tapesToDoStatement = 0;
  m_streamsToDoStatement = 0;
  m_selectTapeStatement = 0;
  m_anyTapeCopyForStreamStatement = 0;
  m_bestTapeCopyForStreamStatement = 0;
  m_bestFileSystemForSegmentStatement = 0;
  m_fileRecalledStatement = 0;
  m_subRequestToDoStatement = 0;
  m_requestToDoStatement = 0;
  m_isSubRequestToScheduleStatement = 0;
  m_getUpdateStartStatement = 0;
  m_putStartStatement = 0;
  m_selectSvcClassStatement = 0;
  m_selectFileClassStatement = 0;
  m_selectFileSystemStatement = 0;
  m_selectCastorFileStatement = 0;
  m_selectDiskPoolStatement = 0;
  m_selectDiskServerStatement = 0;
  m_selectTapeCopiesForMigrationStatement = 0;
  m_updateAndCheckSubRequestStatement = 0;
  m_recreateCastorFileStatement = 0;
  m_prepareForMigrationStatement = 0;
}

// -----------------------------------------------------------------------
// anySegmentsForTape
// -----------------------------------------------------------------------
int castor::db::ora::OraStagerSvc::anySegmentsForTape
(castor::stager::Tape* searchItem)
  throw (castor::exception::Exception) {
  // XXX This is a first version. Has to be improved from
  // XXX the efficiency point of view !
  std::vector<castor::stager::Segment*> result =
    segmentsForTape(searchItem);
  if (result.size() > 0){
    searchItem->setStatus(castor::stager::TAPE_WAITMOUNT);
    cnvSvc()->updateRep(0, searchItem, true);
  }
  return result.size();
}

// -----------------------------------------------------------------------
// segmentsForTape
// -----------------------------------------------------------------------
std::vector<castor::stager::Segment*>
castor::db::ora::OraStagerSvc::segmentsForTape
(castor::stager::Tape* searchItem)
  throw (castor::exception::Exception) {
  cnvSvc()->updateObj(searchItem);
  cnvSvc()->fillObj(0, searchItem, OBJ_Segment);
  std::vector<castor::stager::Segment*> result;
  std::vector<castor::stager::Segment*>::iterator it;
  for (it = searchItem->segments().begin();
       it != searchItem->segments().end();
       it++) {
    switch ((*it)->status()) {
    case castor::stager::SEGMENT_UNPROCESSED :
      result.push_back(*it);
      searchItem->setStatus(castor::stager::TAPE_MOUNTED);
      break;
    default:
      // Do not consider other status
      break;
    }
  }
  if (result.size() > 0) {
    castor::BaseAddress ad;
    ad.setCnvSvcName("OraCnvSvc");
    ad.setCnvSvcType(castor::SVC_ORACNV);
    cnvSvc()->updateRep(&ad, searchItem, true);
  }
  return result;
}

// -----------------------------------------------------------------------
// bestFileSystemForSegment
// -----------------------------------------------------------------------
castor::stager::DiskCopyForRecall*
castor::db::ora::OraStagerSvc::bestFileSystemForSegment
(castor::stager::Segment *segment)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_bestFileSystemForSegmentStatement) {
      m_bestFileSystemForSegmentStatement =
        createStatement(s_bestFileSystemForSegmentStatementString);
      m_bestFileSystemForSegmentStatement->registerOutParam
        (2, oracle::occi::OCCISTRING, 2048);
      m_bestFileSystemForSegmentStatement->registerOutParam
        (3, oracle::occi::OCCISTRING, 2048);
      m_bestFileSystemForSegmentStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);
      m_bestFileSystemForSegmentStatement->registerOutParam
        (5, oracle::occi::OCCIDOUBLE);
    }
    // execute the statement and see whether we found something
    m_bestFileSystemForSegmentStatement->setDouble(1, segment->id());
    unsigned int nb =
      m_bestFileSystemForSegmentStatement->executeUpdate();
    if (nb == 0) {
      return 0;
    }
    // Create result
    castor::stager::DiskCopyForRecall* result =
      new castor::stager::DiskCopyForRecall();
    result->setDiskServer(m_bestFileSystemForSegmentStatement->getString(2));
    result->setMountPoint(m_bestFileSystemForSegmentStatement->getString(3));
    result->setPath(m_bestFileSystemForSegmentStatement->getString(4));
    result->setId
      ((u_signed64)m_bestFileSystemForSegmentStatement->getDouble(5));
    // Fill result for CastorFile
    castor::BaseAddress ad;
    ad.setCnvSvcName("OraCnvSvc");
    ad.setCnvSvcType(castor::SVC_ORACNV);
    cnvSvc()->fillObj(&ad, result, OBJ_CastorFile);
    // commit
    cnvSvc()->commit();
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    if (1403 == e.getErrorCode()) {
      // No data found error, this is ok
      return 0;
    }
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in bestFileSystemForSegment."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// anyTapeCopyForStream
// -----------------------------------------------------------------------
bool castor::db::ora::OraStagerSvc::anyTapeCopyForStream
(castor::stager::Stream* searchItem)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_anyTapeCopyForStreamStatement) {
      m_anyTapeCopyForStreamStatement =
        createStatement(s_anyTapeCopyForStreamStatementString);
    }
    m_anyTapeCopyForStreamStatement->setInt(1, searchItem->id());
    oracle::occi::ResultSet *rset =
      m_anyTapeCopyForStreamStatement->executeQuery();
    bool result =
      oracle::occi::ResultSet::END_OF_FETCH == rset->next();
    m_anyTapeCopyForStreamStatement->closeResultSet(rset);
    if (result) {
      searchItem->setStatus(castor::stager::STREAM_WAITMOUNT);
      castor::BaseAddress ad;
      ad.setCnvSvcName("OraCnvSvc");
      ad.setCnvSvcType(castor::SVC_ORACNV);
      cnvSvc()->updateRep(&ad, searchItem, true);
    }
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in anyTapeCopyForStream."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// bestTapeCopyForStream
// -----------------------------------------------------------------------
castor::stager::TapeCopyForMigration*
castor::db::ora::OraStagerSvc::bestTapeCopyForStream
(castor::stager::Stream* searchItem)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_bestTapeCopyForStreamStatement) {
      m_bestTapeCopyForStreamStatement =
        createStatement(s_bestTapeCopyForStreamStatementString);
      m_bestTapeCopyForStreamStatement->setInt
        (2, castor::stager::TAPECOPY_WAITINSTREAMS);
      m_bestTapeCopyForStreamStatement->setInt
        (3, castor::stager::TAPECOPY_SELECTED);
      m_bestTapeCopyForStreamStatement->setInt
        (4, castor::stager::STREAM_RUNNING);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (7, oracle::occi::OCCISTRING, 2048);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (8, oracle::occi::OCCIDOUBLE);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (9, oracle::occi::OCCIDOUBLE);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (10, oracle::occi::OCCIDOUBLE);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (11, oracle::occi::OCCISTRING, 2048);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (12, oracle::occi::OCCIDOUBLE);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (13, oracle::occi::OCCIDOUBLE);
    }
    // execute the statement and see whether we found something
    m_bestTapeCopyForStreamStatement->setDouble(1, searchItem->id());
    unsigned int nb =
      m_bestTapeCopyForStreamStatement->executeUpdate();
    if (nb == 0) {
      castor::exception::NoEntry e;
      e.getMessage() << "No TapeCopy found";
      throw e;
    }
    // Create result
    castor::stager::TapeCopyForMigration* result =
      new castor::stager::TapeCopyForMigration();
    result->setDiskServer(m_bestTapeCopyForStreamStatement->getString(5));
    result->setMountPoint(m_bestTapeCopyForStreamStatement->getString(6));
    castor::stager::DiskCopy* diskCopy =
      new castor::stager::DiskCopy();
    diskCopy->setPath(m_bestTapeCopyForStreamStatement->getString(7));
    diskCopy->setId((u_signed64)m_bestTapeCopyForStreamStatement->getDouble(8));
    castor::stager::CastorFile* castorFile =
      new castor::stager::CastorFile();
    castorFile->setId
      ((u_signed64)m_bestTapeCopyForStreamStatement->getDouble(9));
    castorFile->setFileId
      ((u_signed64)m_bestTapeCopyForStreamStatement->getDouble(10));
    castorFile->setNsHost(m_bestTapeCopyForStreamStatement->getString(11));
    castorFile->setFileSize
      ((u_signed64)m_bestTapeCopyForStreamStatement->getDouble(12));
    result->setId((u_signed64)m_bestTapeCopyForStreamStatement->getDouble(13));
    diskCopy->setCastorFile(castorFile);
    castorFile->addDiskCopies(diskCopy);
    result->setCastorFile(castorFile);
    castorFile->addTapeCopies(result);
    // Fill result for TapeCopy, Segments and Tape
    cnvSvc()->updateObj(result);
    castor::BaseAddress ad;
    ad.setCnvSvcName("OraCnvSvc");
    ad.setCnvSvcType(castor::SVC_ORACNV);
    cnvSvc()->fillObj(&ad, result, OBJ_Segment);
    for (std::vector<castor::stager::Segment*>::iterator it =
           result->segments().begin();
         it != result->segments().end();
         it++) {
      cnvSvc()->fillObj(&ad, *it, OBJ_Tape);
    }
    // commit
    cnvSvc()->commit();
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    rollback();
    if (1403 == e.getErrorCode()) {
      // No Data Found exception
      castor::exception::NoEntry e;
      e.getMessage() << "No TapeCopy found";
      throw e;
    } else {
      castor::exception::Internal ex;
      ex.getMessage()
        << "Error caught in bestTapeCopyForStream."
        << std::endl << e.what();
      throw ex;
    }
  }
}

// -----------------------------------------------------------------------
// fileRecalled
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::fileRecalled
(castor::stager::TapeCopy* tapeCopy)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_fileRecalledStatement) {
      m_fileRecalledStatement =
        createStatement(s_fileRecalledStatementString);
      m_fileRecalledStatement->setInt
        (3, castor::stager::SUBREQUEST_RESTART);
      m_fileRecalledStatement->setInt
        (4, castor::stager::DISKCOPY_STAGED);
      m_fileRecalledStatement->setAutoCommit(true);
    }
    // get a Cuuid for the DiskCopy
    // XXX Interface to Cuuid has to be improved !
    // XXX its length has currently yo be hardcoded
    // XXX wherever you use it !!!
    Cuuid_t cuuid;
    Cuuid_create(&cuuid);
    char uuid[CUUID_STRING_LEN+1];
    uuid[CUUID_STRING_LEN] = 0;
    Cuuid2string(uuid, CUUID_STRING_LEN, &cuuid);
    // execute the statement and see whether we found something
    m_fileRecalledStatement->setDouble(1, tapeCopy->id());
    m_fileRecalledStatement->setString(2, uuid);
    unsigned int nb =
      m_fileRecalledStatement->executeUpdate();
    if (nb == 0) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "fileRecalled : unable to update SubRequest and DiskCopy status.";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in fileRecalled."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// tapesToDo
// -----------------------------------------------------------------------
std::vector<castor::stager::Tape*>
castor::db::ora::OraStagerSvc::tapesToDo()
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_tapesToDoStatement) {
    m_tapesToDoStatement = createStatement(s_tapesToDoStatementString);
    m_tapesToDoStatement->setInt(1, castor::stager::TAPE_PENDING);
  }
  std::vector<castor::stager::Tape*> result;
  try {
    oracle::occi::ResultSet *rset = m_tapesToDoStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1));
      castor::stager::Tape* tape =
        dynamic_cast<castor::stager::Tape*>(obj);
      if (0 == tape) {
        castor::exception::Internal ex;
        ex.getMessage()
          << "In method OraStagerSvc::tapesToDo, got a non tape object";
        delete obj;
        throw ex;
      }
      // Change tape status
      tape->setStatus(castor::stager::TAPE_WAITDRIVE);
      cnvSvc()->updateRep(0, tape, false);
      result.push_back(tape);
    }
    m_tapesToDoStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error in tapesToDo while retrieving list of tapes."
      << std::endl << e.what();
    throw ex;
  }
  // Commit all status changes
  cnvSvc()->getConnection()->commit();
  return result;
}


// -----------------------------------------------------------------------
// streamsToDo
// -----------------------------------------------------------------------
std::vector<castor::stager::Stream*>
castor::db::ora::OraStagerSvc::streamsToDo()
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_streamsToDoStatement) {
    m_streamsToDoStatement = createStatement(s_streamsToDoStatementString);
    m_streamsToDoStatement->setInt(1, castor::stager::STREAM_PENDING);
  }
  std::vector<castor::stager::Stream*> result;
  try {
    oracle::occi::ResultSet *rset = m_streamsToDoStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1));
      castor::stager::Stream* stream =
        dynamic_cast<castor::stager::Stream*>(obj);
      if (0 == stream) {
        castor::exception::Internal ex;
        ex.getMessage()
          << "In method OraStagerSvc::streamsToDo, got a non stream object";
        delete obj;
        throw ex;
      }
      // Change stream status
      stream->setStatus(castor::stager::STREAM_WAITDRIVE);
      cnvSvc()->updateRep(0, stream, false);
      // Fill TapePool pointer
      castor::BaseAddress ad;
      ad.setCnvSvcName("OraCnvSvc");
      ad.setCnvSvcType(castor::SVC_ORACNV);
      cnvSvc()->fillObj(&ad, obj, OBJ_TapePool);
      result.push_back(stream);
    }
    m_streamsToDoStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error in streamsToDo while retrieving list of streams."
      << std::endl << e.what();
    throw ex;
  }
  // Commit all status changes
  cnvSvc()->getConnection()->commit();
  return result;
}

// -----------------------------------------------------------------------
// selectTape
// -----------------------------------------------------------------------
castor::stager::Tape*
castor::db::ora::OraStagerSvc::selectTape(const std::string vid,
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
    oracle::occi::ResultSet *rset = m_selectTapeStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      m_selectTapeStatement->closeResultSet(rset);
      // we found nothing, so let's create the tape
      castor::stager::Tape* tape = new castor::stager::Tape();
      tape->setVid(vid);
      tape->setSide(side);
      tape->setTpmode(tpmode);
      tape->setStatus(castor::stager::TAPE_UNUSED);
      castor::BaseAddress ad;
      ad.setCnvSvcName("OraCnvSvc");
      ad.setCnvSvcType(castor::SVC_ORACNV);
      try {
        cnvSvc()->createRep(&ad, tape, false);
        return tape;
      } catch (oracle::occi::SQLException e) {
        delete tape;
        if (1 == e.getErrorCode()) {
          // if violation of unique constraint, ie means that
          // some other thread was quicker than us on the insertion
          // So let's select what was inserted
          rset = m_selectTapeStatement->executeQuery();
          if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
            // Still nothing ! Here it's a real error
            m_selectTapeStatement->closeResultSet(rset);
            castor::exception::Internal ex;
            ex.getMessage()
              << "Unable to select tape while inserting "
              << "violated unique constraint :"
              << std::endl << e.getMessage();
            throw ex;
          }
        }
        m_selectTapeStatement->closeResultSet(rset);
        // Else, "standard" error, throw exception
        castor::exception::Internal ex;
        ex.getMessage()
          << "Exception while inserting new tape in the DB :"
          << std::endl << e.getMessage();
        throw ex;
      }
    }
    // If we reach this point, then we selected successfully
    // a tape and it's id is in rset
    id = rset->getInt(1);
    m_selectTapeStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape by vid, side and tpmode :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // Now get the tape from its id
  try {
    castor::BaseAddress ad;
    ad.setId(id);
    ad.setCnvSvcName("OraCnvSvc");
    ad.setCnvSvcType(castor::SVC_ORACNV);
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
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape for id " << id  << " :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // We should never reach this point
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
        << "UPDATE SubRequest SET status = "
        << castor::stager::SUBREQUEST_WAITSCHED
        << " WHERE (status = "
        << castor::stager::SUBREQUEST_START
        << " OR status = "
        << castor::stager::SUBREQUEST_RESTART
        << " OR status = "
        << castor::stager::SUBREQUEST_RETRY
        << ") AND ROWNUM < 2 AND "
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
        << "protocol, xsize, priority, status"
        << " INTO :1, :2, :3, :4, :5 ,:6 , :7";

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
      m_subRequestToDoStatement->setAutoCommit(true);
    }
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
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in subRequestToDo."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// requestToDo
// -----------------------------------------------------------------------
castor::stager::Request*
castor::db::ora::OraStagerSvc::requestToDo
(std::vector<ObjectsIds> &types)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_requestToDoStatement) {
      std::ostringstream stmtString;
      stmtString
        << "UPDATE RequestsStatus SET status = 'PROCESSING'"
        << " WHERE status = 'NEW' AND ROWNUM < 2 AND "
        << "(SELECT type FROM Id2Type WHERE Id2Type.id = RequestsStatus.id)"
        << " IN (";
      for (std::vector<ObjectsIds>::const_iterator it = types.begin();
           it!= types.end();
           it++) {
        if (types.begin() != it) stmtString << ", ";
        stmtString << *it;
      }
      stmtString
        << ") RETURNING id INTO :1";
      m_requestToDoStatement =
        createStatement(stmtString.str());
      m_requestToDoStatement->registerOutParam
        (1, oracle::occi::OCCIDOUBLE);
      m_requestToDoStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    unsigned int nb =
      m_requestToDoStatement->executeUpdate();
    if (0 == nb) {
      // Found no Request to handle
      return 0;
    }
    // Create result
    u_signed64 id = (u_signed64)m_requestToDoStatement->getDouble(1);
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

// -----------------------------------------------------------------------
// isSubRequestToSchedule
// -----------------------------------------------------------------------
bool castor::db::ora::OraStagerSvc::isSubRequestToSchedule
(castor::stager::SubRequest* subreq)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_isSubRequestToScheduleStatement) {
      m_isSubRequestToScheduleStatement =
        createStatement(s_isSubRequestToScheduleStatementString);
      m_isSubRequestToScheduleStatement->registerOutParam
        (2, oracle::occi::OCCIINT);
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
    return 0 != m_isSubRequestToScheduleStatement->getInt(2);
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in isSubRequestToSchedule."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// getUpdateStart
// -----------------------------------------------------------------------
castor::IClient*
castor::db::ora::OraStagerSvc::getUpdateStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem,
 castor::stager::DiskCopy** diskCopy,
 std::list<castor::stager::DiskCopyForRecall*>& sources)
  throw (castor::exception::Exception) {
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
        (7, oracle::occi::OCCIDOUBLE);
      m_getUpdateStartStatement->registerOutParam
        (8, oracle::occi::OCCISTRING);
    }
    // execute the statement and see whether we found something
    m_getUpdateStartStatement->setDouble(1, subreq->id());
    m_getUpdateStartStatement->setDouble(2, fileSystem->id());
    unsigned int nb =
      m_getUpdateStartStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "getUpdateStart : unable to schedule SubRequest.";
      throw ex;
    }
    // First get the IClient
    u_signed64 clientId
      = (u_signed64)m_getUpdateStartStatement->getDouble(7);
    // If no IClient returned, fail
    if (0 == clientId) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "getUpdateStart : No client found.";
      throw ex;
    }
    // Get Client Object and cast it into IClient
    castor::IObject *iobj = cnvSvc()->getObjFromId(clientId);
    if (0 == iobj) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "GetUpdateStart : Could not get IClient object from id "
        << clientId;
      throw ex;
    }
    castor::IClient *result =
      dynamic_cast<castor::IClient*>(iobj);
    if (0 == result) {
      delete iobj;
      castor::exception::Internal ex;
      ex.getMessage()
        << "GetUpdateStart : IClient object had a bad type : "
        << castor::ObjectsIdStrings[clientId];
      throw ex;
    }
    // Check result
    u_signed64 id
      = (u_signed64)m_getUpdateStartStatement->getDouble(3);
    // If no DiskCopy returned, return
    if (0 == id) {
      *diskCopy = 0;
      cnvSvc()->commit();
      return result;
    }
    // In case a diskcopy was created in WAITTAPERECALL
    // status, create the associated TapeCopy and Segments
    unsigned int status =
      m_getUpdateStartStatement->getInt(5);
    if (status == 99) {
      // First get the DiskCopy
      castor::BaseAddress ad;
      ad.setCnvSvcName("OraCnvSvc");
      ad.setCnvSvcType(castor::SVC_ORACNV);
      ad.setId(id);
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
      createTapeCopySegmentsForRecall(dc->castorFile());
      // Cleanup
      delete dc->castorFile();
      delete dc;
      // commit and return
      cnvSvc()->commit();
      *diskCopy = 0;
      return result;
    }
    // Else create a resulting DiskCopy
    *diskCopy = new castor::stager::DiskCopy();
    (*diskCopy)->setId(id);
    (*diskCopy)->setPath(m_getUpdateStartStatement->getString(4));
    (*diskCopy)->setStatus
      ((enum castor::stager::DiskCopyStatusCodes) status);
    (*diskCopy)->setDiskcopyId(m_getUpdateStartStatement->getString(8));
    if ((*diskCopy)->status() ==
        castor::stager::DISKCOPY_WAITDISK2DISKCOPY) {
      try {
        oracle::occi::ResultSet *rs =
          m_getUpdateStartStatement->getCursor(6);
        // We don't fetch the first time since it's done
        // in the PL/SQL code
        oracle::occi::ResultSet::Status status = rs->status();
        while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
          castor::stager::DiskCopyForRecall* item =
            new castor::stager::DiskCopyForRecall();
          item->setId((u_signed64) rs->getDouble(1));
          item->setPath(rs->getString(2));
          item->setStatus((castor::stager::DiskCopyStatusCodes)rs->getInt(3));
          item->setDiskcopyId(rs->getString(4));
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
castor::IClient*
castor::db::ora::OraStagerSvc::putStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem,
 castor::stager::DiskCopy** diskCopy)
  throw (castor::exception::Exception) {
  castor::IObject *iobj = 0;
  try {
    // Check whether the statements are ok
    if (0 == m_putStartStatement) {
      m_putStartStatement =
        createStatement(s_putStartStatementString);
      m_putStartStatement->registerOutParam
        (3, oracle::occi::OCCIDOUBLE);
      m_getUpdateStartStatement->registerOutParam
        (4, oracle::occi::OCCIDOUBLE);
      m_getUpdateStartStatement->registerOutParam
        (5, oracle::occi::OCCIINT);
      m_getUpdateStartStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048);
      m_getUpdateStartStatement->registerOutParam
        (7, oracle::occi::OCCISTRING, 2048);
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
    // Check result
    u_signed64 id
      = (u_signed64)m_putStartStatement->getDouble(3);
    // If no IClient returned, return null
    if (0 == id) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "PutStart : No client found.";
      throw ex;
    }
    // Else get the client
    iobj = cnvSvc()->getObjFromId(id);
    if (0 == iobj) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "PutStart : Could not get IClient object from id "
        << id;
      throw ex;
    }
    castor::IClient *result =
      dynamic_cast<castor::IClient*>(iobj);
    if (0 == result) {
      delete iobj;
      castor::exception::Internal ex;
      ex.getMessage()
        << "PutStart : IClient object had a bad type : "
        << castor::ObjectsIdStrings[id];
      throw ex;
    }
    // Get the DiskCopy
    *diskCopy = new castor::stager::DiskCopy();
    (*diskCopy)->setId((u_signed64)m_putStartStatement->getDouble(4));
    (*diskCopy)->setStatus
      ((enum castor::stager::DiskCopyStatusCodes)
       m_putStartStatement->getInt(5));
    (*diskCopy)->setPath(m_putStartStatement->getString(6));
    (*diskCopy)->setDiskcopyId(m_putStartStatement->getString(7));
    // return
    cnvSvc()->commit();
    return result;
  } catch (oracle::occi::SQLException e) {
    if (0 != iobj) delete iobj;
    if (0 != *diskCopy) {
      delete *diskCopy;
      *diskCopy = 0;
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
// selectSvcClass
// -----------------------------------------------------------------------
castor::stager::SvcClass*
castor::db::ora::OraStagerSvc::selectSvcClass
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
    oracle::occi::ResultSet *rset = m_selectSvcClassStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectSvcClassStatement->closeResultSet(rset);
      return 0;
    }
    // Found the SvcClass, so create it in memory
    castor::stager::SvcClass* result =
      new castor::stager::SvcClass();
    result->setId((u_signed64)rset->getDouble(1));
    result->setPolicy(rset->getString(2));
    result->setNbDrives(rset->getInt(3));
    result->setName(name);
    m_selectSvcClassStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
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
castor::db::ora::OraStagerSvc::selectFileClass
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
    oracle::occi::ResultSet *rset = m_selectFileClassStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectFileClassStatement->closeResultSet(rset);
      return 0;
    }
    // Found the FileClass, so create it in memory
    castor::stager::FileClass* result =
      new castor::stager::FileClass();
    result->setId((u_signed64)rset->getDouble(1));
    result->setMinFileSize((u_signed64)rset->getDouble(2));
    result->setMaxFileSize((u_signed64)rset->getDouble(3));
    result->setNbCopies(rset->getInt(4));
    result->setName(name);
    m_selectFileClassStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select FileClass by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// selectCastorFile
// -----------------------------------------------------------------------
castor::stager::CastorFile*
castor::db::ora::OraStagerSvc::selectCastorFile
(const u_signed64 fileId,
 const std::string nsHost)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectCastorFileStatement) {
    m_selectCastorFileStatement =
      createStatement(s_selectCastorFileStatementString);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectCastorFileStatement->setDouble(1, fileId);
    m_selectCastorFileStatement->setString(2, nsHost);
    oracle::occi::ResultSet *rset = m_selectCastorFileStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectCastorFileStatement->closeResultSet(rset);
      return 0;
    }
    // Found the CastorFile, so create it in memory
    castor::stager::CastorFile* result =
      new castor::stager::CastorFile();
    result->setId((u_signed64)rset->getDouble(1));
    result->setFileId(fileId);
    result->setNsHost(nsHost);
    result->setFileSize((u_signed64)rset->getDouble(2));
    m_selectCastorFileStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select castorFile by fileId :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// selectFileSystem
// -----------------------------------------------------------------------
castor::stager::FileSystem*
castor::db::ora::OraStagerSvc::selectFileSystem
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
    oracle::occi::ResultSet *rset = m_selectFileSystemStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return 0
      m_selectFileSystemStatement->closeResultSet(rset);
      return 0;
    }
    // Found the FileSystem and the DiskServer,
    // create them in memory
    castor::stager::DiskServer* ds =
      new castor::stager::DiskServer();
    ds->setId((u_signed64)rset->getDouble(1));
    ds->setStatus
      ((enum castor::stager::DiskServerStatusCode)rset->getInt(2));
    ds->setName(diskServer);
    castor::stager::FileSystem* result =
      new castor::stager::FileSystem();
    result->setId((u_signed64)rset->getDouble(3));
    result->setFree((u_signed64)rset->getDouble(4));
    result->setWeight(rset->getDouble(5));
    result->setFsDeviation(rset->getDouble(6));
    result->setStatus
      ((enum castor::stager::FileSystemStatusCodes)rset->getInt(7));
    result->setMountPoint(mountPoint);
    result->setDiskserver(ds);
    ds->addFileSystems(result);
    m_selectFileSystemStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select FileSystem by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
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
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select DiskPool by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// selectDiskServer
// -----------------------------------------------------------------------
castor::stager::DiskServer*
castor::db::ora::OraStagerSvc::selectDiskServer
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
    m_selectDiskServerStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select DiskServer by name :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// selectTapeCopiesForMigration
// -----------------------------------------------------------------------
std::vector<castor::stager::TapeCopy*>
castor::db::ora::OraStagerSvc::selectTapeCopiesForMigration
(castor::stager::SvcClass *svcClass)
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_selectTapeCopiesForMigrationStatement) {
    m_selectTapeCopiesForMigrationStatement =
      createStatement(s_selectTapeCopiesForMigrationStatementString);
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectTapeCopiesForMigrationStatement->setDouble(1, svcClass->id());
    oracle::occi::ResultSet *rset =
      m_selectTapeCopiesForMigrationStatement->executeQuery();
    // create result
    std::vector<castor::stager::TapeCopy*> result;
    // Fill it
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1));
      castor::stager::TapeCopy* tapeCopy =
        dynamic_cast<castor::stager::TapeCopy*>(obj);
      if (0 == tapeCopy) {
        castor::exception::Internal ex;
        ex.getMessage()
          << "In method OraStagerSvc::selectTapeCopiesForMigration, "
          << "got a non tapeCopy object";
        delete obj;
        throw ex;
      }
      // Change tapeCopy status
      tapeCopy->setStatus(castor::stager::TAPECOPY_WAITINSTREAMS);
      cnvSvc()->updateRep(0, tapeCopy, false);
      result.push_back(tapeCopy);
    }
    m_selectTapeCopiesForMigrationStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select TapeCopies for migration :"
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
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateAndCheckSubRequest."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// updateRep
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::updateRep
(castor::IAddress* address, castor::IObject* object)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "OraStagerSvc implementation does not supported the updateRep method.";
  throw ex;
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
      ((vmgrTapeInfo.status & DISABLED) == ARCHIVED) ||
      ((vmgrTapeInfo.status & DISABLED) == EXPORTED)) {
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
// createTapeCopySegmentsForRecall
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::createTapeCopySegmentsForRecall
(castor::stager::CastorFile *castorFile)
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
    castor::exception::Internal e;
    e.getMessage() << "createTapeCopySegmentsForRecall : "
                   << "No valid copy found";
    throw e;
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
    segment->setBlockid(nsSegmentAttrs[i].blockid);
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
    delete seg->tape();
  }
  cnvSvc()->commit();
}

// -----------------------------------------------------------------------
// recreateCastorFile
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::db::ora::OraStagerSvc::recreateCastorFile(
castor::stager::CastorFile *castorFile)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_recreateCastorFileStatement) {
      m_recreateCastorFileStatement =
        createStatement(s_recreateCastorFileStatementString);
      m_recreateCastorFileStatement->registerOutParam
        (2, oracle::occi::OCCIDOUBLE);
      m_recreateCastorFileStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_recreateCastorFileStatement->setDouble(1, castorFile->id());
    unsigned int nb = m_recreateCastorFileStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "recreateCastorFile did not return any result.";
      throw ex;
    }
    // return
    u_signed64 id =
      (u_signed64)m_recreateCastorFileStatement->getDouble(2);
    if (0 == id) return 0;
    castor::stager::DiskCopy *result =
      new castor::stager::DiskCopy();
    result->setId(id);
    result->setStatus(castor::stager::DISKCOPY_WAITFS);
    return result;
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in recreateCastorFile."
      << std::endl << e.what();
    throw ex;
  }  
}

// -----------------------------------------------------------------------
// prepareForMigration
// -----------------------------------------------------------------------
void castor::db::ora::OraStagerSvc::prepareForMigration
(castor::stager::SubRequest *subreq,
 u_signed64 fileSize)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_prepareForMigrationStatement) {
      m_prepareForMigrationStatement =
        createStatement(s_prepareForMigrationStatementString);
      m_prepareForMigrationStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_prepareForMigrationStatement->setDouble(1, subreq->id());
    m_prepareForMigrationStatement->setDouble(2, fileSize);
    m_prepareForMigrationStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in prepareForMigration."
      << std::endl << e.what();
    throw ex;
  }
}
