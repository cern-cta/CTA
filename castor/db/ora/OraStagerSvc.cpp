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
 * @(#)$RCSfile: OraStagerSvc.cpp,v $ $Revision: 1.49 $ $Release$ $Date: 2004/11/23 16:27:17 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IService.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/SvcClass.hpp"
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
#include "castor/stager/TapeStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/db/DbAddress.hpp"
#include "occi.h"
#include <string>
#include <sstream>
#include <vector>

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraStagerSvc> s_factoryOraStagerSvc;
const castor::IFactory<castor::IService>& OraStagerSvcFactory = s_factoryOraStagerSvc;

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
  "BEGIN fileRecalled(:1, :2, :3); END;";

/// SQL statement for isSubRequestToSchedule
const std::string castor::db::ora::OraStagerSvc::s_isSubRequestToScheduleStatementString =
  "BEGIN isSubRequestToSchedule(:1, :2); END;";

/// SQL statement for scheduleSubRequest
const std::string castor::db::ora::OraStagerSvc::s_scheduleSubRequestStatementString =
  "BEGIN scheduleSubRequest(:1, :2, :3, :4, :5); END;";

/// SQL statement for selectSvcClass
const std::string castor::db::ora::OraStagerSvc::s_selectSvcClassStatementString =
  "SELECT id, policy, nbDrives FROM SvcClass WHERE name = :1";

/// SQL statement for selectFileClass
const std::string castor::db::ora::OraStagerSvc::s_selectFileClassStatementString =
  "SELECT id, minFileSize, maxFileSize, nbCopies FROM FileClass WHERE name = :1";

/// SQL statement for selectCastorFile
const std::string castor::db::ora::OraStagerSvc::s_selectCastorFileStatementString =
  "SELECT id, fileSize FROM CastorFile WHERE fileId = :1 AND nsHost = :2";

/// SQL statement for scheduleSubRequest
const std::string castor::db::ora::OraStagerSvc::s_updateAndCheckSubRequestStatementString =
  "BEGIN updateAndCheckSubRequest(:1, :2, :3); END;";

// -----------------------------------------------------------------------
// OraStagerSvc
// -----------------------------------------------------------------------
castor::db::ora::OraStagerSvc::OraStagerSvc(const std::string name) :
  BaseSvc(name), OraBaseObj(0),
  m_tapesToDoStatement(0), m_streamsToDoStatement(0),
  m_selectTapeStatement(0), m_anyTapeCopyForStreamStatement(0),
  m_bestTapeCopyForStreamStatement(0),
  m_bestFileSystemForSegmentStatement(0),
  m_fileRecalledStatement(0), m_subRequestToDoStatement(0),
  m_selectSvcClassStatement(0), m_selectFileClassStatement(0),
  m_selectCastorFileStatement(0), m_updateAndCheckSubRequestStatement(0) {
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
    deleteStatement(m_selectSvcClassStatement);
    deleteStatement(m_selectFileClassStatement);
    deleteStatement(m_selectCastorFileStatement);
    deleteStatement(m_updateAndCheckSubRequestStatement);
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
  m_selectSvcClassStatement = 0;
  m_selectFileClassStatement = 0;
  m_selectCastorFileStatement = 0;
  m_updateAndCheckSubRequestStatement = 0;
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
    castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
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
        (2, oracle::occi::OCCISTRING, 255);
      m_bestFileSystemForSegmentStatement->registerOutParam
        (3, oracle::occi::OCCISTRING, 255);
      m_bestFileSystemForSegmentStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 255);
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
    result->setId((u_signed64)m_bestFileSystemForSegmentStatement->getDouble(5));
    // Fill result for CastorFile
    castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
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
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
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
        (5, oracle::occi::OCCISTRING, 255);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 255);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (7, oracle::occi::OCCISTRING, 255);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (8, oracle::occi::OCCIDOUBLE);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (9, oracle::occi::OCCIDOUBLE);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (10, oracle::occi::OCCIDOUBLE);
      m_bestTapeCopyForStreamStatement->registerOutParam
        (11, oracle::occi::OCCISTRING, 255);
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
    castorFile->setId((u_signed64)m_bestTapeCopyForStreamStatement->getDouble(9));
    castorFile->setFileId((u_signed64)m_bestTapeCopyForStreamStatement->getDouble(10));
    castorFile->setNsHost(m_bestTapeCopyForStreamStatement->getString(11));
    castorFile->setFileSize((u_signed64)m_bestTapeCopyForStreamStatement->getDouble(12));
    result->setId((u_signed64)m_bestTapeCopyForStreamStatement->getDouble(13));
    diskCopy->setCastorFile(castorFile);
    castorFile->addDiskCopies(diskCopy);
    result->setCastorFile(castorFile);
    castorFile->addTapeCopies(result);
    // Fill result for TapeCopy, Segments and Tape
    cnvSvc()->updateObj(result);
    castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
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
        (2, castor::stager::SUBREQUEST_RESTART);
      m_fileRecalledStatement->setInt
        (3, castor::stager::DISKCOPY_STAGED);
      m_fileRecalledStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_fileRecalledStatement->setDouble(1, tapeCopy->id());
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
      << "Error caught in bestTapeCopyForStream."
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
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
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
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
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
              << "Unable to select tape while inserting violated unique constraint :"
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
    castor::db::DbAddress ad(id, "OraCnvSvc", castor::SVC_ORACNV);
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
        << ") RETURNING id, retryCounter, fileName, protocol, xsize, priority, status"
        << " INTO :1, :2, :3, :4, :5 ,:6 , :7";

      m_subRequestToDoStatement =
        createStatement(stmtString.str());
      m_subRequestToDoStatement->registerOutParam
        (1, oracle::occi::OCCIDOUBLE);
      m_subRequestToDoStatement->registerOutParam
        (2, oracle::occi::OCCIINT);
      m_subRequestToDoStatement->registerOutParam
        (3, oracle::occi::OCCISTRING, 255);
      m_subRequestToDoStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 255);
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
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in subRequestToDo."
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
        << "isSubRequestToSchedule : unable to know whether SubRequest should be scheduled.";
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
// scheduleSubRequest
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::db::ora::OraStagerSvc::scheduleSubRequest
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem,
 std::list<castor::stager::DiskCopyForRecall*>& sources)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_scheduleSubRequestStatement) {
      m_scheduleSubRequestStatement =
        createStatement(s_scheduleSubRequestStatementString);
      m_scheduleSubRequestStatement->registerOutParam
        (3, oracle::occi::OCCIDOUBLE);
      m_scheduleSubRequestStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 255);
      m_scheduleSubRequestStatement->registerOutParam
        (5, oracle::occi::OCCIINT);
      m_scheduleSubRequestStatement->registerOutParam
        (6, oracle::occi::OCCICURSOR);
    }
    // execute the statement and see whether we found something
    m_scheduleSubRequestStatement->setDouble(1, subreq->id());
    m_scheduleSubRequestStatement->setDouble(2, fileSystem->id());
    unsigned int nb =
      m_scheduleSubRequestStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "scheduleSubRequest : unable to schedule SubRequest.";
      throw ex;
    }
    // Create result
    castor::stager::DiskCopy* result =
      new castor::stager::DiskCopy();
    result->setId((u_signed64)m_scheduleSubRequestStatement->getDouble(3));
    result->setPath(m_scheduleSubRequestStatement->getString(4));
    result->setStatus
      ((enum castor::stager::DiskCopyStatusCodes)
       m_scheduleSubRequestStatement->getInt(5));
    oracle::occi::ResultSet *rs =
      m_scheduleSubRequestStatement->getCursor(6);
    oracle::occi::ResultSet::Status status = rs->status(); 
    status = rs->next();
    while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::stager::DiskCopyForRecall* item =
        new castor::stager::DiskCopyForRecall();
      item->setId((u_signed64) rs->getDouble(1));
      item->setPath(rs->getString(2));
      item->setStatus((castor::stager::DiskCopyStatusCodes)rs->getInt(3));
      sources.push_back(item);
      status = rs->next();
    }
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in scheduleSubRequest."
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
    m_selectSvcClassStatement = createStatement(s_selectSvcClassStatementString);
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
    m_selectFileClassStatement = createStatement(s_selectFileClassStatementString);
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
    m_selectCastorFileStatement = createStatement(s_selectCastorFileStatementString);
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

