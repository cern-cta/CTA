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
 * @(#)$RCSfile: OraStagerSvc.cpp,v $ $Revision: 1.15 $ $Release$ $Date: 2004/10/18 13:04:04 $ $Author: sponcec3 $
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
#include "castor/db/ora/OraStagerSvc.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/stager/TapeStatusCodes.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/db/DbAddress.hpp"
#include "occi.h"

#include <string>

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
  "SELECT id FROM rh_Tape WHERE status = :1";

/// SQL statement for streamsToDo
const std::string castor::db::ora::OraStagerSvc::s_streamsToDoStatementString =
  "SELECT id FROM rh_Stream WHERE status = :1";

/// SQL statement for selectTape
const std::string castor::db::ora::OraStagerSvc::s_selectTapeStatementString =
  "SELECT id FROM rh_Tape WHERE vid = :1 AND side = :2 AND tpmode = :3 FOR UPDATE";

// -----------------------------------------------------------------------
// OraStagerSvc
// -----------------------------------------------------------------------
castor::db::ora::OraStagerSvc::OraStagerSvc(const std::string name) :
  BaseSvc(name), OraBaseObj(),
  m_tapesToDoStatement(0), m_streamsToDoStatement(0),
  m_selectTapeStatement(0) {
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
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_tapesToDoStatement = 0;
  m_streamsToDoStatement = 0;
  m_selectTapeStatement = 0;
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
    cnvSvc()->updateRep(0, searchItem, true);
  }
  return result;
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
// tapesToDo
// -----------------------------------------------------------------------
std::vector<castor::stager::Tape*>
castor::db::ora::OraStagerSvc::tapesToDo()
  throw (castor::exception::Exception) {
  // Check whether the statements are ok
  if (0 == m_tapesToDoStatement) {
    try {
      m_tapesToDoStatement = cnvSvc()->getConnection()->createStatement();
      m_tapesToDoStatement->setSQL(s_tapesToDoStatementString);
      m_tapesToDoStatement->setInt(1, castor::stager::TAPE_PENDING);
    } catch (oracle::occi::SQLException e) {
      try {
        // Always try to rollback
        cnvSvc()->getConnection()->rollback();
        if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
          // We've obviously lost the ORACLE connection here
          cnvSvc()->dropConnection();
        }
      } catch (oracle::occi::SQLException e) {
        // rollback failed, let's drop the connection for security
        cnvSvc()->dropConnection();
      }
      m_tapesToDoStatement = 0;
      castor::exception::Internal ex;
      ex.getMessage()
        << "Error in creating tapesToDo statement."
        << std::endl << e.what();
      throw ex;
    }
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
  } catch (oracle::occi::SQLException e) {
    try {
      // Always try to rollback
      cnvSvc()->getConnection()->rollback();
      if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
        // We've obviously lost the ORACLE connection here
        cnvSvc()->dropConnection();
      }
    } catch (oracle::occi::SQLException e) {
      // rollback failed, let's drop the connection for security
      cnvSvc()->dropConnection();
    }
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
    try {
      m_streamsToDoStatement = cnvSvc()->getConnection()->createStatement();
      m_streamsToDoStatement->setSQL(s_streamsToDoStatementString);
      m_streamsToDoStatement->setInt(1, castor::stager::STREAM_PENDING);
    } catch (oracle::occi::SQLException e) {
      try {
        // Always try to rollback
        cnvSvc()->getConnection()->rollback();
        if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
          // We've obviously lost the ORACLE connection here
          cnvSvc()->dropConnection();
        }
      } catch (oracle::occi::SQLException e) {
        // rollback failed, let's drop the connection for security
        cnvSvc()->dropConnection();
      }
      m_streamsToDoStatement = 0;
      castor::exception::Internal ex;
      ex.getMessage()
        << "Error in creating streamsToDo statement."
        << std::endl << e.what();
      throw ex;
    }
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
      cnvSvc()->fillRep(&ad, obj, OBJ_TapePool, false);
      result.push_back(stream);
    }
  } catch (oracle::occi::SQLException e) {
    try {
      // Always try to rollback
      cnvSvc()->getConnection()->rollback();
      if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
        // We've obviously lost the ORACLE connection here
        cnvSvc()->dropConnection();
      }
    } catch (oracle::occi::SQLException e) {
      // rollback failed, let's drop the connection for security
      cnvSvc()->dropConnection();
    }
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
    try {
      m_selectTapeStatement = cnvSvc()->getConnection()->createStatement();
      m_selectTapeStatement->setSQL(s_selectTapeStatementString);
    } catch (oracle::occi::SQLException e) {
      try {
        // Always try to rollback
        cnvSvc()->getConnection()->rollback();
        if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
          // We've obviously lost the ORACLE connection here
          cnvSvc()->dropConnection();
        }
      } catch (oracle::occi::SQLException e) {
        // rollback failed, let's drop the connection for security
        cnvSvc()->dropConnection();
      }
      m_selectTapeStatement = 0;
      castor::exception::Internal ex;
      ex.getMessage()
        << "Error in creating selectTape statement."
        << std::endl << e.what();
      throw ex;
    }
  }
  // Execute statement and get result
  unsigned long id;
  try {
    m_selectTapeStatement->setString(1, vid);
    m_selectTapeStatement->setInt(2, side);
    m_selectTapeStatement->setInt(3, tpmode);
    oracle::occi::ResultSet *rset = m_selectTapeStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
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
            castor::exception::Internal ex;
            ex.getMessage()
              << "Unable to select tape while inserting violated unique constraint :"
              << std::endl << e.getMessage();
            throw ex;
          }
        }
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
