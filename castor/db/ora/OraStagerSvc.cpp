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
 * @(#)$RCSfile: OraStagerSvc.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2004/06/25 13:31:35 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IService.hpp"
#include "castor/IFactory.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/db/ora/OraStagerSvc.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/stager/TapeStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
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

// -----------------------------------------------------------------------
// OraStagerSvc
// -----------------------------------------------------------------------
castor::db::ora::OraStagerSvc::OraStagerSvc(const std::string name) :
  BaseSvc(name), OraBaseObj(), m_tapesToDoStatement(0) {
}

// -----------------------------------------------------------------------
// ~OraStagerSvc
// -----------------------------------------------------------------------
castor::db::ora::OraStagerSvc::~OraStagerSvc() {
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
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_tapesToDoStatement = 0;
}

// -----------------------------------------------------------------------
// segmentsForTape
// -----------------------------------------------------------------------
std::vector<castor::stager::Segment*>
castor::db::ora::OraStagerSvc::segmentsForTape
(castor::stager::Tape* searchItem)
  throw (castor::exception::Exception) {
  castor::ObjectCatalog alreadyDone;
  cnvSvc()->updateObj(searchItem, alreadyDone);
  std::vector<castor::stager::Segment*> result;
  std::vector<castor::stager::Segment*>::iterator it;
  for (it = searchItem->segments().begin();
       it != searchItem->segments().end();
       it++) {
    switch ((*it)->status()) {
    case castor::stager::SEGMENT_UNPROCESSED :
    case castor::stager::SEGMENT_WAITFSEQ :
    case castor::stager::SEGMENT_WAITPATH :
    case castor::stager::SEGMENT_WAITCOPY :
      result.push_back(*it);
      searchItem->setStatus(castor::stager::TAPE_MOUNTED);
      break;
    default:
      // Do not consider other status
      break;
    }
  }
  if (result.size() > 0) {
    castor::ObjectSet alreadyDone;
    cnvSvc()->updateRep(0, searchItem, alreadyDone, true);
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
    castor::ObjectSet alreadyDone;
    cnvSvc()->updateRep(0, searchItem, alreadyDone, true);
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
  castor::ObjectCatalog done;
  try {
    oracle::occi::ResultSet *rset = m_tapesToDoStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1), done);
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
      tape->setStatus(castor::stager::TAPE_WAITVDQM);
      castor::ObjectSet alreadyDone;
      cnvSvc()->updateRep(0, tape, alreadyDone, false);
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
