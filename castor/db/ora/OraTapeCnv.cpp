/******************************************************************************
 *                      castor/db/ora/OraTapeCnv.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "OraTapeCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/IAddress.hpp"
#include "castor/IConverter.hpp"
#include "castor/IFactory.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/db/DbAddress.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/stager/TapeStatusCodes.hpp"
#include <list>
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraTapeCnv> s_factoryOraTapeCnv;
const castor::IFactory<castor::IConverter>& OraTapeCnvFactory = 
  s_factoryOraTapeCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraTapeCnv::s_insertStatementString =
"INSERT INTO rh_Tape (vid, side, tpmode, errMsgTxt, errorCode, severity, vwAddress, id, pool, status) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraTapeCnv::s_deleteStatementString =
"DELETE FROM rh_Tape WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraTapeCnv::s_selectStatementString =
"SELECT vid, side, tpmode, errMsgTxt, errorCode, severity, vwAddress, id, pool, status FROM rh_Tape WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraTapeCnv::s_updateStatementString =
"UPDATE rh_Tape SET vid = :1, side = :2, tpmode = :3, errMsgTxt = :4, errorCode = :5, severity = :6, vwAddress = :7, pool = :8, status = :9 WHERE id = :10";

/// SQL statement for type storage
const std::string castor::db::ora::OraTapeCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraTapeCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL select statement for member segments
const std::string castor::db::ora::OraTapeCnv::s_Tape2SegmentStatementString =
"SELECT Child from rh_Tape2Segment WHERE Parent = :1";

/// SQL insert statement for member pool
const std::string castor::db::ora::OraTapeCnv::s_insertTapePool2TapeStatementString =
"INSERT INTO rh_TapePool2Tape (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member pool
const std::string castor::db::ora::OraTapeCnv::s_deleteTapePool2TapeStatementString =
"DELETE FROM rh_TapePool2Tape WHERE Parent = :1 AND Child = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraTapeCnv::OraTapeCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_Tape2SegmentStatement(0),
  m_insertTapePool2TapeStatement(0),
  m_deleteTapePool2TapeStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraTapeCnv::~OraTapeCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_Tape2SegmentStatement);
    deleteStatement(m_insertTapePool2TapeStatement);
    deleteStatement(m_deleteTapePool2TapeStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_Tape2SegmentStatement = 0;
  m_insertTapePool2TapeStatement = 0;
  m_deleteTapePool2TapeStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraTapeCnv::ObjType() {
  return castor::stager::Tape::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraTapeCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCnv::createRep(castor::IAddress* address,
                                            castor::IObject* object,
                                            castor::ObjectSet& alreadyDone,
                                            bool autocommit,
                                            bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::Tape* obj = 
    dynamic_cast<castor::stager::Tape*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_insertStatement) {
      m_insertStatement = createStatement(s_insertStatementString);
    }
    if (0 == m_storeTypeStatement) {
      m_storeTypeStatement = createStatement(s_storeTypeStatementString);
    }
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Set ids of all objects
    int nids = obj->id() == 0 ? 1 : 0;
    // check which objects need to be saved/updated and keeps a list of them
    std::list<castor::IObject*> toBeSaved;
    std::list<castor::IObject*> toBeUpdated;
    if (recursive) {
      for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
           it != obj->segments().end();
           it++) {
        if (alreadyDone.find(*it) == alreadyDone.end()) {
          if (0 == (*it)->id()) {
            toBeSaved.push_back(*it);
            nids++;
          } else {
            toBeUpdated.push_back(*it);
          }
        }
      }
    }
    if (alreadyDone.find(obj->pool()) == alreadyDone.end() &&
        obj->pool() != 0) {
      if (0 == obj->pool()->id()) {
        if (!recursive) {
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "CreateNoRep called on type Tape while its pool does not exist in the database.";
          throw ex;
        }
        toBeSaved.push_back(obj->pool());
        nids++;
      } else {
        if (recursive) {
          toBeUpdated.push_back(obj->pool());
        }
      }
    }
    unsigned long id = cnvSvc()->getIds(nids);
    if (0 == obj->id()) obj->setId(id++);
    for (std::list<castor::IObject*>::const_iterator it = toBeSaved.begin();
         it != toBeSaved.end();
         it++) {
      (*it)->setId(id++);
    }
    // Now Save the current object
    m_storeTypeStatement->setInt(1, obj->id());
    m_storeTypeStatement->setInt(2, obj->type());
    m_storeTypeStatement->executeUpdate();
    m_insertStatement->setString(1, obj->vid());
    m_insertStatement->setInt(2, obj->side());
    m_insertStatement->setInt(3, obj->tpmode());
    m_insertStatement->setString(4, obj->errMsgTxt());
    m_insertStatement->setInt(5, obj->errorCode());
    m_insertStatement->setInt(6, obj->severity());
    m_insertStatement->setString(7, obj->vwAddress());
    m_insertStatement->setInt(8, obj->id());
    m_insertStatement->setInt(9, obj->pool() ? obj->pool()->id() : 0);
    m_insertStatement->setInt(10, (int)obj->status());
    m_insertStatement->executeUpdate();
    if (recursive) {
      // Save dependant objects that need it
      for (std::list<castor::IObject*>::iterator it = toBeSaved.begin();
           it != toBeSaved.end();
           it++) {
        cnvSvc()->createRep(0, *it, alreadyDone, false, true);
      }
      // Update dependant objects that need it
      for (std::list<castor::IObject*>::iterator it = toBeUpdated.begin();
           it != toBeUpdated.end();
           it++) {
        cnvSvc()->updateRep(0, *it, alreadyDone, false, true);
      }
    }
    // Deal with pool
    if (0 != obj->pool()) {
      if (0 == m_insertTapePool2TapeStatement) {
        m_insertTapePool2TapeStatement = createStatement(s_insertTapePool2TapeStatementString);
      }
      m_insertTapePool2TapeStatement->setInt(1, obj->pool()->id());
      m_insertTapePool2TapeStatement->setInt(2, obj->id());
      m_insertTapePool2TapeStatement->executeUpdate();
    }
    if (autocommit) {
      cnvSvc()->getConnection()->commit();
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
    castor::exception::InvalidArgument ex; // XXX Fix it, depending on ORACLE error
    ex.getMessage() << "Error in insert request :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_insertStatementString << std::endl
                    << "and parameters' values were :" << std::endl
                    << "  vid : " << obj->vid() << std::endl
                    << "  side : " << obj->side() << std::endl
                    << "  tpmode : " << obj->tpmode() << std::endl
                    << "  errMsgTxt : " << obj->errMsgTxt() << std::endl
                    << "  errorCode : " << obj->errorCode() << std::endl
                    << "  severity : " << obj->severity() << std::endl
                    << "  vwAddress : " << obj->vwAddress() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  pool : " << obj->pool() << std::endl
                    << "  status : " << obj->status() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCnv::updateRep(castor::IAddress* address,
                                            castor::IObject* object,
                                            castor::ObjectSet& alreadyDone,
                                            bool autocommit,
                                            bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::Tape* obj = 
    dynamic_cast<castor::stager::Tape*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    if (0 == m_updateStatement) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to create statement :" << std::endl
                      << s_updateStatementString;
      throw ex;
    }
    if (recursive) {
      if (0 == m_selectStatement) {
        m_selectStatement = createStatement(s_selectStatementString);
      }
      if (0 == m_selectStatement) {
        castor::exception::Internal ex;
        ex.getMessage() << "Unable to create statement :" << std::endl
                        << s_selectStatementString;
        throw ex;
      }
    }
    // Mark the current object as done
    alreadyDone.insert(obj);
    if (recursive) {
      // retrieve the object from the database
      m_selectStatement->setInt(1, obj->id());
      oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();
      if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
        castor::exception::NoEntry ex;
        ex.getMessage() << "No object found for id :" << obj->id();
        throw ex;
      }
      // Dealing with pool
      {
        unsigned long poolId = rset->getInt(9);
        castor::db::DbAddress ad(poolId, " ", 0);
        if (0 != poolId &&
            0 != obj->pool() &&
            obj->pool()->id() != poolId) {
          cnvSvc()->deleteRepByAddress(&ad, false);
          poolId = 0;
          if (0 == m_deleteTapePool2TapeStatement) {
            m_deleteTapePool2TapeStatement = createStatement(s_deleteTapePool2TapeStatementString);
          }
          m_deleteTapePool2TapeStatement->setInt(1, obj->pool()->id());
          m_deleteTapePool2TapeStatement->setInt(2, obj->id());
          m_deleteTapePool2TapeStatement->executeUpdate();
        }
        if (poolId == 0) {
          if (0 != obj->pool()) {
            if (alreadyDone.find(obj->pool()) == alreadyDone.end()) {
              cnvSvc()->createRep(&ad, obj->pool(), alreadyDone, false, true);
              if (0 == m_insertTapePool2TapeStatement) {
                m_insertTapePool2TapeStatement = createStatement(s_insertTapePool2TapeStatementString);
              }
              m_insertTapePool2TapeStatement->setInt(1, obj->pool()->id());
              m_insertTapePool2TapeStatement->setInt(2, obj->id());
              m_insertTapePool2TapeStatement->executeUpdate();
            }
          }
        } else {
          if (alreadyDone.find(obj->pool()) == alreadyDone.end()) {
            cnvSvc()->updateRep(&ad, obj->pool(), alreadyDone, false, recursive);
          }
        }
      }
      m_selectStatement->closeResultSet(rset);
    }
    // Now Update the current object
    m_updateStatement->setString(1, obj->vid());
    m_updateStatement->setInt(2, obj->side());
    m_updateStatement->setInt(3, obj->tpmode());
    m_updateStatement->setString(4, obj->errMsgTxt());
    m_updateStatement->setInt(5, obj->errorCode());
    m_updateStatement->setInt(6, obj->severity());
    m_updateStatement->setString(7, obj->vwAddress());
    m_updateStatement->setInt(8, obj->pool() ? obj->pool()->id() : 0);
    m_updateStatement->setInt(9, (int)obj->status());
    m_updateStatement->setInt(10, obj->id());
    m_updateStatement->executeUpdate();
    if (recursive) {
      // Dealing with segments
      {
        if (0 == m_Tape2SegmentStatement) {
          m_Tape2SegmentStatement = createStatement(s_Tape2SegmentStatementString);
        }
        std::set<int> segmentsList;
        m_Tape2SegmentStatement->setInt(1, obj->id());
        oracle::occi::ResultSet *rset = m_Tape2SegmentStatement->executeQuery();
        while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
          segmentsList.insert(rset->getInt(1));
        }
        m_Tape2SegmentStatement->closeResultSet(rset);
        for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
             it != obj->segments().end();
             it++) {
          std::set<int>::iterator item;
          if ((item = segmentsList.find((*it)->id())) == segmentsList.end()) {
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->createRep(0, *it, alreadyDone, false, true);
            }
          } else {
            segmentsList.erase(item);
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->updateRep(0, *it, alreadyDone, false, recursive);
            }
          }
        }
        for (std::set<int>::iterator it = segmentsList.begin();
             it != segmentsList.end();
             it++) {
          castor::db::DbAddress ad(*it, " ", 0);
          cnvSvc()->deleteRepByAddress(&ad, false);
        }
      }
    }
    if (autocommit) {
      cnvSvc()->getConnection()->commit();
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
    castor::exception::InvalidArgument ex; // XXX Fix it, depending on ORACLE error
    ex.getMessage() << "Error in update request :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_updateStatementString << std::endl
                    << "and id was " << obj->id() << std::endl;;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// deleteRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCnv::deleteRep(castor::IAddress* address,
                                            castor::IObject* object,
                                            castor::ObjectSet& alreadyDone,
                                            bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::Tape* obj = 
    dynamic_cast<castor::stager::Tape*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_deleteStatement) {
      m_deleteStatement = createStatement(s_deleteStatementString);
    }
    if (0 == m_deleteTypeStatement) {
      m_deleteTypeStatement = createStatement(s_deleteTypeStatementString);
    }
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Now Delete the object
    m_deleteTypeStatement->setInt(1, obj->id());
    m_deleteTypeStatement->executeUpdate();
    m_deleteStatement->setInt(1, obj->id());
    m_deleteStatement->executeUpdate();
    for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
         it != obj->segments().end();
         it++) {
      if (alreadyDone.find(*it) == alreadyDone.end()) {
        cnvSvc()->deleteRep(0, *it, alreadyDone, false);
      }
    }
    // Delete link to pool object
    if (0 != obj->pool()) {
      // Check whether the statement is ok
      if (0 == m_deleteTapePool2TapeStatement) {
        m_deleteTapePool2TapeStatement = createStatement(s_deleteTapePool2TapeStatementString);
      }
      // Delete links to objects
      m_deleteTapePool2TapeStatement->setInt(1, obj->pool()->id());
      m_deleteTapePool2TapeStatement->setInt(2, obj->id());
      m_deleteTapePool2TapeStatement->executeUpdate();
    }
    if (autocommit) {
      cnvSvc()->getConnection()->commit();
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
    castor::exception::InvalidArgument ex; // XXX Fix it, depending on ORACLE error
    ex.getMessage() << "Error in delete request :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_deleteStatementString << std::endl
                    << "and id was " << obj->id() << std::endl;;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createObj
//------------------------------------------------------------------------------
castor::IObject* castor::db::ora::OraTapeCnv::createObj(castor::IAddress* address,
                                                        castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  castor::db::DbAddress* ad = 
    dynamic_cast<castor::db::DbAddress*>(address);
  try {
    // Check whether the statement is ok
    if (0 == m_selectStatement) {
      m_selectStatement = createStatement(s_selectStatementString);
    }
    if (0 == m_selectStatement) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to create statement :" << std::endl
                      << s_selectStatementString;
      throw ex;
    }
    // retrieve the object from the database
    m_selectStatement->setInt(1, ad->id());
    oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::exception::NoEntry ex;
      ex.getMessage() << "No object found for id :" << ad->id();
      throw ex;
    }
    // create the new Object
    castor::stager::Tape* object = new castor::stager::Tape();
    // Now retrieve and set members
    object->setVid(rset->getString(1));
    object->setSide(rset->getInt(2));
    object->setTpmode(rset->getInt(3));
    object->setErrMsgTxt(rset->getString(4));
    object->setErrorCode(rset->getInt(5));
    object->setSeverity(rset->getInt(6));
    object->setVwAddress(rset->getString(7));
    object->setId(rset->getInt(8));
    newlyCreated[object->id()] = object;
    unsigned long poolId = rset->getInt(9);
    IObject* objPool = cnvSvc()->getObjFromId(poolId, newlyCreated);
    object->setPool(dynamic_cast<castor::stager::TapePool*>(objPool));
    object->setStatus((enum castor::stager::TapeStatusCodes)rset->getInt(10));
    m_selectStatement->closeResultSet(rset);
    // Get ids of objs to retrieve
    if (0 == m_Tape2SegmentStatement) {
      m_Tape2SegmentStatement = createStatement(s_Tape2SegmentStatementString);
    }
    m_Tape2SegmentStatement->setInt(1, ad->id());
    rset = m_Tape2SegmentStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1), newlyCreated);
      object->addSegments(dynamic_cast<castor::stager::Segment*>(obj));
    }
    m_Tape2SegmentStatement->closeResultSet(rset);
    return object;
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
    castor::exception::InvalidArgument ex; // XXX Fix it, depending on ORACLE error
    ex.getMessage() << "Error in select request :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_selectStatementString << std::endl
                    << "and id was " << ad->id() << std::endl;;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCnv::updateObj(castor::IObject* obj,
                                            castor::ObjectCatalog& alreadyDone)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_selectStatement) {
      m_selectStatement = createStatement(s_selectStatementString);
    }
    if (0 == m_selectStatement) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to create statement :" << std::endl
                      << s_selectStatementString;
      throw ex;
    }
    // retrieve the object from the database
    m_selectStatement->setInt(1, obj->id());
    oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::exception::NoEntry ex;
      ex.getMessage() << "No object found for id :" << obj->id();
      throw ex;
    }
    // Now retrieve and set members
    castor::stager::Tape* object = 
      dynamic_cast<castor::stager::Tape*>(obj);
    object->setVid(rset->getString(1));
    object->setSide(rset->getInt(2));
    object->setTpmode(rset->getInt(3));
    object->setErrMsgTxt(rset->getString(4));
    object->setErrorCode(rset->getInt(5));
    object->setSeverity(rset->getInt(6));
    object->setVwAddress(rset->getString(7));
    object->setId(rset->getInt(8));
    alreadyDone[obj->id()] = obj;
    // Dealing with pool
    unsigned long poolId = rset->getInt(9);
    if (0 != object->pool() &&
        (0 == poolId ||
         object->pool()->id() != poolId)) {
      delete object->pool();
      object->setPool(0);
    }
    if (0 != poolId) {
      if (0 == object->pool()) {
        object->setPool
          (dynamic_cast<castor::stager::TapePool*>
           (cnvSvc()->getObjFromId(poolId, alreadyDone)));
      } else if (object->pool()->id() == poolId) {
        if (alreadyDone.find(object->pool()->id()) == alreadyDone.end()) {
          cnvSvc()->updateObj(object->pool(), alreadyDone);
        }
      }
    }
    object->setStatus((enum castor::stager::TapeStatusCodes)rset->getInt(10));
    m_selectStatement->closeResultSet(rset);
    // Deal with segments
    if (0 == m_Tape2SegmentStatement) {
      m_Tape2SegmentStatement = createStatement(s_Tape2SegmentStatementString);
    }
    std::set<int> segmentsList;
    m_Tape2SegmentStatement->setInt(1, obj->id());
    rset = m_Tape2SegmentStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      segmentsList.insert(rset->getInt(1));
    }
    m_Tape2SegmentStatement->closeResultSet(rset);
    {
      std::vector<castor::stager::Segment*> toBeDeleted;
      for (std::vector<castor::stager::Segment*>::iterator it = object->segments().begin();
           it != object->segments().end();
           it++) {
        std::set<int>::iterator item;
        if ((item = segmentsList.find((*it)->id())) == segmentsList.end()) {
          toBeDeleted.push_back(*it);
        } else {
          segmentsList.erase(item);
          cnvSvc()->updateObj((*it), alreadyDone);
        }
      }
      for (std::vector<castor::stager::Segment*>::iterator it = toBeDeleted.begin();
           it != toBeDeleted.end();
           it++) {
        object->removeSegments(*it);
        delete (*it);
      }
    }
    for (std::set<int>::iterator it = segmentsList.begin();
         it != segmentsList.end();
         it++) {
      IObject* item = cnvSvc()->getObjFromId(*it, alreadyDone);
      object->addSegments(dynamic_cast<castor::stager::Segment*>(item));
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
    castor::exception::InvalidArgument ex; // XXX Fix it, depending on ORACLE error
    ex.getMessage() << "Error in update request :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_updateStatementString << std::endl
                    << "and id was " << obj->id() << std::endl;;
    throw ex;
  }
}

