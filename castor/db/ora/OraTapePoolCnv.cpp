/******************************************************************************
 *                      castor/db/ora/OraTapePoolCnv.cpp
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
#include "OraTapePoolCnv.hpp"
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
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapePool.hpp"
#include <list>
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraTapePoolCnv> s_factoryOraTapePoolCnv;
const castor::IFactory<castor::IConverter>& OraTapePoolCnvFactory = 
  s_factoryOraTapePoolCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraTapePoolCnv::s_insertStatementString =
"INSERT INTO rh_TapePool (name, id) VALUES (:1,:2)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraTapePoolCnv::s_deleteStatementString =
"DELETE FROM rh_TapePool WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraTapePoolCnv::s_selectStatementString =
"SELECT name, id FROM rh_TapePool WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraTapePoolCnv::s_updateStatementString =
"UPDATE rh_TapePool SET name = :1 WHERE id = :2";

/// SQL statement for type storage
const std::string castor::db::ora::OraTapePoolCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraTapePoolCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL select statement for member tapes
const std::string castor::db::ora::OraTapePoolCnv::s_TapePool2TapeStatementString =
"SELECT Child from rh_TapePool2Tape WHERE Parent = :1";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraTapePoolCnv::OraTapePoolCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_TapePool2TapeStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraTapePoolCnv::~OraTapePoolCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraTapePoolCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_TapePool2TapeStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_TapePool2TapeStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraTapePoolCnv::ObjType() {
  return castor::stager::TapePool::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraTapePoolCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapePoolCnv::createRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                castor::ObjectSet& alreadyDone,
                                                bool autocommit,
                                                bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::TapePool* obj = 
    dynamic_cast<castor::stager::TapePool*>(object);
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
      for (std::vector<castor::stager::Tape*>::iterator it = obj->tapes().begin();
           it != obj->tapes().end();
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
    m_insertStatement->setString(1, obj->name());
    m_insertStatement->setInt(2, obj->id());
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
                    << "  name : " << obj->name() << std::endl
                    << "  id : " << obj->id() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapePoolCnv::updateRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                castor::ObjectSet& alreadyDone,
                                                bool autocommit,
                                                bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::TapePool* obj = 
    dynamic_cast<castor::stager::TapePool*>(object);
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
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Now Update the current object
    m_updateStatement->setString(1, obj->name());
    m_updateStatement->setInt(2, obj->id());
    m_updateStatement->executeUpdate();
    if (recursive) {
      // Dealing with tapes
      {
        if (0 == m_TapePool2TapeStatement) {
          m_TapePool2TapeStatement = createStatement(s_TapePool2TapeStatementString);
        }
        std::set<int> tapesList;
        m_TapePool2TapeStatement->setInt(1, obj->id());
        oracle::occi::ResultSet *rset = m_TapePool2TapeStatement->executeQuery();
        while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
          tapesList.insert(rset->getInt(1));
        }
        m_TapePool2TapeStatement->closeResultSet(rset);
        for (std::vector<castor::stager::Tape*>::iterator it = obj->tapes().begin();
             it != obj->tapes().end();
             it++) {
          std::set<int>::iterator item;
          if ((item = tapesList.find((*it)->id())) == tapesList.end()) {
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->createRep(0, *it, alreadyDone, false, true);
            }
          } else {
            tapesList.erase(item);
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->updateRep(0, *it, alreadyDone, false, recursive);
            }
          }
        }
        for (std::set<int>::iterator it = tapesList.begin();
             it != tapesList.end();
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
void castor::db::ora::OraTapePoolCnv::deleteRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                castor::ObjectSet& alreadyDone,
                                                bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::TapePool* obj = 
    dynamic_cast<castor::stager::TapePool*>(object);
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
    for (std::vector<castor::stager::Tape*>::iterator it = obj->tapes().begin();
         it != obj->tapes().end();
         it++) {
      if (alreadyDone.find(*it) == alreadyDone.end()) {
        cnvSvc()->deleteRep(0, *it, alreadyDone, false);
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
castor::IObject* castor::db::ora::OraTapePoolCnv::createObj(castor::IAddress* address,
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
    castor::stager::TapePool* object = new castor::stager::TapePool();
    // Now retrieve and set members
    object->setName(rset->getString(1));
    object->setId(rset->getInt(2));
    newlyCreated[object->id()] = object;
    m_selectStatement->closeResultSet(rset);
    // Get ids of objs to retrieve
    if (0 == m_TapePool2TapeStatement) {
      m_TapePool2TapeStatement = createStatement(s_TapePool2TapeStatementString);
    }
    m_TapePool2TapeStatement->setInt(1, ad->id());
    rset = m_TapePool2TapeStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1), newlyCreated);
      object->addTapes(dynamic_cast<castor::stager::Tape*>(obj));
    }
    m_TapePool2TapeStatement->closeResultSet(rset);
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
void castor::db::ora::OraTapePoolCnv::updateObj(castor::IObject* obj,
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
    castor::stager::TapePool* object = 
      dynamic_cast<castor::stager::TapePool*>(obj);
    object->setName(rset->getString(1));
    object->setId(rset->getInt(2));
    alreadyDone[obj->id()] = obj;
    m_selectStatement->closeResultSet(rset);
    // Deal with tapes
    if (0 == m_TapePool2TapeStatement) {
      m_TapePool2TapeStatement = createStatement(s_TapePool2TapeStatementString);
    }
    std::set<int> tapesList;
    m_TapePool2TapeStatement->setInt(1, obj->id());
    rset = m_TapePool2TapeStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      tapesList.insert(rset->getInt(1));
    }
    m_TapePool2TapeStatement->closeResultSet(rset);
    {
      std::vector<castor::stager::Tape*> toBeDeleted;
      for (std::vector<castor::stager::Tape*>::iterator it = object->tapes().begin();
           it != object->tapes().end();
           it++) {
        std::set<int>::iterator item;
        if ((item = tapesList.find((*it)->id())) == tapesList.end()) {
          toBeDeleted.push_back(*it);
        } else {
          tapesList.erase(item);
          cnvSvc()->updateObj((*it), alreadyDone);
        }
      }
      for (std::vector<castor::stager::Tape*>::iterator it = toBeDeleted.begin();
           it != toBeDeleted.end();
           it++) {
        object->removeTapes(*it);
        delete (*it);
      }
    }
    for (std::set<int>::iterator it = tapesList.begin();
         it != tapesList.end();
         it++) {
      IObject* item = cnvSvc()->getObjFromId(*it, alreadyDone);
      object->addTapes(dynamic_cast<castor::stager::Tape*>(item));
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

