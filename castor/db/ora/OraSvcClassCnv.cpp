/******************************************************************************
 *                      castor/db/ora/OraSvcClassCnv.cpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "OraSvcClassCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/ICnvFactory.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/IObject.hpp"
#include "castor/db/DbAddress.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/stager/DiskPool.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapePool.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraSvcClassCnv> s_factoryOraSvcClassCnv;
const castor::ICnvFactory& OraSvcClassCnvFactory = 
  s_factoryOraSvcClassCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraSvcClassCnv::s_insertStatementString =
"INSERT INTO SvcClass (policy, nbDrives, name, defaultFileSize, id) VALUES (:1,:2,:3,:4,:5)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraSvcClassCnv::s_deleteStatementString =
"DELETE FROM SvcClass WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraSvcClassCnv::s_selectStatementString =
"SELECT policy, nbDrives, name, defaultFileSize, id FROM SvcClass WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraSvcClassCnv::s_updateStatementString =
"UPDATE SvcClass SET policy = :1, nbDrives = :2, name = :3, defaultFileSize = :4 WHERE id = :5";

/// SQL statement for type storage
const std::string castor::db::ora::OraSvcClassCnv::s_storeTypeStatementString =
"INSERT INTO Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraSvcClassCnv::s_deleteTypeStatementString =
"DELETE FROM Id2Type WHERE id = :1";

/// SQL insert statement for member tapePools
const std::string castor::db::ora::OraSvcClassCnv::s_insertTapePoolStatementString =
"INSERT INTO SvcClass2TapePool (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member tapePools
const std::string castor::db::ora::OraSvcClassCnv::s_deleteTapePoolStatementString =
"DELETE FROM SvcClass2TapePool WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member tapePools
const std::string castor::db::ora::OraSvcClassCnv::s_selectTapePoolStatementString =
"SELECT Child from SvcClass2TapePool WHERE Parent = :1";

/// SQL insert statement for member diskPools
const std::string castor::db::ora::OraSvcClassCnv::s_insertDiskPoolStatementString =
"INSERT INTO DiskPool2SvcClass (Child, Parent) VALUES (:1, :2)";

/// SQL delete statement for member diskPools
const std::string castor::db::ora::OraSvcClassCnv::s_deleteDiskPoolStatementString =
"DELETE FROM DiskPool2SvcClass WHERE Child = :1 AND Parent = :2";

/// SQL select statement for member diskPools
const std::string castor::db::ora::OraSvcClassCnv::s_selectDiskPoolStatementString =
"SELECT Parent from DiskPool2SvcClass WHERE Child = :1";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraSvcClassCnv::OraSvcClassCnv(castor::ICnvSvc* cnvSvc) :
  OraBaseCnv(cnvSvc),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_insertTapePoolStatement(0),
  m_deleteTapePoolStatement(0),
  m_selectTapePoolStatement(0),
  m_insertDiskPoolStatement(0),
  m_deleteDiskPoolStatement(0),
  m_selectDiskPoolStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraSvcClassCnv::~OraSvcClassCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraSvcClassCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_insertTapePoolStatement);
    deleteStatement(m_deleteTapePoolStatement);
    deleteStatement(m_selectTapePoolStatement);
    deleteStatement(m_insertDiskPoolStatement);
    deleteStatement(m_deleteDiskPoolStatement);
    deleteStatement(m_selectDiskPoolStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertTapePoolStatement = 0;
  m_deleteTapePoolStatement = 0;
  m_selectTapePoolStatement = 0;
  m_insertDiskPoolStatement = 0;
  m_deleteDiskPoolStatement = 0;
  m_selectDiskPoolStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraSvcClassCnv::ObjType() {
  return castor::stager::SvcClass::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraSvcClassCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSvcClassCnv::fillRep(castor::IAddress* address,
                                              castor::IObject* object,
                                              unsigned int type,
                                              bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::SvcClass* obj = 
    dynamic_cast<castor::stager::SvcClass*>(object);
  try {
    switch (type) {
    case castor::OBJ_TapePool :
      fillRepTapePool(obj);
      break;
    case castor::OBJ_DiskPool :
      fillRepDiskPool(obj);
      break;
    default :
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "fillRep called for type " << type 
                      << " on object of type " << obj->type() 
                      << ". This is meaningless.";
      throw ex;
    }
    if (autocommit) {
      cnvSvc()->getConnection()->commit();
    }
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex; // XXX Fix it, depending on ORACLE error
    ex.getMessage() << "Error in fillRep for type " << type
                    << std::endl << e.what() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// fillRepTapePool
//------------------------------------------------------------------------------
void castor::db::ora::OraSvcClassCnv::fillRepTapePool(castor::stager::SvcClass* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // check select statement
  if (0 == m_selectTapePoolStatement) {
    m_selectTapePoolStatement = createStatement(s_selectTapePoolStatementString);
  }
  // Get current database data
  std::set<int> tapePoolsList;
  m_selectTapePoolStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectTapePoolStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    tapePoolsList.insert(rset->getInt(1));
  }
  m_selectTapePoolStatement->closeResultSet(rset);
  // update tapePools and create new ones
  for (std::vector<castor::stager::TapePool*>::iterator it = obj->tapePools().begin();
       it != obj->tapePools().end();
       it++) {
    if (0 == (*it)->id()) {
      cnvSvc()->createRep(0, *it, false);
      if (0 == m_insertTapePoolStatement) {
        m_insertTapePoolStatement = createStatement(s_insertTapePoolStatementString);
      }
      m_insertTapePoolStatement->setDouble(1, obj->id());
      m_insertTapePoolStatement->setDouble(2, (*it)->id());
      m_insertTapePoolStatement->executeUpdate();
    } else {
      std::set<int>::iterator item;
      if ((item = tapePoolsList.find((*it)->id())) != tapePoolsList.end()) {
        tapePoolsList.erase(item);
      }
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = tapePoolsList.begin();
       it != tapePoolsList.end();
       it++) {
    if (0 == m_deleteTapePoolStatement) {
      m_deleteTapePoolStatement = createStatement(s_deleteTapePoolStatementString);
    }
    m_deleteTapePoolStatement->setDouble(1, obj->id());
    m_deleteTapePoolStatement->setDouble(2, *it);
    m_deleteTapePoolStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepDiskPool
//------------------------------------------------------------------------------
void castor::db::ora::OraSvcClassCnv::fillRepDiskPool(castor::stager::SvcClass* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // check select statement
  if (0 == m_selectDiskPoolStatement) {
    m_selectDiskPoolStatement = createStatement(s_selectDiskPoolStatementString);
  }
  // Get current database data
  std::set<int> diskPoolsList;
  m_selectDiskPoolStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectDiskPoolStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    diskPoolsList.insert(rset->getInt(1));
  }
  m_selectDiskPoolStatement->closeResultSet(rset);
  // update diskPools and create new ones
  for (std::vector<castor::stager::DiskPool*>::iterator it = obj->diskPools().begin();
       it != obj->diskPools().end();
       it++) {
    if (0 == (*it)->id()) {
      cnvSvc()->createRep(0, *it, false);
      if (0 == m_insertDiskPoolStatement) {
        m_insertDiskPoolStatement = createStatement(s_insertDiskPoolStatementString);
      }
      m_insertDiskPoolStatement->setDouble(1, obj->id());
      m_insertDiskPoolStatement->setDouble(2, (*it)->id());
      m_insertDiskPoolStatement->executeUpdate();
    } else {
      std::set<int>::iterator item;
      if ((item = diskPoolsList.find((*it)->id())) != diskPoolsList.end()) {
        diskPoolsList.erase(item);
      }
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = diskPoolsList.begin();
       it != diskPoolsList.end();
       it++) {
    if (0 == m_deleteDiskPoolStatement) {
      m_deleteDiskPoolStatement = createStatement(s_deleteDiskPoolStatementString);
    }
    m_deleteDiskPoolStatement->setDouble(1, obj->id());
    m_deleteDiskPoolStatement->setDouble(2, *it);
    m_deleteDiskPoolStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraSvcClassCnv::fillObj(castor::IAddress* address,
                                              castor::IObject* object,
                                              unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::SvcClass* obj = 
    dynamic_cast<castor::stager::SvcClass*>(object);
  switch (type) {
  case castor::OBJ_TapePool :
    fillObjTapePool(obj);
    break;
  case castor::OBJ_DiskPool :
    fillObjDiskPool(obj);
    break;
  default :
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "fillObj called on type " << type 
                    << " on object of type " << obj->type() 
                    << ". This is meaningless.";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// fillObjTapePool
//------------------------------------------------------------------------------
void castor::db::ora::OraSvcClassCnv::fillObjTapePool(castor::stager::SvcClass* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectTapePoolStatement) {
    m_selectTapePoolStatement = createStatement(s_selectTapePoolStatementString);
  }
  // retrieve the object from the database
  std::set<int> tapePoolsList;
  m_selectTapePoolStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectTapePoolStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    tapePoolsList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectTapePoolStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::TapePool*> toBeDeleted;
  for (std::vector<castor::stager::TapePool*>::iterator it = obj->tapePools().begin();
       it != obj->tapePools().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = tapePoolsList.find((*it)->id())) == tapePoolsList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      tapePoolsList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::TapePool*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeTapePools(*it);
    (*it)->removeSvcClasses(obj);
  }
  // Create new objects
  for (std::set<int>::iterator it = tapePoolsList.begin();
       it != tapePoolsList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    castor::stager::TapePool* remoteObj = 
      dynamic_cast<castor::stager::TapePool*>(item);
    obj->addTapePools(remoteObj);
    remoteObj->addSvcClasses(obj);
  }
}

//------------------------------------------------------------------------------
// fillObjDiskPool
//------------------------------------------------------------------------------
void castor::db::ora::OraSvcClassCnv::fillObjDiskPool(castor::stager::SvcClass* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectDiskPoolStatement) {
    m_selectDiskPoolStatement = createStatement(s_selectDiskPoolStatementString);
  }
  // retrieve the object from the database
  std::set<int> diskPoolsList;
  m_selectDiskPoolStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectDiskPoolStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    diskPoolsList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectDiskPoolStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::DiskPool*> toBeDeleted;
  for (std::vector<castor::stager::DiskPool*>::iterator it = obj->diskPools().begin();
       it != obj->diskPools().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = diskPoolsList.find((*it)->id())) == diskPoolsList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      diskPoolsList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::DiskPool*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeDiskPools(*it);
    (*it)->removeSvcClasses(obj);
  }
  // Create new objects
  for (std::set<int>::iterator it = diskPoolsList.begin();
       it != diskPoolsList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    castor::stager::DiskPool* remoteObj = 
      dynamic_cast<castor::stager::DiskPool*>(item);
    obj->addDiskPools(remoteObj);
    remoteObj->addSvcClasses(obj);
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSvcClassCnv::createRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                bool autocommit,
                                                unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::SvcClass* obj = 
    dynamic_cast<castor::stager::SvcClass*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  if (0 != obj->id()) return;
  try {
    // Check whether the statements are ok
    if (0 == m_insertStatement) {
      m_insertStatement = createStatement(s_insertStatementString);
    }
    if (0 == m_storeTypeStatement) {
      m_storeTypeStatement = createStatement(s_storeTypeStatementString);
    }
    // Get an id for the new object
    obj->setId(cnvSvc()->getIds(1));
    // Now Save the current object
    m_storeTypeStatement->setDouble(1, obj->id());
    m_storeTypeStatement->setInt(2, obj->type());
    m_storeTypeStatement->executeUpdate();
    m_insertStatement->setString(1, obj->policy());
    m_insertStatement->setInt(2, obj->nbDrives());
    m_insertStatement->setString(3, obj->name());
    m_insertStatement->setDouble(4, obj->defaultFileSize());
    m_insertStatement->setDouble(5, obj->id());
    m_insertStatement->executeUpdate();
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
                    << "  policy : " << obj->policy() << std::endl
                    << "  nbDrives : " << obj->nbDrives() << std::endl
                    << "  name : " << obj->name() << std::endl
                    << "  defaultFileSize : " << obj->defaultFileSize() << std::endl
                    << "  id : " << obj->id() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSvcClassCnv::updateRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::SvcClass* obj = 
    dynamic_cast<castor::stager::SvcClass*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setString(1, obj->policy());
    m_updateStatement->setInt(2, obj->nbDrives());
    m_updateStatement->setString(3, obj->name());
    m_updateStatement->setDouble(4, obj->defaultFileSize());
    m_updateStatement->setDouble(5, obj->id());
    m_updateStatement->executeUpdate();
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
void castor::db::ora::OraSvcClassCnv::deleteRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::SvcClass* obj = 
    dynamic_cast<castor::stager::SvcClass*>(object);
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
    // Now Delete the object
    m_deleteTypeStatement->setDouble(1, obj->id());
    m_deleteTypeStatement->executeUpdate();
    m_deleteStatement->setDouble(1, obj->id());
    m_deleteStatement->executeUpdate();
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
castor::IObject* castor::db::ora::OraSvcClassCnv::createObj(castor::IAddress* address)
  throw (castor::exception::Exception) {
  castor::db::DbAddress* ad = 
    dynamic_cast<castor::db::DbAddress*>(address);
  try {
    // Check whether the statement is ok
    if (0 == m_selectStatement) {
      m_selectStatement = createStatement(s_selectStatementString);
    }
    // retrieve the object from the database
    m_selectStatement->setDouble(1, ad->id());
    oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::exception::NoEntry ex;
      ex.getMessage() << "No object found for id :" << ad->id();
      throw ex;
    }
    // create the new Object
    castor::stager::SvcClass* object = new castor::stager::SvcClass();
    // Now retrieve and set members
    object->setPolicy(rset->getString(1));
    object->setNbDrives(rset->getInt(2));
    object->setName(rset->getString(3));
    object->setDefaultFileSize((u_signed64)rset->getDouble(4));
    object->setId((u_signed64)rset->getDouble(5));
    m_selectStatement->closeResultSet(rset);
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
void castor::db::ora::OraSvcClassCnv::updateObj(castor::IObject* obj)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_selectStatement) {
      m_selectStatement = createStatement(s_selectStatementString);
    }
    // retrieve the object from the database
    m_selectStatement->setDouble(1, obj->id());
    oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::exception::NoEntry ex;
      ex.getMessage() << "No object found for id :" << obj->id();
      throw ex;
    }
    // Now retrieve and set members
    castor::stager::SvcClass* object = 
      dynamic_cast<castor::stager::SvcClass*>(obj);
    object->setPolicy(rset->getString(1));
    object->setNbDrives(rset->getInt(2));
    object->setName(rset->getString(3));
    object->setDefaultFileSize((u_signed64)rset->getDouble(4));
    object->setId((u_signed64)rset->getDouble(5));
    m_selectStatement->closeResultSet(rset);
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

