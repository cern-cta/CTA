/******************************************************************************
 *                      castor/db/ora/OraCastorFileCnv.cpp
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
#include "OraCastorFileCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/IConverter.hpp"
#include "castor/IFactory.hpp"
#include "castor/IObject.hpp"
#include "castor/db/DbAddress.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/TapeCopy.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraCastorFileCnv> s_factoryOraCastorFileCnv;
const castor::IFactory<castor::IConverter>& OraCastorFileCnvFactory = 
  s_factoryOraCastorFileCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraCastorFileCnv::s_insertStatementString =
"INSERT INTO rh_CastorFile (fileId, nsHost, size, id) VALUES (:1,:2,:3,:4)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraCastorFileCnv::s_deleteStatementString =
"DELETE FROM rh_CastorFile WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraCastorFileCnv::s_selectStatementString =
"SELECT fileId, nsHost, size, id FROM rh_CastorFile WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraCastorFileCnv::s_updateStatementString =
"UPDATE rh_CastorFile SET fileId = :1, nsHost = :2, size = :3 WHERE id = :4";

/// SQL statement for type storage
const std::string castor::db::ora::OraCastorFileCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraCastorFileCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL insert statement for member diskFileCopies
const std::string castor::db::ora::OraCastorFileCnv::s_insertCastorFile2DiskCopyStatementString =
"INSERT INTO rh_CastorFile2DiskCopy (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member diskFileCopies
const std::string castor::db::ora::OraCastorFileCnv::s_deleteCastorFile2DiskCopyStatementString =
"DELETE FROM rh_CastorFile2DiskCopy WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member diskFileCopies
const std::string castor::db::ora::OraCastorFileCnv::s_CastorFile2DiskCopyStatementString =
"SELECT Child from rh_CastorFile2DiskCopy WHERE Parent = :1";

/// SQL insert statement for member copies
const std::string castor::db::ora::OraCastorFileCnv::s_insertCastorFile2TapeCopyStatementString =
"INSERT INTO rh_CastorFile2TapeCopy (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member copies
const std::string castor::db::ora::OraCastorFileCnv::s_deleteCastorFile2TapeCopyStatementString =
"DELETE FROM rh_CastorFile2TapeCopy WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member copies
const std::string castor::db::ora::OraCastorFileCnv::s_CastorFile2TapeCopyStatementString =
"SELECT Child from rh_CastorFile2TapeCopy WHERE Parent = :1";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraCastorFileCnv::OraCastorFileCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_insertCastorFile2DiskCopyStatement(0),
  m_deleteCastorFile2DiskCopyStatement(0),
  m_CastorFile2DiskCopyStatement(0),
  m_insertCastorFile2TapeCopyStatement(0),
  m_deleteCastorFile2TapeCopyStatement(0),
  m_CastorFile2TapeCopyStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraCastorFileCnv::~OraCastorFileCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_insertCastorFile2DiskCopyStatement);
    deleteStatement(m_CastorFile2DiskCopyStatement);
    deleteStatement(m_insertCastorFile2TapeCopyStatement);
    deleteStatement(m_CastorFile2TapeCopyStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertCastorFile2DiskCopyStatement = 0;
  m_deleteCastorFile2DiskCopyStatement = 0;
  m_CastorFile2DiskCopyStatement = 0;
  m_insertCastorFile2TapeCopyStatement = 0;
  m_deleteCastorFile2TapeCopyStatement = 0;
  m_CastorFile2TapeCopyStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraCastorFileCnv::ObjType() {
  return castor::stager::CastorFile::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraCastorFileCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                unsigned int type,
                                                bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::CastorFile* obj = 
    dynamic_cast<castor::stager::CastorFile*>(object);
  switch (type) {
  case castor::OBJ_DiskCopy :
    fillRepDiskCopy(obj);
    break;
  case castor::OBJ_TapeCopy :
    fillRepTapeCopy(obj);
    break;
  default :
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "fillRep called on type " << type 
                    << " on object of type " << obj->type() 
                    << ". This is meaningless.";
    throw ex;
  }
  if (autocommit) {
    cnvSvc()->getConnection()->commit();
  }
}

//------------------------------------------------------------------------------
// fillRepDiskCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillRepDiskCopy(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception) {
  // check select statement
  if (0 == m_CastorFile2DiskCopyStatement) {
    m_CastorFile2DiskCopyStatement = createStatement(s_CastorFile2DiskCopyStatementString);
  }
  // Get current database data
  std::set<int> diskFileCopiesList;
  m_CastorFile2DiskCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_CastorFile2DiskCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    diskFileCopiesList.insert(rset->getInt(1));
  }
  m_CastorFile2DiskCopyStatement->closeResultSet(rset);
  // update segments and create new ones
  for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->diskFileCopies().begin();
       it != obj->diskFileCopies().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = diskFileCopiesList.find((*it)->id())) == diskFileCopiesList.end()) {
      cnvSvc()->createRep(0, *it, false);
      if (0 == m_insertCastorFile2DiskCopyStatement) {
        m_insertCastorFile2DiskCopyStatement = createStatement(s_insertCastorFile2DiskCopyStatementString);
      }
      m_insertCastorFile2DiskCopyStatement->setDouble(1, obj->id());
      m_insertCastorFile2DiskCopyStatement->setDouble(2, (*it)->id());
      m_insertCastorFile2DiskCopyStatement->executeUpdate();
    } else {
      diskFileCopiesList.erase(item);
      cnvSvc()->updateRep(0, *it, false);
    }
  }
  // Delete old data
  for (std::set<int>::iterator it = diskFileCopiesList.begin();
       it != diskFileCopiesList.end();
       it++) {
    castor::db::DbAddress ad(*it, " ", 0);
    cnvSvc()->deleteRepByAddress(&ad, false);
    if (0 == m_deleteCastorFile2DiskCopyStatement) {
      m_deleteCastorFile2DiskCopyStatement = createStatement(s_deleteCastorFile2DiskCopyStatementString);
    }
    m_deleteCastorFile2DiskCopyStatement->setDouble(1, obj->id());
    m_deleteCastorFile2DiskCopyStatement->setDouble(2, *it);
    m_deleteCastorFile2DiskCopyStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepTapeCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillRepTapeCopy(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception) {
  // check select statement
  if (0 == m_CastorFile2TapeCopyStatement) {
    m_CastorFile2TapeCopyStatement = createStatement(s_CastorFile2TapeCopyStatementString);
  }
  // Get current database data
  std::set<int> copiesList;
  m_CastorFile2TapeCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_CastorFile2TapeCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    copiesList.insert(rset->getInt(1));
  }
  m_CastorFile2TapeCopyStatement->closeResultSet(rset);
  // update segments and create new ones
  for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->copies().begin();
       it != obj->copies().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = copiesList.find((*it)->id())) == copiesList.end()) {
      cnvSvc()->createRep(0, *it, false);
      if (0 == m_insertCastorFile2TapeCopyStatement) {
        m_insertCastorFile2TapeCopyStatement = createStatement(s_insertCastorFile2TapeCopyStatementString);
      }
      m_insertCastorFile2TapeCopyStatement->setDouble(1, obj->id());
      m_insertCastorFile2TapeCopyStatement->setDouble(2, (*it)->id());
      m_insertCastorFile2TapeCopyStatement->executeUpdate();
    } else {
      copiesList.erase(item);
      cnvSvc()->updateRep(0, *it, false);
    }
  }
  // Delete old data
  for (std::set<int>::iterator it = copiesList.begin();
       it != copiesList.end();
       it++) {
    castor::db::DbAddress ad(*it, " ", 0);
    cnvSvc()->deleteRepByAddress(&ad, false);
    if (0 == m_deleteCastorFile2TapeCopyStatement) {
      m_deleteCastorFile2TapeCopyStatement = createStatement(s_deleteCastorFile2TapeCopyStatementString);
    }
    m_deleteCastorFile2TapeCopyStatement->setDouble(1, obj->id());
    m_deleteCastorFile2TapeCopyStatement->setDouble(2, *it);
    m_deleteCastorFile2TapeCopyStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillObj(castor::IAddress* address,
                                                castor::IObject* object,
                                                unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::CastorFile* obj = 
    dynamic_cast<castor::stager::CastorFile*>(object);
  switch (type) {
  case castor::OBJ_DiskCopy :
    fillObjDiskCopy(obj);
    break;
  case castor::OBJ_TapeCopy :
    fillObjTapeCopy(obj);
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
// fillObjDiskCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillObjDiskCopy(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_CastorFile2DiskCopyStatement) {
    m_CastorFile2DiskCopyStatement = createStatement(s_CastorFile2DiskCopyStatementString);
  }
  // retrieve the object from the database
  std::set<int> diskFileCopiesList;
  m_CastorFile2DiskCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_CastorFile2DiskCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    diskFileCopiesList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_CastorFile2DiskCopyStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::DiskCopy*> toBeDeleted;
  for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->diskFileCopies().begin();
       it != obj->diskFileCopies().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = diskFileCopiesList.find((*it)->id())) == diskFileCopiesList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      diskFileCopiesList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::DiskCopy*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeDiskFileCopies(*it);
    delete (*it);
  }
  // Create new objects
  for (std::set<int>::iterator it = diskFileCopiesList.begin();
       it != diskFileCopiesList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    obj->addDiskFileCopies(dynamic_cast<castor::stager::DiskCopy*>(item));
  }
}

//------------------------------------------------------------------------------
// fillObjTapeCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillObjTapeCopy(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_CastorFile2TapeCopyStatement) {
    m_CastorFile2TapeCopyStatement = createStatement(s_CastorFile2TapeCopyStatementString);
  }
  // retrieve the object from the database
  std::set<int> copiesList;
  m_CastorFile2TapeCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_CastorFile2TapeCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    copiesList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_CastorFile2TapeCopyStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::TapeCopy*> toBeDeleted;
  for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->copies().begin();
       it != obj->copies().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = copiesList.find((*it)->id())) == copiesList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      copiesList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::TapeCopy*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeCopies(*it);
    delete (*it);
  }
  // Create new objects
  for (std::set<int>::iterator it = copiesList.begin();
       it != copiesList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    obj->addCopies(dynamic_cast<castor::stager::TapeCopy*>(item));
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::createRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::CastorFile* obj = 
    dynamic_cast<castor::stager::CastorFile*>(object);
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
    // Get an id for the new object
    obj->setId(cnvSvc()->getIds(1));
    // Now Save the current object
    m_storeTypeStatement->setDouble(1, obj->id());
    m_storeTypeStatement->setInt(2, obj->type());
    m_storeTypeStatement->executeUpdate();
    m_insertStatement->setDouble(1, obj->fileId());
    m_insertStatement->setString(2, obj->nsHost());
    m_insertStatement->setDouble(3, obj->size());
    m_insertStatement->setDouble(4, obj->id());
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
                    << "  fileId : " << obj->fileId() << std::endl
                    << "  nsHost : " << obj->nsHost() << std::endl
                    << "  size : " << obj->size() << std::endl
                    << "  id : " << obj->id() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::updateRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::CastorFile* obj = 
    dynamic_cast<castor::stager::CastorFile*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setDouble(1, obj->fileId());
    m_updateStatement->setString(2, obj->nsHost());
    m_updateStatement->setDouble(3, obj->size());
    m_updateStatement->setDouble(4, obj->id());
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
void castor::db::ora::OraCastorFileCnv::deleteRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::CastorFile* obj = 
    dynamic_cast<castor::stager::CastorFile*>(object);
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
    for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->copies().begin();
         it != obj->copies().end();
         it++) {
      cnvSvc()->deleteRep(0, *it, false);
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
castor::IObject* castor::db::ora::OraCastorFileCnv::createObj(castor::IAddress* address)
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
    castor::stager::CastorFile* object = new castor::stager::CastorFile();
    // Now retrieve and set members
    object->setFileId((unsigned long long)rset->getDouble(1));
    object->setNsHost(rset->getString(2));
    object->setSize((unsigned long long)rset->getDouble(3));
    object->setId((unsigned long long)rset->getDouble(4));
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
void castor::db::ora::OraCastorFileCnv::updateObj(castor::IObject* obj)
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
    castor::stager::CastorFile* object = 
      dynamic_cast<castor::stager::CastorFile*>(obj);
    object->setFileId((unsigned long long)rset->getDouble(1));
    object->setNsHost(rset->getString(2));
    object->setSize((unsigned long long)rset->getDouble(3));
    object->setId((unsigned long long)rset->getDouble(4));
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

