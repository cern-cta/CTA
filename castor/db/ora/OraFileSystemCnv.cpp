/******************************************************************************
 *                      castor/db/ora/OraFileSystemCnv.cpp
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
#include "OraFileSystemCnv.hpp"
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
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskPool.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/FileSystem.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraFileSystemCnv> s_factoryOraFileSystemCnv;
const castor::IFactory<castor::IConverter>& OraFileSystemCnvFactory = 
  s_factoryOraFileSystemCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraFileSystemCnv::s_insertStatementString =
"INSERT INTO rh_FileSystem (free, weight, fsDeviation, randomize, mountPoint, id, diskPool, diskserver) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraFileSystemCnv::s_deleteStatementString =
"DELETE FROM rh_FileSystem WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraFileSystemCnv::s_selectStatementString =
"SELECT free, weight, fsDeviation, randomize, mountPoint, id, diskPool, diskserver FROM rh_FileSystem WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraFileSystemCnv::s_updateStatementString =
"UPDATE rh_FileSystem SET free = :1, weight = :2, fsDeviation = :3, randomize = :4, mountPoint = :5, diskPool = :6, diskserver = :7 WHERE id = :8";

/// SQL statement for type storage
const std::string castor::db::ora::OraFileSystemCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraFileSystemCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL insert statement for member copies
const std::string castor::db::ora::OraFileSystemCnv::s_insertFileSystem2DiskCopyStatementString =
"INSERT INTO rh_FileSystem2DiskCopy (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member copies
const std::string castor::db::ora::OraFileSystemCnv::s_deleteFileSystem2DiskCopyStatementString =
"DELETE FROM rh_FileSystem2DiskCopy WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member copies
const std::string castor::db::ora::OraFileSystemCnv::s_FileSystem2DiskCopyStatementString =
"SELECT Child from rh_FileSystem2DiskCopy WHERE Parent = :1";

/// SQL insert statement for member diskserver
const std::string castor::db::ora::OraFileSystemCnv::s_insertDiskServer2FileSystemStatementString =
"INSERT INTO rh_DiskServer2FileSystem (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member diskserver
const std::string castor::db::ora::OraFileSystemCnv::s_deleteDiskServer2FileSystemStatementString =
"DELETE FROM rh_DiskServer2FileSystem WHERE Parent = :1 AND Child = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraFileSystemCnv::OraFileSystemCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_insertFileSystem2DiskCopyStatement(0),
  m_deleteFileSystem2DiskCopyStatement(0),
  m_FileSystem2DiskCopyStatement(0),
  m_insertDiskServer2FileSystemStatement(0),
  m_deleteDiskServer2FileSystemStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraFileSystemCnv::~OraFileSystemCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_insertFileSystem2DiskCopyStatement);
    deleteStatement(m_FileSystem2DiskCopyStatement);
    deleteStatement(m_insertDiskServer2FileSystemStatement);
    deleteStatement(m_deleteDiskServer2FileSystemStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertFileSystem2DiskCopyStatement = 0;
  m_deleteFileSystem2DiskCopyStatement = 0;
  m_FileSystem2DiskCopyStatement = 0;
  m_insertDiskServer2FileSystemStatement = 0;
  m_deleteDiskServer2FileSystemStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraFileSystemCnv::ObjType() {
  return castor::stager::FileSystem::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraFileSystemCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::fillRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                unsigned int type,
                                                bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::FileSystem* obj = 
    dynamic_cast<castor::stager::FileSystem*>(object);
  switch (type) {
  case castor::OBJ_DiskPool :
    fillRepDiskPool(obj);
    break;
  case castor::OBJ_DiskCopy :
    fillRepDiskCopy(obj);
    break;
  case castor::OBJ_DiskServer :
    fillRepDiskServer(obj);
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
// fillRepDiskPool
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::fillRepDiskPool(castor::stager::FileSystem* obj)
  throw (castor::exception::Exception) {
  // Check select statement
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
  u_signed64 diskPoolId = (u_signed64)rset->getDouble(6);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(diskPoolId, " ", 0);
  // Check whether old object should be deleted
  if (0 != diskPoolId &&
      0 != obj->diskPool() &&
      obj->diskPool()->id() != diskPoolId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    diskPoolId = 0;
  }
  // Update object or create new one
  if (diskPoolId == 0) {
    if (0 != obj->diskPool()) {
      cnvSvc()->createRep(&ad, obj->diskPool(), false);
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->diskPool(), false);
  }
}

//------------------------------------------------------------------------------
// fillRepDiskCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::fillRepDiskCopy(castor::stager::FileSystem* obj)
  throw (castor::exception::Exception) {
  // check select statement
  if (0 == m_FileSystem2DiskCopyStatement) {
    m_FileSystem2DiskCopyStatement = createStatement(s_FileSystem2DiskCopyStatementString);
  }
  // Get current database data
  std::set<int> copiesList;
  m_FileSystem2DiskCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_FileSystem2DiskCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    copiesList.insert(rset->getInt(1));
  }
  m_FileSystem2DiskCopyStatement->closeResultSet(rset);
  // update segments and create new ones
  for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->copies().begin();
       it != obj->copies().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = copiesList.find((*it)->id())) == copiesList.end()) {
      cnvSvc()->createRep(0, *it, false);
      if (0 == m_insertFileSystem2DiskCopyStatement) {
        m_insertFileSystem2DiskCopyStatement = createStatement(s_insertFileSystem2DiskCopyStatementString);
      }
      m_insertFileSystem2DiskCopyStatement->setDouble(1, obj->id());
      m_insertFileSystem2DiskCopyStatement->setDouble(2, (*it)->id());
      m_insertFileSystem2DiskCopyStatement->executeUpdate();
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
    if (0 == m_deleteFileSystem2DiskCopyStatement) {
      m_deleteFileSystem2DiskCopyStatement = createStatement(s_deleteFileSystem2DiskCopyStatementString);
    }
    m_deleteFileSystem2DiskCopyStatement->setDouble(1, obj->id());
    m_deleteFileSystem2DiskCopyStatement->setDouble(2, *it);
    m_deleteFileSystem2DiskCopyStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepDiskServer
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::fillRepDiskServer(castor::stager::FileSystem* obj)
  throw (castor::exception::Exception) {
  // Check select statement
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
  u_signed64 diskserverId = (u_signed64)rset->getDouble(7);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(diskserverId, " ", 0);
  // Check whether old object should be deleted
  if (0 != diskserverId &&
      0 != obj->diskserver() &&
      obj->diskserver()->id() != diskserverId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    diskserverId = 0;
    if (0 == m_deleteDiskServer2FileSystemStatement) {
      m_deleteDiskServer2FileSystemStatement = createStatement(s_deleteDiskServer2FileSystemStatementString);
    }
    m_deleteDiskServer2FileSystemStatement->setDouble(1, obj->diskserver()->id());
    m_deleteDiskServer2FileSystemStatement->setDouble(2, obj->id());
    m_deleteDiskServer2FileSystemStatement->executeUpdate();
  }
  // Update object or create new one
  if (diskserverId == 0) {
    if (0 != obj->diskserver()) {
      cnvSvc()->createRep(&ad, obj->diskserver(), false);
      if (0 == m_insertDiskServer2FileSystemStatement) {
        m_insertDiskServer2FileSystemStatement = createStatement(s_insertDiskServer2FileSystemStatementString);
      }
      m_insertDiskServer2FileSystemStatement->setDouble(1, obj->diskserver()->id());
      m_insertDiskServer2FileSystemStatement->setDouble(2, obj->id());
      m_insertDiskServer2FileSystemStatement->executeUpdate();
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->diskserver(), false);
  }
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::fillObj(castor::IAddress* address,
                                                castor::IObject* object,
                                                unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::FileSystem* obj = 
    dynamic_cast<castor::stager::FileSystem*>(object);
  switch (type) {
  case castor::OBJ_DiskPool :
    fillObjDiskPool(obj);
    break;
  case castor::OBJ_DiskCopy :
    fillObjDiskCopy(obj);
    break;
  case castor::OBJ_DiskServer :
    fillObjDiskServer(obj);
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
// fillObjDiskPool
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::fillObjDiskPool(castor::stager::FileSystem* obj)
  throw (castor::exception::Exception) {
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
  u_signed64 diskPoolId = (u_signed64)rset->getDouble(5);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->diskPool() &&
      (0 == diskPoolId ||
       obj->diskPool()->id() != diskPoolId)) {
    delete obj->diskPool();
    obj->setDiskPool(0);
  }
  // Update object or create new one
  if (0 != diskPoolId) {
    if (0 == obj->diskPool()) {
      obj->setDiskPool
        (dynamic_cast<castor::stager::DiskPool*>
         (cnvSvc()->getObjFromId(diskPoolId)));
    } else if (obj->diskPool()->id() == diskPoolId) {
      cnvSvc()->updateObj(obj->diskPool());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjDiskCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::fillObjDiskCopy(castor::stager::FileSystem* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_FileSystem2DiskCopyStatement) {
    m_FileSystem2DiskCopyStatement = createStatement(s_FileSystem2DiskCopyStatementString);
  }
  // retrieve the object from the database
  std::set<int> copiesList;
  m_FileSystem2DiskCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_FileSystem2DiskCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    copiesList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_FileSystem2DiskCopyStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::DiskCopy*> toBeDeleted;
  for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->copies().begin();
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
  for (std::vector<castor::stager::DiskCopy*>::iterator it = toBeDeleted.begin();
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
    obj->addCopies(dynamic_cast<castor::stager::DiskCopy*>(item));
  }
}

//------------------------------------------------------------------------------
// fillObjDiskServer
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::fillObjDiskServer(castor::stager::FileSystem* obj)
  throw (castor::exception::Exception) {
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
  u_signed64 diskserverId = (u_signed64)rset->getDouble(6);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->diskserver() &&
      (0 == diskserverId ||
       obj->diskserver()->id() != diskserverId)) {
    delete obj->diskserver();
    obj->setDiskserver(0);
  }
  // Update object or create new one
  if (0 != diskserverId) {
    if (0 == obj->diskserver()) {
      obj->setDiskserver
        (dynamic_cast<castor::stager::DiskServer*>
         (cnvSvc()->getObjFromId(diskserverId)));
    } else if (obj->diskserver()->id() == diskserverId) {
      cnvSvc()->updateObj(obj->diskserver());
    }
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::createRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::FileSystem* obj = 
    dynamic_cast<castor::stager::FileSystem*>(object);
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
    m_insertStatement->setDouble(1, obj->free());
    m_insertStatement->setFloat(2, obj->weight());
    m_insertStatement->setFloat(3, obj->fsDeviation());
    m_insertStatement->setInt(4, obj->randomize());
    m_insertStatement->setString(5, obj->mountPoint());
    m_insertStatement->setDouble(6, obj->id());
    m_insertStatement->setDouble(7, obj->diskPool() ? obj->diskPool()->id() : 0);
    m_insertStatement->setDouble(8, obj->diskserver() ? obj->diskserver()->id() : 0);
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
                    << "  free : " << obj->free() << std::endl
                    << "  weight : " << obj->weight() << std::endl
                    << "  fsDeviation : " << obj->fsDeviation() << std::endl
                    << "  randomize : " << obj->randomize() << std::endl
                    << "  mountPoint : " << obj->mountPoint() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  diskPool : " << obj->diskPool() << std::endl
                    << "  diskserver : " << obj->diskserver() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::updateRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::FileSystem* obj = 
    dynamic_cast<castor::stager::FileSystem*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setDouble(1, obj->free());
    m_updateStatement->setFloat(2, obj->weight());
    m_updateStatement->setFloat(3, obj->fsDeviation());
    m_updateStatement->setInt(4, obj->randomize());
    m_updateStatement->setString(5, obj->mountPoint());
    m_updateStatement->setDouble(6, obj->diskPool() ? obj->diskPool()->id() : 0);
    m_updateStatement->setDouble(7, obj->diskserver() ? obj->diskserver()->id() : 0);
    m_updateStatement->setDouble(8, obj->id());
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
void castor::db::ora::OraFileSystemCnv::deleteRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::FileSystem* obj = 
    dynamic_cast<castor::stager::FileSystem*>(object);
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
    // Delete link to diskserver object
    if (0 != obj->diskserver()) {
      // Check whether the statement is ok
      if (0 == m_deleteDiskServer2FileSystemStatement) {
        m_deleteDiskServer2FileSystemStatement = createStatement(s_deleteDiskServer2FileSystemStatementString);
      }
      // Delete links to objects
      m_deleteDiskServer2FileSystemStatement->setDouble(1, obj->diskserver()->id());
      m_deleteDiskServer2FileSystemStatement->setDouble(2, obj->id());
      m_deleteDiskServer2FileSystemStatement->executeUpdate();
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
castor::IObject* castor::db::ora::OraFileSystemCnv::createObj(castor::IAddress* address)
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
    castor::stager::FileSystem* object = new castor::stager::FileSystem();
    // Now retrieve and set members
    object->setFree((u_signed64)rset->getDouble(1));
    object->setWeight(rset->getFloat(2));
    object->setFsDeviation(rset->getFloat(3));
    object->setRandomize(rset->getInt(4));
    object->setMountPoint(rset->getString(5));
    object->setId((u_signed64)rset->getDouble(6));
    u_signed64 diskPoolId = (u_signed64)rset->getDouble(7);
    u_signed64 diskserverId = (u_signed64)rset->getDouble(8);
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
void castor::db::ora::OraFileSystemCnv::updateObj(castor::IObject* obj)
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
    castor::stager::FileSystem* object = 
      dynamic_cast<castor::stager::FileSystem*>(obj);
    object->setFree((u_signed64)rset->getDouble(1));
    object->setWeight(rset->getFloat(2));
    object->setFsDeviation(rset->getFloat(3));
    object->setRandomize(rset->getInt(4));
    object->setMountPoint(rset->getString(5));
    object->setId((u_signed64)rset->getDouble(6));
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

