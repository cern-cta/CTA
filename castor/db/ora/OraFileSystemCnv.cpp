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
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/FileSystem.hpp"
#include <list>
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
"INSERT INTO rh_FileSystem (free, weight, fsDeviation, randomize, mountPoint, id, diskserver) VALUES (:1,:2,:3,:4,:5,:6,:7)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraFileSystemCnv::s_deleteStatementString =
"DELETE FROM rh_FileSystem WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraFileSystemCnv::s_selectStatementString =
"SELECT free, weight, fsDeviation, randomize, mountPoint, id, diskserver FROM rh_FileSystem WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraFileSystemCnv::s_updateStatementString =
"UPDATE rh_FileSystem SET free = :1, weight = :2, fsDeviation = :3, randomize = :4, mountPoint = :5, diskserver = :6 WHERE id = :7";

/// SQL statement for type storage
const std::string castor::db::ora::OraFileSystemCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraFileSystemCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

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
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::createRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  castor::ObjectSet& alreadyDone,
                                                  bool autocommit,
                                                  bool recursive)
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
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Set ids of all objects
    int nids = obj->id() == 0 ? 1 : 0;
    // check which objects need to be saved/updated and keeps a list of them
    std::list<castor::IObject*> toBeSaved;
    std::list<castor::IObject*> toBeUpdated;
    if (recursive) {
      for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->copies().begin();
           it != obj->copies().end();
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
    if (recursive) {
      if (alreadyDone.find(obj->diskserver()) == alreadyDone.end() &&
          obj->diskserver() != 0) {
        if (0 == obj->diskserver()->id()) {
          toBeSaved.push_back(obj->diskserver());
          nids++;
        } else {
          toBeUpdated.push_back(obj->diskserver());
        }
      }
    }
    u_signed64 id = cnvSvc()->getIds(nids);
    if (0 == obj->id()) obj->setId(id++);
    for (std::list<castor::IObject*>::const_iterator it = toBeSaved.begin();
         it != toBeSaved.end();
         it++) {
      (*it)->setId(id++);
    }
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
    m_insertStatement->setDouble(7, obj->diskserver() ? obj->diskserver()->id() : 0);
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
    // Deal with diskserver
    if (0 != obj->diskserver()) {
      if (0 == m_insertDiskServer2FileSystemStatement) {
        m_insertDiskServer2FileSystemStatement = createStatement(s_insertDiskServer2FileSystemStatementString);
      }
      m_insertDiskServer2FileSystemStatement->setDouble(1, obj->diskserver()->id());
      m_insertDiskServer2FileSystemStatement->setDouble(2, obj->id());
      m_insertDiskServer2FileSystemStatement->executeUpdate();
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
                    << "  free : " << obj->free() << std::endl
                    << "  weight : " << obj->weight() << std::endl
                    << "  fsDeviation : " << obj->fsDeviation() << std::endl
                    << "  randomize : " << obj->randomize() << std::endl
                    << "  mountPoint : " << obj->mountPoint() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  diskserver : " << obj->diskserver() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraFileSystemCnv::updateRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  castor::ObjectSet& alreadyDone,
                                                  bool autocommit,
                                                  bool recursive)
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
      m_selectStatement->setDouble(1, obj->id());
      oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();
      if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
        castor::exception::NoEntry ex;
        ex.getMessage() << "No object found for id :" << obj->id();
        throw ex;
      }
      // Dealing with diskserver
      {
        u_signed64 diskserverId = (unsigned long long)rset->getDouble(7);
        castor::db::DbAddress ad(diskserverId, " ", 0);
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
        if (diskserverId == 0) {
          if (0 != obj->diskserver()) {
            if (alreadyDone.find(obj->diskserver()) == alreadyDone.end()) {
              cnvSvc()->createRep(&ad, obj->diskserver(), alreadyDone, false, true);
              if (0 == m_insertDiskServer2FileSystemStatement) {
                m_insertDiskServer2FileSystemStatement = createStatement(s_insertDiskServer2FileSystemStatementString);
              }
              m_insertDiskServer2FileSystemStatement->setDouble(1, obj->diskserver()->id());
              m_insertDiskServer2FileSystemStatement->setDouble(2, obj->id());
              m_insertDiskServer2FileSystemStatement->executeUpdate();
            }
          }
        } else {
          if (alreadyDone.find(obj->diskserver()) == alreadyDone.end()) {
            cnvSvc()->updateRep(&ad, obj->diskserver(), alreadyDone, false, recursive);
          }
        }
      }
      m_selectStatement->closeResultSet(rset);
    }
    // Now Update the current object
    m_updateStatement->setDouble(1, obj->free());
    m_updateStatement->setFloat(2, obj->weight());
    m_updateStatement->setFloat(3, obj->fsDeviation());
    m_updateStatement->setInt(4, obj->randomize());
    m_updateStatement->setString(5, obj->mountPoint());
    m_updateStatement->setDouble(6, obj->diskserver() ? obj->diskserver()->id() : 0);
    m_updateStatement->setDouble(7, obj->id());
    m_updateStatement->executeUpdate();
    if (recursive) {
      // Dealing with copies
      {
        if (0 == m_FileSystem2DiskCopyStatement) {
          m_FileSystem2DiskCopyStatement = createStatement(s_FileSystem2DiskCopyStatementString);
        }
        std::set<int> copiesList;
        m_FileSystem2DiskCopyStatement->setDouble(1, obj->id());
        oracle::occi::ResultSet *rset = m_FileSystem2DiskCopyStatement->executeQuery();
        while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
          copiesList.insert(rset->getInt(1));
        }
        m_FileSystem2DiskCopyStatement->closeResultSet(rset);
        for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->copies().begin();
             it != obj->copies().end();
             it++) {
          std::set<int>::iterator item;
          if ((item = copiesList.find((*it)->id())) == copiesList.end()) {
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->createRep(0, *it, alreadyDone, false, true);
            }
          } else {
            copiesList.erase(item);
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->updateRep(0, *it, alreadyDone, false, recursive);
            }
          }
        }
        for (std::set<int>::iterator it = copiesList.begin();
             it != copiesList.end();
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
void castor::db::ora::OraFileSystemCnv::deleteRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  castor::ObjectSet& alreadyDone,
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
    // Mark the current object as done
    alreadyDone.insert(obj);
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
castor::IObject* castor::db::ora::OraFileSystemCnv::createObj(castor::IAddress* address,
                                                              castor::ObjectCatalog& newlyCreated,
                                                              bool recursive)
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
    object->setFree((unsigned long long)rset->getDouble(1));
    object->setWeight(rset->getFloat(2));
    object->setFsDeviation(rset->getFloat(3));
    object->setRandomize(rset->getInt(4));
    object->setMountPoint(rset->getString(5));
    object->setId((unsigned long long)rset->getDouble(6));
    newlyCreated[object->id()] = object;
    if (recursive) {
      u_signed64 diskserverId = (unsigned long long)rset->getDouble(8);
      IObject* objDiskserver = cnvSvc()->getObjFromId(diskserverId, newlyCreated);
      object->setDiskserver(dynamic_cast<castor::stager::DiskServer*>(objDiskserver));
    }
    m_selectStatement->closeResultSet(rset);
    if (recursive) {
      // Get ids of objs to retrieve
      if (0 == m_FileSystem2DiskCopyStatement) {
        m_FileSystem2DiskCopyStatement = createStatement(s_FileSystem2DiskCopyStatementString);
      }
      m_FileSystem2DiskCopyStatement->setDouble(1, ad->id());
      rset = m_FileSystem2DiskCopyStatement->executeQuery();
      while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
        IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1), newlyCreated);
        object->addCopies(dynamic_cast<castor::stager::DiskCopy*>(obj));
      }
      m_FileSystem2DiskCopyStatement->closeResultSet(rset);
    }
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
void castor::db::ora::OraFileSystemCnv::updateObj(castor::IObject* obj,
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
    object->setFree((unsigned long long)rset->getDouble(1));
    object->setWeight(rset->getFloat(2));
    object->setFsDeviation(rset->getFloat(3));
    object->setRandomize(rset->getInt(4));
    object->setMountPoint(rset->getString(5));
    object->setId((unsigned long long)rset->getDouble(6));
    alreadyDone[obj->id()] = obj;
    // Dealing with diskserver
    u_signed64 diskserverId = (unsigned long long)rset->getDouble(7);
    if (0 != object->diskserver() &&
        (0 == diskserverId ||
         object->diskserver()->id() != diskserverId)) {
      delete object->diskserver();
      object->setDiskserver(0);
    }
    if (0 != diskserverId) {
      if (0 == object->diskserver()) {
        object->setDiskserver
          (dynamic_cast<castor::stager::DiskServer*>
           (cnvSvc()->getObjFromId(diskserverId, alreadyDone)));
      } else if (object->diskserver()->id() == diskserverId) {
        if (alreadyDone.find(object->diskserver()->id()) == alreadyDone.end()) {
          cnvSvc()->updateObj(object->diskserver(), alreadyDone);
        }
      }
    }
    m_selectStatement->closeResultSet(rset);
    // Deal with copies
    if (0 == m_FileSystem2DiskCopyStatement) {
      m_FileSystem2DiskCopyStatement = createStatement(s_FileSystem2DiskCopyStatementString);
    }
    std::set<int> copiesList;
    m_FileSystem2DiskCopyStatement->setDouble(1, obj->id());
    rset = m_FileSystem2DiskCopyStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      copiesList.insert(rset->getInt(1));
    }
    m_FileSystem2DiskCopyStatement->closeResultSet(rset);
    {
      std::vector<castor::stager::DiskCopy*> toBeDeleted;
      for (std::vector<castor::stager::DiskCopy*>::iterator it = object->copies().begin();
           it != object->copies().end();
           it++) {
        std::set<int>::iterator item;
        if ((item = copiesList.find((*it)->id())) == copiesList.end()) {
          toBeDeleted.push_back(*it);
        } else {
          copiesList.erase(item);
          cnvSvc()->updateObj((*it), alreadyDone);
        }
      }
      for (std::vector<castor::stager::DiskCopy*>::iterator it = toBeDeleted.begin();
           it != toBeDeleted.end();
           it++) {
        object->removeCopies(*it);
        delete (*it);
      }
    }
    for (std::set<int>::iterator it = copiesList.begin();
         it != copiesList.end();
         it++) {
      IObject* item = cnvSvc()->getObjFromId(*it, alreadyDone);
      object->addCopies(dynamic_cast<castor::stager::DiskCopy*>(item));
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

