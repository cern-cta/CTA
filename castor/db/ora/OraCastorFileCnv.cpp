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
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/TapeCopy.hpp"
#include <list>
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

/// SQL select statement for member diskFileCopies
const std::string castor::db::ora::OraCastorFileCnv::s_CastorFile2DiskCopyStatementString =
"SELECT Child from rh_CastorFile2DiskCopy WHERE Parent = :1";

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
  m_CastorFile2DiskCopyStatement(0),
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
    deleteStatement(m_CastorFile2DiskCopyStatement);
    deleteStatement(m_CastorFile2TapeCopyStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_CastorFile2DiskCopyStatement = 0;
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
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::createRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  castor::ObjectSet& alreadyDone,
                                                  bool autocommit,
                                                  bool recursive)
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
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Set ids of all objects
    int nids = obj->id() == 0 ? 1 : 0;
    // check which objects need to be saved/updated and keeps a list of them
    std::list<castor::IObject*> toBeSaved;
    std::list<castor::IObject*> toBeUpdated;
    if (recursive) {
      for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->diskFileCopies().begin();
           it != obj->diskFileCopies().end();
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
      for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->copies().begin();
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
    m_insertStatement->setDouble(1, obj->fileId());
    m_insertStatement->setString(2, obj->nsHost());
    m_insertStatement->setDouble(3, obj->size());
    m_insertStatement->setDouble(4, obj->id());
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
                                                  castor::ObjectSet& alreadyDone,
                                                  bool autocommit,
                                                  bool recursive)
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
    if (0 == m_updateStatement) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to create statement :" << std::endl
                      << s_updateStatementString;
      throw ex;
    }
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Now Update the current object
    m_updateStatement->setDouble(1, obj->fileId());
    m_updateStatement->setString(2, obj->nsHost());
    m_updateStatement->setDouble(3, obj->size());
    m_updateStatement->setDouble(4, obj->id());
    m_updateStatement->executeUpdate();
    if (recursive) {
      // Dealing with diskFileCopies
      {
        if (0 == m_CastorFile2DiskCopyStatement) {
          m_CastorFile2DiskCopyStatement = createStatement(s_CastorFile2DiskCopyStatementString);
        }
        std::set<int> diskFileCopiesList;
        m_CastorFile2DiskCopyStatement->setDouble(1, obj->id());
        oracle::occi::ResultSet *rset = m_CastorFile2DiskCopyStatement->executeQuery();
        while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
          diskFileCopiesList.insert(rset->getInt(1));
        }
        m_CastorFile2DiskCopyStatement->closeResultSet(rset);
        for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->diskFileCopies().begin();
             it != obj->diskFileCopies().end();
             it++) {
          std::set<int>::iterator item;
          if ((item = diskFileCopiesList.find((*it)->id())) == diskFileCopiesList.end()) {
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->createRep(0, *it, alreadyDone, false, true);
            }
          } else {
            diskFileCopiesList.erase(item);
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->updateRep(0, *it, alreadyDone, false, recursive);
            }
          }
        }
        for (std::set<int>::iterator it = diskFileCopiesList.begin();
             it != diskFileCopiesList.end();
             it++) {
          castor::db::DbAddress ad(*it, " ", 0);
          cnvSvc()->deleteRepByAddress(&ad, false);
        }
      }
      // Dealing with copies
      {
        if (0 == m_CastorFile2TapeCopyStatement) {
          m_CastorFile2TapeCopyStatement = createStatement(s_CastorFile2TapeCopyStatementString);
        }
        std::set<int> copiesList;
        m_CastorFile2TapeCopyStatement->setDouble(1, obj->id());
        oracle::occi::ResultSet *rset = m_CastorFile2TapeCopyStatement->executeQuery();
        while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
          copiesList.insert(rset->getInt(1));
        }
        m_CastorFile2TapeCopyStatement->closeResultSet(rset);
        for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->copies().begin();
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
void castor::db::ora::OraCastorFileCnv::deleteRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  castor::ObjectSet& alreadyDone,
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
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Now Delete the object
    m_deleteTypeStatement->setDouble(1, obj->id());
    m_deleteTypeStatement->executeUpdate();
    m_deleteStatement->setDouble(1, obj->id());
    m_deleteStatement->executeUpdate();
    for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->copies().begin();
         it != obj->copies().end();
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
castor::IObject* castor::db::ora::OraCastorFileCnv::createObj(castor::IAddress* address,
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
    castor::stager::CastorFile* object = new castor::stager::CastorFile();
    // Now retrieve and set members
    object->setFileId((unsigned long long)rset->getDouble(1));
    object->setNsHost(rset->getString(2));
    object->setSize((unsigned long long)rset->getDouble(3));
    object->setId((unsigned long long)rset->getDouble(4));
    newlyCreated[object->id()] = object;
    m_selectStatement->closeResultSet(rset);
    if (recursive) {
      // Get ids of objs to retrieve
      if (0 == m_CastorFile2DiskCopyStatement) {
        m_CastorFile2DiskCopyStatement = createStatement(s_CastorFile2DiskCopyStatementString);
      }
      m_CastorFile2DiskCopyStatement->setDouble(1, ad->id());
      rset = m_CastorFile2DiskCopyStatement->executeQuery();
      while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
        IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1), newlyCreated);
        object->addDiskFileCopies(dynamic_cast<castor::stager::DiskCopy*>(obj));
      }
      m_CastorFile2DiskCopyStatement->closeResultSet(rset);
      if (0 == m_CastorFile2TapeCopyStatement) {
        m_CastorFile2TapeCopyStatement = createStatement(s_CastorFile2TapeCopyStatementString);
      }
      m_CastorFile2TapeCopyStatement->setDouble(1, ad->id());
      rset = m_CastorFile2TapeCopyStatement->executeQuery();
      while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
        IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1), newlyCreated);
        object->addCopies(dynamic_cast<castor::stager::TapeCopy*>(obj));
      }
      m_CastorFile2TapeCopyStatement->closeResultSet(rset);
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
void castor::db::ora::OraCastorFileCnv::updateObj(castor::IObject* obj,
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
    castor::stager::CastorFile* object = 
      dynamic_cast<castor::stager::CastorFile*>(obj);
    object->setFileId((unsigned long long)rset->getDouble(1));
    object->setNsHost(rset->getString(2));
    object->setSize((unsigned long long)rset->getDouble(3));
    object->setId((unsigned long long)rset->getDouble(4));
    alreadyDone[obj->id()] = obj;
    m_selectStatement->closeResultSet(rset);
    // Deal with diskFileCopies
    if (0 == m_CastorFile2DiskCopyStatement) {
      m_CastorFile2DiskCopyStatement = createStatement(s_CastorFile2DiskCopyStatementString);
    }
    std::set<int> diskFileCopiesList;
    m_CastorFile2DiskCopyStatement->setDouble(1, obj->id());
    rset = m_CastorFile2DiskCopyStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      diskFileCopiesList.insert(rset->getInt(1));
    }
    m_CastorFile2DiskCopyStatement->closeResultSet(rset);
    {
      std::vector<castor::stager::DiskCopy*> toBeDeleted;
      for (std::vector<castor::stager::DiskCopy*>::iterator it = object->diskFileCopies().begin();
           it != object->diskFileCopies().end();
           it++) {
        std::set<int>::iterator item;
        if ((item = diskFileCopiesList.find((*it)->id())) == diskFileCopiesList.end()) {
          toBeDeleted.push_back(*it);
        } else {
          diskFileCopiesList.erase(item);
          cnvSvc()->updateObj((*it), alreadyDone);
        }
      }
      for (std::vector<castor::stager::DiskCopy*>::iterator it = toBeDeleted.begin();
           it != toBeDeleted.end();
           it++) {
        object->removeDiskFileCopies(*it);
        delete (*it);
      }
    }
    for (std::set<int>::iterator it = diskFileCopiesList.begin();
         it != diskFileCopiesList.end();
         it++) {
      IObject* item = cnvSvc()->getObjFromId(*it, alreadyDone);
      object->addDiskFileCopies(dynamic_cast<castor::stager::DiskCopy*>(item));
    }
    // Deal with copies
    if (0 == m_CastorFile2TapeCopyStatement) {
      m_CastorFile2TapeCopyStatement = createStatement(s_CastorFile2TapeCopyStatementString);
    }
    std::set<int> copiesList;
    m_CastorFile2TapeCopyStatement->setDouble(1, obj->id());
    rset = m_CastorFile2TapeCopyStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      copiesList.insert(rset->getInt(1));
    }
    m_CastorFile2TapeCopyStatement->closeResultSet(rset);
    {
      std::vector<castor::stager::TapeCopy*> toBeDeleted;
      for (std::vector<castor::stager::TapeCopy*>::iterator it = object->copies().begin();
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
      for (std::vector<castor::stager::TapeCopy*>::iterator it = toBeDeleted.begin();
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
      object->addCopies(dynamic_cast<castor::stager::TapeCopy*>(item));
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

