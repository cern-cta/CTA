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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "OraCastorFileCnv.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/ICnvFactory.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/IObject.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/FileClass.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapeCopy.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraCastorFileCnv> s_factoryOraCastorFileCnv;
const castor::ICnvFactory& OraCastorFileCnvFactory = 
  s_factoryOraCastorFileCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraCastorFileCnv::s_insertStatementString =
"INSERT INTO CastorFile (fileId, nsHost, fileSize, id, svcClass, fileClass) VALUES (:1,:2,:3,ids_seq.nextval,:4,:5) RETURNING id INTO :6";

/// SQL statement for request deletion
const std::string castor::db::ora::OraCastorFileCnv::s_deleteStatementString =
"DELETE FROM CastorFile WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraCastorFileCnv::s_selectStatementString =
"SELECT fileId, nsHost, fileSize, id, svcClass, fileClass FROM CastorFile WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraCastorFileCnv::s_updateStatementString =
"UPDATE CastorFile SET fileId = :1, nsHost = :2, fileSize = :3 WHERE id = :4";

/// SQL statement for type storage
const std::string castor::db::ora::OraCastorFileCnv::s_storeTypeStatementString =
"INSERT INTO Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraCastorFileCnv::s_deleteTypeStatementString =
"DELETE FROM Id2Type WHERE id = :1";

/// SQL existence statement for member svcClass
const std::string castor::db::ora::OraCastorFileCnv::s_checkSvcClassExistStatementString =
"SELECT id from SvcClass WHERE id = :1";

/// SQL update statement for member svcClass
const std::string castor::db::ora::OraCastorFileCnv::s_updateSvcClassStatementString =
"UPDATE CastorFile SET svcClass = : 1 WHERE id = :2";

/// SQL existence statement for member fileClass
const std::string castor::db::ora::OraCastorFileCnv::s_checkFileClassExistStatementString =
"SELECT id from FileClass WHERE id = :1";

/// SQL update statement for member fileClass
const std::string castor::db::ora::OraCastorFileCnv::s_updateFileClassStatementString =
"UPDATE CastorFile SET fileClass = : 1 WHERE id = :2";

/// SQL select statement for member diskCopies
const std::string castor::db::ora::OraCastorFileCnv::s_selectDiskCopyStatementString =
"SELECT id from DiskCopy WHERE castorFile = :1";

/// SQL delete statement for member diskCopies
const std::string castor::db::ora::OraCastorFileCnv::s_deleteDiskCopyStatementString =
"UPDATE DiskCopy SET castorFile = 0 WHERE id = :1";

/// SQL remote update statement for member diskCopies
const std::string castor::db::ora::OraCastorFileCnv::s_remoteUpdateDiskCopyStatementString =
"UPDATE DiskCopy SET castorFile = :1 WHERE id = :2";

/// SQL select statement for member tapeCopies
const std::string castor::db::ora::OraCastorFileCnv::s_selectTapeCopyStatementString =
"SELECT id from TapeCopy WHERE castorFile = :1";

/// SQL delete statement for member tapeCopies
const std::string castor::db::ora::OraCastorFileCnv::s_deleteTapeCopyStatementString =
"UPDATE TapeCopy SET castorFile = 0 WHERE id = :1";

/// SQL remote update statement for member tapeCopies
const std::string castor::db::ora::OraCastorFileCnv::s_remoteUpdateTapeCopyStatementString =
"UPDATE TapeCopy SET castorFile = :1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraCastorFileCnv::OraCastorFileCnv(castor::ICnvSvc* cnvSvc) :
  OraBaseCnv(cnvSvc),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_checkSvcClassExistStatement(0),
  m_updateSvcClassStatement(0),
  m_checkFileClassExistStatement(0),
  m_updateFileClassStatement(0),
  m_selectDiskCopyStatement(0),
  m_deleteDiskCopyStatement(0),
  m_remoteUpdateDiskCopyStatement(0),
  m_selectTapeCopyStatement(0),
  m_deleteTapeCopyStatement(0),
  m_remoteUpdateTapeCopyStatement(0) {}

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
    deleteStatement(m_checkSvcClassExistStatement);
    deleteStatement(m_updateSvcClassStatement);
    deleteStatement(m_checkFileClassExistStatement);
    deleteStatement(m_updateFileClassStatement);
    deleteStatement(m_deleteDiskCopyStatement);
    deleteStatement(m_selectDiskCopyStatement);
    deleteStatement(m_remoteUpdateDiskCopyStatement);
    deleteStatement(m_deleteTapeCopyStatement);
    deleteStatement(m_selectTapeCopyStatement);
    deleteStatement(m_remoteUpdateTapeCopyStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_checkSvcClassExistStatement = 0;
  m_updateSvcClassStatement = 0;
  m_checkFileClassExistStatement = 0;
  m_updateFileClassStatement = 0;
  m_selectDiskCopyStatement = 0;
  m_deleteDiskCopyStatement = 0;
  m_remoteUpdateDiskCopyStatement = 0;
  m_selectTapeCopyStatement = 0;
  m_deleteTapeCopyStatement = 0;
  m_remoteUpdateTapeCopyStatement = 0;
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
  try {
    switch (type) {
    case castor::OBJ_SvcClass :
      fillRepSvcClass(obj);
      break;
    case castor::OBJ_FileClass :
      fillRepFileClass(obj);
      break;
    case castor::OBJ_DiskCopy :
      fillRepDiskCopy(obj);
      break;
    case castor::OBJ_TapeCopy :
      fillRepTapeCopy(obj);
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
// fillRepSvcClass
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillRepSvcClass(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  if (0 != obj->svcClass()) {
    // Check checkSvcClassExist statement
    if (0 == m_checkSvcClassExistStatement) {
      m_checkSvcClassExistStatement = createStatement(s_checkSvcClassExistStatementString);
    }
    // retrieve the object from the database
    m_checkSvcClassExistStatement->setDouble(1, obj->svcClass()->id());
    oracle::occi::ResultSet *rset = m_checkSvcClassExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad;
      ad.setCnvSvcName("OraCnvSvc");
      ad.setCnvSvcType(castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->svcClass(), false);
    }
    // Close resultset
    m_checkSvcClassExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateSvcClassStatement) {
    m_updateSvcClassStatement = createStatement(s_updateSvcClassStatementString);
  }
  // Update local object
  m_updateSvcClassStatement->setDouble(1, 0 == obj->svcClass() ? 0 : obj->svcClass()->id());
  m_updateSvcClassStatement->setDouble(2, obj->id());
  m_updateSvcClassStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillRepFileClass
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillRepFileClass(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  if (0 != obj->fileClass()) {
    // Check checkFileClassExist statement
    if (0 == m_checkFileClassExistStatement) {
      m_checkFileClassExistStatement = createStatement(s_checkFileClassExistStatementString);
    }
    // retrieve the object from the database
    m_checkFileClassExistStatement->setDouble(1, obj->fileClass()->id());
    oracle::occi::ResultSet *rset = m_checkFileClassExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad;
      ad.setCnvSvcName("OraCnvSvc");
      ad.setCnvSvcType(castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->fileClass(), false);
    }
    // Close resultset
    m_checkFileClassExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateFileClassStatement) {
    m_updateFileClassStatement = createStatement(s_updateFileClassStatementString);
  }
  // Update local object
  m_updateFileClassStatement->setDouble(1, 0 == obj->fileClass() ? 0 : obj->fileClass()->id());
  m_updateFileClassStatement->setDouble(2, obj->id());
  m_updateFileClassStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillRepDiskCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillRepDiskCopy(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // check select statement
  if (0 == m_selectDiskCopyStatement) {
    m_selectDiskCopyStatement = createStatement(s_selectDiskCopyStatementString);
  }
  // Get current database data
  std::set<int> diskCopiesList;
  m_selectDiskCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectDiskCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    diskCopiesList.insert(rset->getInt(1));
  }
  m_selectDiskCopyStatement->closeResultSet(rset);
  // update diskCopies and create new ones
  for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->diskCopies().begin();
       it != obj->diskCopies().end();
       it++) {
    if (0 == (*it)->id()) {
      cnvSvc()->createRep(0, *it, false, OBJ_CastorFile);
    } else {
      // Check remote update statement
      if (0 == m_remoteUpdateDiskCopyStatement) {
        m_remoteUpdateDiskCopyStatement = createStatement(s_remoteUpdateDiskCopyStatementString);
      }
      // Update remote object
      m_remoteUpdateDiskCopyStatement->setDouble(1, obj->id());
      m_remoteUpdateDiskCopyStatement->setDouble(2, (*it)->id());
      m_remoteUpdateDiskCopyStatement->executeUpdate();
      std::set<int>::iterator item;
      if ((item = diskCopiesList.find((*it)->id())) != diskCopiesList.end()) {
        diskCopiesList.erase(item);
      }
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = diskCopiesList.begin();
       it != diskCopiesList.end();
       it++) {
    if (0 == m_deleteDiskCopyStatement) {
      m_deleteDiskCopyStatement = createStatement(s_deleteDiskCopyStatementString);
    }
    m_deleteDiskCopyStatement->setDouble(1, *it);
    m_deleteDiskCopyStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepTapeCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillRepTapeCopy(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // check select statement
  if (0 == m_selectTapeCopyStatement) {
    m_selectTapeCopyStatement = createStatement(s_selectTapeCopyStatementString);
  }
  // Get current database data
  std::set<int> tapeCopiesList;
  m_selectTapeCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectTapeCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    tapeCopiesList.insert(rset->getInt(1));
  }
  m_selectTapeCopyStatement->closeResultSet(rset);
  // update tapeCopies and create new ones
  for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->tapeCopies().begin();
       it != obj->tapeCopies().end();
       it++) {
    if (0 == (*it)->id()) {
      cnvSvc()->createRep(0, *it, false, OBJ_CastorFile);
    } else {
      // Check remote update statement
      if (0 == m_remoteUpdateTapeCopyStatement) {
        m_remoteUpdateTapeCopyStatement = createStatement(s_remoteUpdateTapeCopyStatementString);
      }
      // Update remote object
      m_remoteUpdateTapeCopyStatement->setDouble(1, obj->id());
      m_remoteUpdateTapeCopyStatement->setDouble(2, (*it)->id());
      m_remoteUpdateTapeCopyStatement->executeUpdate();
      std::set<int>::iterator item;
      if ((item = tapeCopiesList.find((*it)->id())) != tapeCopiesList.end()) {
        tapeCopiesList.erase(item);
      }
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = tapeCopiesList.begin();
       it != tapeCopiesList.end();
       it++) {
    if (0 == m_deleteTapeCopyStatement) {
      m_deleteTapeCopyStatement = createStatement(s_deleteTapeCopyStatementString);
    }
    m_deleteTapeCopyStatement->setDouble(1, *it);
    m_deleteTapeCopyStatement->executeUpdate();
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
  case castor::OBJ_SvcClass :
    fillObjSvcClass(obj);
    break;
  case castor::OBJ_FileClass :
    fillObjFileClass(obj);
    break;
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
// fillObjSvcClass
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillObjSvcClass(castor::stager::CastorFile* obj)
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
  u_signed64 svcClassId = (u_signed64)rset->getDouble(5);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->svcClass() &&
      (0 == svcClassId ||
       obj->svcClass()->id() != svcClassId)) {
    obj->setSvcClass(0);
  }
  // Update object or create new one
  if (0 != svcClassId) {
    if (0 == obj->svcClass()) {
      obj->setSvcClass
        (dynamic_cast<castor::stager::SvcClass*>
         (cnvSvc()->getObjFromId(svcClassId)));
    } else {
      cnvSvc()->updateObj(obj->svcClass());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjFileClass
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillObjFileClass(castor::stager::CastorFile* obj)
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
  u_signed64 fileClassId = (u_signed64)rset->getDouble(6);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->fileClass() &&
      (0 == fileClassId ||
       obj->fileClass()->id() != fileClassId)) {
    obj->setFileClass(0);
  }
  // Update object or create new one
  if (0 != fileClassId) {
    if (0 == obj->fileClass()) {
      obj->setFileClass
        (dynamic_cast<castor::stager::FileClass*>
         (cnvSvc()->getObjFromId(fileClassId)));
    } else {
      cnvSvc()->updateObj(obj->fileClass());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjDiskCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillObjDiskCopy(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectDiskCopyStatement) {
    m_selectDiskCopyStatement = createStatement(s_selectDiskCopyStatementString);
  }
  // retrieve the object from the database
  std::set<int> diskCopiesList;
  m_selectDiskCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectDiskCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    diskCopiesList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectDiskCopyStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::DiskCopy*> toBeDeleted;
  for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->diskCopies().begin();
       it != obj->diskCopies().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = diskCopiesList.find((*it)->id())) == diskCopiesList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      diskCopiesList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::DiskCopy*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeDiskCopies(*it);
    (*it)->setCastorFile(0);
  }
  // Create new objects
  for (std::set<int>::iterator it = diskCopiesList.begin();
       it != diskCopiesList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    castor::stager::DiskCopy* remoteObj = 
      dynamic_cast<castor::stager::DiskCopy*>(item);
    obj->addDiskCopies(remoteObj);
    remoteObj->setCastorFile(obj);
  }
}

//------------------------------------------------------------------------------
// fillObjTapeCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::fillObjTapeCopy(castor::stager::CastorFile* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectTapeCopyStatement) {
    m_selectTapeCopyStatement = createStatement(s_selectTapeCopyStatementString);
  }
  // retrieve the object from the database
  std::set<int> tapeCopiesList;
  m_selectTapeCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectTapeCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    tapeCopiesList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectTapeCopyStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::TapeCopy*> toBeDeleted;
  for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->tapeCopies().begin();
       it != obj->tapeCopies().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = tapeCopiesList.find((*it)->id())) == tapeCopiesList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      tapeCopiesList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::TapeCopy*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeTapeCopies(*it);
    (*it)->setCastorFile(0);
  }
  // Create new objects
  for (std::set<int>::iterator it = tapeCopiesList.begin();
       it != tapeCopiesList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    castor::stager::TapeCopy* remoteObj = 
      dynamic_cast<castor::stager::TapeCopy*>(item);
    obj->addTapeCopies(remoteObj);
    remoteObj->setCastorFile(obj);
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraCastorFileCnv::createRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit,
                                                  unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::CastorFile* obj = 
    dynamic_cast<castor::stager::CastorFile*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  if (0 != obj->id()) return;
  try {
    // Check whether the statements are ok
    if (0 == m_insertStatement) {
      m_insertStatement = createStatement(s_insertStatementString);
      m_insertStatement->registerOutParam(6, oracle::occi::OCCIINT);
    }
    if (0 == m_storeTypeStatement) {
      m_storeTypeStatement = createStatement(s_storeTypeStatementString);
    }
    // Now Save the current object
    m_insertStatement->setDouble(1, obj->fileId());
    m_insertStatement->setString(2, obj->nsHost());
    m_insertStatement->setDouble(3, obj->fileSize());
    m_insertStatement->setDouble(4, (type == OBJ_SvcClass && obj->svcClass() != 0) ? obj->svcClass()->id() : 0);
    m_insertStatement->setDouble(5, (type == OBJ_FileClass && obj->fileClass() != 0) ? obj->fileClass()->id() : 0);
    m_insertStatement->executeUpdate();
    obj->setId((u_signed64)m_insertStatement->getDouble(6));
    m_storeTypeStatement->setDouble(1, obj->id());
    m_storeTypeStatement->setInt(2, obj->type());
    m_storeTypeStatement->executeUpdate();
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
                    << "  fileSize : " << obj->fileSize() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  svcClass : " << obj->svcClass() << std::endl
                    << "  fileClass : " << obj->fileClass() << std::endl;
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
    m_updateStatement->setDouble(3, obj->fileSize());
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
    for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->tapeCopies().begin();
         it != obj->tapeCopies().end();
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
  castor::BaseAddress* ad = 
    dynamic_cast<castor::BaseAddress*>(address);
  try {
    // Check whether the statement is ok
    if (0 == m_selectStatement) {
      m_selectStatement = createStatement(s_selectStatementString);
    }
    // retrieve the object from the database
    m_selectStatement->setDouble(1, ad->target());
    oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::exception::NoEntry ex;
      ex.getMessage() << "No object found for id :" << ad->target();
      throw ex;
    }
    // create the new Object
    castor::stager::CastorFile* object = new castor::stager::CastorFile();
    // Now retrieve and set members
    object->setFileId((u_signed64)rset->getDouble(1));
    object->setNsHost(rset->getString(2));
    object->setFileSize((u_signed64)rset->getDouble(3));
    object->setId((u_signed64)rset->getDouble(4));
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
                    << "and id was " << ad->target() << std::endl;;
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
    object->setFileId((u_signed64)rset->getDouble(1));
    object->setNsHost(rset->getString(2));
    object->setFileSize((u_signed64)rset->getDouble(3));
    object->setId((u_signed64)rset->getDouble(4));
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

