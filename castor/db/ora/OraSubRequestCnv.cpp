/******************************************************************************
 *                      castor/db/ora/OraSubRequestCnv.cpp
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
#include "OraSubRequestCnv.hpp"
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
#include "castor/stager/Request.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraSubRequestCnv> s_factoryOraSubRequestCnv;
const castor::IFactory<castor::IConverter>& OraSubRequestCnvFactory = 
  s_factoryOraSubRequestCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraSubRequestCnv::s_insertStatementString =
"INSERT INTO rh_SubRequest (retryCounter, fileName, protocol, poolName, xsize, id, diskcopy, castorFile, request, status) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraSubRequestCnv::s_deleteStatementString =
"DELETE FROM rh_SubRequest WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraSubRequestCnv::s_selectStatementString =
"SELECT retryCounter, fileName, protocol, poolName, xsize, id, diskcopy, castorFile, request, status FROM rh_SubRequest WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraSubRequestCnv::s_updateStatementString =
"UPDATE rh_SubRequest SET retryCounter = :1, fileName = :2, protocol = :3, poolName = :4, xsize = :5, diskcopy = :6, castorFile = :7, request = :8, status = :9 WHERE id = :10";

/// SQL statement for type storage
const std::string castor::db::ora::OraSubRequestCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraSubRequestCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL insert statement for member parent
const std::string castor::db::ora::OraSubRequestCnv::s_insertSubRequest2SubRequestStatementString =
"INSERT INTO rh_SubRequest2SubRequest (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member parent
const std::string castor::db::ora::OraSubRequestCnv::s_deleteSubRequest2SubRequestStatementString =
"DELETE FROM rh_SubRequest2SubRequest WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member parent
const std::string castor::db::ora::OraSubRequestCnv::s_SubRequest2SubRequestStatementString =
"SELECT Child from rh_SubRequest2SubRequest WHERE Parent = :1";

/// SQL insert statement for member request
const std::string castor::db::ora::OraSubRequestCnv::s_insertRequest2SubRequestStatementString =
"INSERT INTO rh_Request2SubRequest (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member request
const std::string castor::db::ora::OraSubRequestCnv::s_deleteRequest2SubRequestStatementString =
"DELETE FROM rh_Request2SubRequest WHERE Parent = :1 AND Child = :2";

/// SQL insert statement for member status
const std::string castor::db::ora::OraSubRequestCnv::s_insertSubRequest2SubRequestStatusCodesStatementString =
"INSERT INTO rh_SubRequest2SubRequestStatusCodes (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member status
const std::string castor::db::ora::OraSubRequestCnv::s_deleteSubRequest2SubRequestStatusCodesStatementString =
"DELETE FROM rh_SubRequest2SubRequestStatusCodes WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member status
const std::string castor::db::ora::OraSubRequestCnv::s_SubRequest2SubRequestStatusCodesStatementString =
"SELECT Child from rh_SubRequest2SubRequestStatusCodes WHERE Parent = :1";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraSubRequestCnv::OraSubRequestCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_insertSubRequest2SubRequestStatement(0),
  m_deleteSubRequest2SubRequestStatement(0),
  m_SubRequest2SubRequestStatement(0),
  m_insertRequest2SubRequestStatement(0),
  m_deleteRequest2SubRequestStatement(0),
  m_insertSubRequest2SubRequestStatusCodesStatement(0),
  m_deleteSubRequest2SubRequestStatusCodesStatement(0),
  m_SubRequest2SubRequestStatusCodesStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraSubRequestCnv::~OraSubRequestCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_insertSubRequest2SubRequestStatement);
    deleteStatement(m_SubRequest2SubRequestStatement);
    deleteStatement(m_insertRequest2SubRequestStatement);
    deleteStatement(m_deleteRequest2SubRequestStatement);
    deleteStatement(m_insertSubRequest2SubRequestStatusCodesStatement);
    deleteStatement(m_SubRequest2SubRequestStatusCodesStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertSubRequest2SubRequestStatement = 0;
  m_deleteSubRequest2SubRequestStatement = 0;
  m_SubRequest2SubRequestStatement = 0;
  m_insertRequest2SubRequestStatement = 0;
  m_deleteRequest2SubRequestStatement = 0;
  m_insertSubRequest2SubRequestStatusCodesStatement = 0;
  m_deleteSubRequest2SubRequestStatusCodesStatement = 0;
  m_SubRequest2SubRequestStatusCodesStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraSubRequestCnv::ObjType() {
  return castor::stager::SubRequest::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraSubRequestCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                unsigned int type,
                                                bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
  switch (type) {
  case castor::OBJ_DiskCopy :
    fillRepDiskCopy(obj);
    break;
  case castor::OBJ_CastorFile :
    fillRepCastorFile(obj);
    break;
  case castor::OBJ_SubRequest :
    fillRepSubRequest(obj);
    break;
  case castor::OBJ_Request :
    fillRepRequest(obj);
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
void castor::db::ora::OraSubRequestCnv::fillRepDiskCopy(castor::stager::SubRequest* obj)
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
  u_signed64 diskcopyId = (u_signed64)rset->getDouble(7);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(diskcopyId, " ", 0);
  // Check whether old object should be deleted
  if (0 != diskcopyId &&
      0 != obj->diskcopy() &&
      obj->diskcopy()->id() != diskcopyId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    diskcopyId = 0;
  }
  // Update object or create new one
  if (diskcopyId == 0) {
    if (0 != obj->diskcopy()) {
      cnvSvc()->createRep(&ad, obj->diskcopy(), false);
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->diskcopy(), false);
  }
}

//------------------------------------------------------------------------------
// fillRepCastorFile
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillRepCastorFile(castor::stager::SubRequest* obj)
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
  u_signed64 castorFileId = (u_signed64)rset->getDouble(8);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(castorFileId, " ", 0);
  // Check whether old object should be deleted
  if (0 != castorFileId &&
      0 != obj->castorFile() &&
      obj->castorFile()->id() != castorFileId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    castorFileId = 0;
  }
  // Update object or create new one
  if (castorFileId == 0) {
    if (0 != obj->castorFile()) {
      cnvSvc()->createRep(&ad, obj->castorFile(), false);
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->castorFile(), false);
  }
}

//------------------------------------------------------------------------------
// fillRepSubRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillRepSubRequest(castor::stager::SubRequest* obj)
  throw (castor::exception::Exception) {
  // check select statement
  if (0 == m_SubRequest2SubRequestStatement) {
    m_SubRequest2SubRequestStatement = createStatement(s_SubRequest2SubRequestStatementString);
  }
  // Get current database data
  std::set<int> parentList;
  m_SubRequest2SubRequestStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_SubRequest2SubRequestStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    parentList.insert(rset->getInt(1));
  }
  m_SubRequest2SubRequestStatement->closeResultSet(rset);
  // update segments and create new ones
  for (std::vector<castor::stager::SubRequest*>::iterator it = obj->parent().begin();
       it != obj->parent().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = parentList.find((*it)->id())) == parentList.end()) {
      cnvSvc()->createRep(0, *it, false);
    } else {
      parentList.erase(item);
      cnvSvc()->updateRep(0, *it, false);
    }
  }
  // Delete old data
  for (std::set<int>::iterator it = parentList.begin();
       it != parentList.end();
       it++) {
    castor::db::DbAddress ad(*it, " ", 0);
    cnvSvc()->deleteRepByAddress(&ad, false);
  }
}

//------------------------------------------------------------------------------
// fillRepRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillRepRequest(castor::stager::SubRequest* obj)
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
  u_signed64 requestId = (u_signed64)rset->getDouble(9);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(requestId, " ", 0);
  // Check whether old object should be deleted
  if (0 != requestId &&
      0 != obj->request() &&
      obj->request()->id() != requestId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    requestId = 0;
    if (0 == m_deleteRequest2SubRequestStatement) {
      m_deleteRequest2SubRequestStatement = createStatement(s_deleteRequest2SubRequestStatementString);
    }
    m_deleteRequest2SubRequestStatement->setDouble(1, obj->request()->id());
    m_deleteRequest2SubRequestStatement->setDouble(2, obj->id());
    m_deleteRequest2SubRequestStatement->executeUpdate();
  }
  // Update object or create new one
  if (requestId == 0) {
    if (0 != obj->request()) {
      cnvSvc()->createRep(&ad, obj->request(), false);
      if (0 == m_insertRequest2SubRequestStatement) {
        m_insertRequest2SubRequestStatement = createStatement(s_insertRequest2SubRequestStatementString);
      }
      m_insertRequest2SubRequestStatement->setDouble(1, obj->request()->id());
      m_insertRequest2SubRequestStatement->setDouble(2, obj->id());
      m_insertRequest2SubRequestStatement->executeUpdate();
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->request(), false);
  }
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillObj(castor::IAddress* address,
                                                castor::IObject* object,
                                                unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
  switch (type) {
  case castor::OBJ_DiskCopy :
    fillObjDiskCopy(obj);
    break;
  case castor::OBJ_CastorFile :
    fillObjCastorFile(obj);
    break;
  case castor::OBJ_SubRequest :
    fillObjSubRequest(obj);
    break;
  case castor::OBJ_Request :
    fillObjRequest(obj);
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
void castor::db::ora::OraSubRequestCnv::fillObjDiskCopy(castor::stager::SubRequest* obj)
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
  u_signed64 diskcopyId = (u_signed64)rset->getDouble(7);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->diskcopy() &&
      (0 == diskcopyId ||
       obj->diskcopy()->id() != diskcopyId)) {
    delete obj->diskcopy();
    obj->setDiskcopy(0);
  }
  // Update object or create new one
  if (0 != diskcopyId) {
    if (0 == obj->diskcopy()) {
      obj->setDiskcopy
        (dynamic_cast<castor::stager::DiskCopy*>
         (cnvSvc()->getObjFromId(diskcopyId)));
    } else if (obj->diskcopy()->id() == diskcopyId) {
      cnvSvc()->updateObj(obj->diskcopy());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjCastorFile
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillObjCastorFile(castor::stager::SubRequest* obj)
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
  u_signed64 castorFileId = (u_signed64)rset->getDouble(8);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->castorFile() &&
      (0 == castorFileId ||
       obj->castorFile()->id() != castorFileId)) {
    delete obj->castorFile();
    obj->setCastorFile(0);
  }
  // Update object or create new one
  if (0 != castorFileId) {
    if (0 == obj->castorFile()) {
      obj->setCastorFile
        (dynamic_cast<castor::stager::CastorFile*>
         (cnvSvc()->getObjFromId(castorFileId)));
    } else if (obj->castorFile()->id() == castorFileId) {
      cnvSvc()->updateObj(obj->castorFile());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjSubRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillObjSubRequest(castor::stager::SubRequest* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_SubRequest2SubRequestStatement) {
    m_SubRequest2SubRequestStatement = createStatement(s_SubRequest2SubRequestStatementString);
  }
  // retrieve the object from the database
  std::set<int> parentList;
  m_SubRequest2SubRequestStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_SubRequest2SubRequestStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    parentList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_SubRequest2SubRequestStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::SubRequest*> toBeDeleted;
  for (std::vector<castor::stager::SubRequest*>::iterator it = obj->parent().begin();
       it != obj->parent().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = parentList.find((*it)->id())) == parentList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      parentList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::SubRequest*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeParent(*it);
    delete (*it);
  }
  // Create new objects
  for (std::set<int>::iterator it = parentList.begin();
       it != parentList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    obj->addParent(dynamic_cast<castor::stager::SubRequest*>(item));
  }
}

//------------------------------------------------------------------------------
// fillObjRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillObjRequest(castor::stager::SubRequest* obj)
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
  u_signed64 requestId = (u_signed64)rset->getDouble(9);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->request() &&
      (0 == requestId ||
       obj->request()->id() != requestId)) {
    delete obj->request();
    obj->setRequest(0);
  }
  // Update object or create new one
  if (0 != requestId) {
    if (0 == obj->request()) {
      obj->setRequest
        (dynamic_cast<castor::stager::Request*>
         (cnvSvc()->getObjFromId(requestId)));
    } else if (obj->request()->id() == requestId) {
      cnvSvc()->updateObj(obj->request());
    }
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::createRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
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
    m_insertStatement->setInt(1, obj->retryCounter());
    m_insertStatement->setString(2, obj->fileName());
    m_insertStatement->setString(3, obj->protocol());
    m_insertStatement->setString(4, obj->poolName());
    m_insertStatement->setDouble(5, obj->xsize());
    m_insertStatement->setDouble(6, obj->id());
    m_insertStatement->setDouble(7, obj->diskcopy() ? obj->diskcopy()->id() : 0);
    m_insertStatement->setDouble(8, obj->castorFile() ? obj->castorFile()->id() : 0);
    m_insertStatement->setDouble(9, obj->request() ? obj->request()->id() : 0);
    m_insertStatement->setDouble(10, (int)obj->status());
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
                    << "  retryCounter : " << obj->retryCounter() << std::endl
                    << "  fileName : " << obj->fileName() << std::endl
                    << "  protocol : " << obj->protocol() << std::endl
                    << "  poolName : " << obj->poolName() << std::endl
                    << "  xsize : " << obj->xsize() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  diskcopy : " << obj->diskcopy() << std::endl
                    << "  castorFile : " << obj->castorFile() << std::endl
                    << "  request : " << obj->request() << std::endl
                    << "  status : " << obj->status() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::updateRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setInt(1, obj->retryCounter());
    m_updateStatement->setString(2, obj->fileName());
    m_updateStatement->setString(3, obj->protocol());
    m_updateStatement->setString(4, obj->poolName());
    m_updateStatement->setDouble(5, obj->xsize());
    m_updateStatement->setDouble(6, obj->diskcopy() ? obj->diskcopy()->id() : 0);
    m_updateStatement->setDouble(7, obj->castorFile() ? obj->castorFile()->id() : 0);
    m_updateStatement->setDouble(8, obj->request() ? obj->request()->id() : 0);
    m_updateStatement->setDouble(9, (int)obj->status());
    m_updateStatement->setDouble(10, obj->id());
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
void castor::db::ora::OraSubRequestCnv::deleteRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
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
    // Delete link to request object
    if (0 != obj->request()) {
      // Check whether the statement is ok
      if (0 == m_deleteRequest2SubRequestStatement) {
        m_deleteRequest2SubRequestStatement = createStatement(s_deleteRequest2SubRequestStatementString);
      }
      // Delete links to objects
      m_deleteRequest2SubRequestStatement->setDouble(1, obj->request()->id());
      m_deleteRequest2SubRequestStatement->setDouble(2, obj->id());
      m_deleteRequest2SubRequestStatement->executeUpdate();
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
castor::IObject* castor::db::ora::OraSubRequestCnv::createObj(castor::IAddress* address)
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
    castor::stager::SubRequest* object = new castor::stager::SubRequest();
    // Now retrieve and set members
    object->setRetryCounter(rset->getInt(1));
    object->setFileName(rset->getString(2));
    object->setProtocol(rset->getString(3));
    object->setPoolName(rset->getString(4));
    object->setXsize((u_signed64)rset->getDouble(5));
    object->setId((u_signed64)rset->getDouble(6));
    u_signed64 diskcopyId = (u_signed64)rset->getDouble(7);
    u_signed64 castorFileId = (u_signed64)rset->getDouble(8);
    u_signed64 requestId = (u_signed64)rset->getDouble(9);
    object->setStatus((enum castor::stager::SubRequestStatusCodes)rset->getInt(10));
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
void castor::db::ora::OraSubRequestCnv::updateObj(castor::IObject* obj)
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
    castor::stager::SubRequest* object = 
      dynamic_cast<castor::stager::SubRequest*>(obj);
    object->setRetryCounter(rset->getInt(1));
    object->setFileName(rset->getString(2));
    object->setProtocol(rset->getString(3));
    object->setPoolName(rset->getString(4));
    object->setXsize((u_signed64)rset->getDouble(5));
    object->setId((u_signed64)rset->getDouble(6));
    object->setStatus((enum castor::stager::SubRequestStatusCodes)rset->getInt(10));
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

