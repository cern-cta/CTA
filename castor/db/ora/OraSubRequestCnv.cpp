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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "OraSubRequestCnv.hpp"
#include "castor/BaseAddress.hpp"
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
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraSubRequestCnv> s_factoryOraSubRequestCnv;
const castor::ICnvFactory& OraSubRequestCnvFactory = 
  s_factoryOraSubRequestCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraSubRequestCnv::s_insertStatementString =
"INSERT INTO SubRequest (retryCounter, fileName, protocol, poolName, xsize, priority, id, diskcopy, castorFile, parent, status, request) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraSubRequestCnv::s_deleteStatementString =
"DELETE FROM SubRequest WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraSubRequestCnv::s_selectStatementString =
"SELECT retryCounter, fileName, protocol, poolName, xsize, priority, id, diskcopy, castorFile, parent, status, request FROM SubRequest WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraSubRequestCnv::s_updateStatementString =
"UPDATE SubRequest SET retryCounter = :1, fileName = :2, protocol = :3, poolName = :4, xsize = :5, priority = :6, status = :7 WHERE id = :8";

/// SQL statement for type storage
const std::string castor::db::ora::OraSubRequestCnv::s_storeTypeStatementString =
"INSERT INTO Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraSubRequestCnv::s_deleteTypeStatementString =
"DELETE FROM Id2Type WHERE id = :1";

/// SQL existence statement for member diskcopy
const std::string castor::db::ora::OraSubRequestCnv::s_checkDiskCopyExistStatementString =
"SELECT id from DiskCopy WHERE id = :1";

/// SQL update statement for member diskcopy
const std::string castor::db::ora::OraSubRequestCnv::s_updateDiskCopyStatementString =
"UPDATE SubRequest SET diskcopy = : 1 WHERE id = :2";

/// SQL existence statement for member castorFile
const std::string castor::db::ora::OraSubRequestCnv::s_checkCastorFileExistStatementString =
"SELECT id from CastorFile WHERE id = :1";

/// SQL update statement for member castorFile
const std::string castor::db::ora::OraSubRequestCnv::s_updateCastorFileStatementString =
"UPDATE SubRequest SET castorFile = : 1 WHERE id = :2";

/// SQL existence statement for member parent
const std::string castor::db::ora::OraSubRequestCnv::s_checkSubRequestExistStatementString =
"SELECT id from SubRequest WHERE id = :1";

/// SQL update statement for member parent
const std::string castor::db::ora::OraSubRequestCnv::s_updateSubRequestStatementString =
"UPDATE SubRequest SET parent = : 1 WHERE id = :2";

/// SQL update statement for member request
const std::string castor::db::ora::OraSubRequestCnv::s_updateFileRequestStatementString =
"UPDATE SubRequest SET request = : 1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraSubRequestCnv::OraSubRequestCnv(castor::ICnvSvc* cnvSvc) :
  OraBaseCnv(cnvSvc),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_checkDiskCopyExistStatement(0),
  m_updateDiskCopyStatement(0),
  m_checkCastorFileExistStatement(0),
  m_updateCastorFileStatement(0),
  m_checkSubRequestExistStatement(0),
  m_updateSubRequestStatement(0),
  m_updateFileRequestStatement(0) {}

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
    deleteStatement(m_checkDiskCopyExistStatement);
    deleteStatement(m_updateDiskCopyStatement);
    deleteStatement(m_checkCastorFileExistStatement);
    deleteStatement(m_updateCastorFileStatement);
    deleteStatement(m_checkSubRequestExistStatement);
    deleteStatement(m_updateSubRequestStatement);
    deleteStatement(m_updateFileRequestStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_checkDiskCopyExistStatement = 0;
  m_updateDiskCopyStatement = 0;
  m_checkCastorFileExistStatement = 0;
  m_updateCastorFileStatement = 0;
  m_checkSubRequestExistStatement = 0;
  m_updateSubRequestStatement = 0;
  m_updateFileRequestStatement = 0;
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
  try {
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
    case castor::OBJ_FileRequest :
      fillRepFileRequest(obj);
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
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex; // XXX Fix it, depending on ORACLE error
    ex.getMessage() << "Error in fillRep for type " << type
                    << std::endl << e.what() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// fillRepDiskCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillRepDiskCopy(castor::stager::SubRequest* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  if (0 != obj->diskcopy()) {
    // Check checkDiskCopyExist statement
    if (0 == m_checkDiskCopyExistStatement) {
      m_checkDiskCopyExistStatement = createStatement(s_checkDiskCopyExistStatementString);
    }
    // retrieve the object from the database
    m_checkDiskCopyExistStatement->setDouble(1, obj->diskcopy()->id());
    oracle::occi::ResultSet *rset = m_checkDiskCopyExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->diskcopy(), false);
    }
    // Close resultset
    m_checkDiskCopyExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateDiskCopyStatement) {
    m_updateDiskCopyStatement = createStatement(s_updateDiskCopyStatementString);
  }
  // Update local object
  m_updateDiskCopyStatement->setDouble(1, 0 == obj->diskcopy() ? 0 : obj->diskcopy()->id());
  m_updateDiskCopyStatement->setDouble(2, obj->id());
  m_updateDiskCopyStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillRepCastorFile
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillRepCastorFile(castor::stager::SubRequest* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  if (0 != obj->castorFile()) {
    // Check checkCastorFileExist statement
    if (0 == m_checkCastorFileExistStatement) {
      m_checkCastorFileExistStatement = createStatement(s_checkCastorFileExistStatementString);
    }
    // retrieve the object from the database
    m_checkCastorFileExistStatement->setDouble(1, obj->castorFile()->id());
    oracle::occi::ResultSet *rset = m_checkCastorFileExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->castorFile(), false);
    }
    // Close resultset
    m_checkCastorFileExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateCastorFileStatement) {
    m_updateCastorFileStatement = createStatement(s_updateCastorFileStatementString);
  }
  // Update local object
  m_updateCastorFileStatement->setDouble(1, 0 == obj->castorFile() ? 0 : obj->castorFile()->id());
  m_updateCastorFileStatement->setDouble(2, obj->id());
  m_updateCastorFileStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillRepSubRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillRepSubRequest(castor::stager::SubRequest* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  if (0 != obj->parent()) {
    // Check checkSubRequestExist statement
    if (0 == m_checkSubRequestExistStatement) {
      m_checkSubRequestExistStatement = createStatement(s_checkSubRequestExistStatementString);
    }
    // retrieve the object from the database
    m_checkSubRequestExistStatement->setDouble(1, obj->parent()->id());
    oracle::occi::ResultSet *rset = m_checkSubRequestExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->parent(), false);
    }
    // Close resultset
    m_checkSubRequestExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateSubRequestStatement) {
    m_updateSubRequestStatement = createStatement(s_updateSubRequestStatementString);
  }
  // Update local object
  m_updateSubRequestStatement->setDouble(1, 0 == obj->parent() ? 0 : obj->parent()->id());
  m_updateSubRequestStatement->setDouble(2, obj->id());
  m_updateSubRequestStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillRepFileRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillRepFileRequest(castor::stager::SubRequest* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check update statement
  if (0 == m_updateFileRequestStatement) {
    m_updateFileRequestStatement = createStatement(s_updateFileRequestStatementString);
  }
  // Update local object
  m_updateFileRequestStatement->setDouble(1, 0 == obj->request() ? 0 : obj->request()->id());
  m_updateFileRequestStatement->setDouble(2, obj->id());
  m_updateFileRequestStatement->executeUpdate();
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
  case castor::OBJ_FileRequest :
    fillObjFileRequest(obj);
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
  u_signed64 diskcopyId = (u_signed64)rset->getDouble(8);
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
  u_signed64 castorFileId = (u_signed64)rset->getDouble(9);
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
  u_signed64 parentId = (u_signed64)rset->getDouble(10);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->parent() &&
      (0 == parentId ||
       obj->parent()->id() != parentId)) {
    delete obj->parent();
    obj->setParent(0);
  }
  // Update object or create new one
  if (0 != parentId) {
    if (0 == obj->parent()) {
      obj->setParent
        (dynamic_cast<castor::stager::SubRequest*>
         (cnvSvc()->getObjFromId(parentId)));
    } else if (obj->parent()->id() == parentId) {
      cnvSvc()->updateObj(obj->parent());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjFileRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::fillObjFileRequest(castor::stager::SubRequest* obj)
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
  u_signed64 requestId = (u_signed64)rset->getDouble(11);
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
        (dynamic_cast<castor::stager::FileRequest*>
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
                                                  bool autocommit,
                                                  unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
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
    m_insertStatement->setInt(1, obj->retryCounter());
    m_insertStatement->setString(2, obj->fileName());
    m_insertStatement->setString(3, obj->protocol());
    m_insertStatement->setString(4, obj->poolName());
    m_insertStatement->setDouble(5, obj->xsize());
    m_insertStatement->setInt(6, obj->priority());
    m_insertStatement->setDouble(7, obj->id());
    m_insertStatement->setDouble(8, (type == OBJ_DiskCopy && obj->diskcopy() != 0) ? obj->diskcopy()->id() : 0);
    m_insertStatement->setDouble(9, (type == OBJ_CastorFile && obj->castorFile() != 0) ? obj->castorFile()->id() : 0);
    m_insertStatement->setDouble(10, (type == OBJ_SubRequest && obj->parent() != 0) ? obj->parent()->id() : 0);
    m_insertStatement->setInt(11, (int)obj->status());
    m_insertStatement->setDouble(12, (type == OBJ_FileRequest && obj->request() != 0) ? obj->request()->id() : 0);
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
                    << "  priority : " << obj->priority() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  diskcopy : " << obj->diskcopy() << std::endl
                    << "  castorFile : " << obj->castorFile() << std::endl
                    << "  parent : " << obj->parent() << std::endl
                    << "  status : " << obj->status() << std::endl
                    << "  request : " << obj->request() << std::endl;
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
    m_updateStatement->setInt(6, obj->priority());
    m_updateStatement->setInt(7, (int)obj->status());
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
    object->setPriority(rset->getInt(6));
    object->setId((u_signed64)rset->getDouble(7));
    object->setStatus((enum castor::stager::SubRequestStatusCodes)rset->getInt(11));
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
    object->setPriority(rset->getInt(6));
    object->setId((u_signed64)rset->getDouble(7));
    object->setStatus((enum castor::stager::SubRequestStatusCodes)rset->getInt(11));
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

