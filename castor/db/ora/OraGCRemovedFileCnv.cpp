/******************************************************************************
 *                      castor/db/ora/OraGCRemovedFileCnv.cpp
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
 * @(#)$RCSfile: OraGCRemovedFileCnv.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/04/08 08:50:47 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "OraGCRemovedFileCnv.hpp"
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
#include "castor/stager/FilesDeleted.hpp"
#include "castor/stager/GCRemovedFile.hpp"

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraGCRemovedFileCnv> s_factoryOraGCRemovedFileCnv;
const castor::ICnvFactory& OraGCRemovedFileCnvFactory = 
  s_factoryOraGCRemovedFileCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraGCRemovedFileCnv::s_insertStatementString =
"INSERT INTO GCRemovedFile (diskCopyId, id, request) VALUES (:1,ids_seq.nextval,:2) RETURNING id INTO :3";

/// SQL statement for request deletion
const std::string castor::db::ora::OraGCRemovedFileCnv::s_deleteStatementString =
"DELETE FROM GCRemovedFile WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraGCRemovedFileCnv::s_selectStatementString =
"SELECT diskCopyId, id, request FROM GCRemovedFile WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraGCRemovedFileCnv::s_updateStatementString =
"UPDATE GCRemovedFile SET diskCopyId = :1 WHERE id = :2";

/// SQL statement for type storage
const std::string castor::db::ora::OraGCRemovedFileCnv::s_storeTypeStatementString =
"INSERT INTO Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraGCRemovedFileCnv::s_deleteTypeStatementString =
"DELETE FROM Id2Type WHERE id = :1";

/// SQL existence statement for member request
const std::string castor::db::ora::OraGCRemovedFileCnv::s_checkFilesDeletedExistStatementString =
"SELECT id from FilesDeleted WHERE id = :1";

/// SQL update statement for member request
const std::string castor::db::ora::OraGCRemovedFileCnv::s_updateFilesDeletedStatementString =
"UPDATE GCRemovedFile SET request = :1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraGCRemovedFileCnv::OraGCRemovedFileCnv(castor::ICnvSvc* cnvSvc) :
  OraBaseCnv(cnvSvc),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_checkFilesDeletedExistStatement(0),
  m_updateFilesDeletedStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraGCRemovedFileCnv::~OraGCRemovedFileCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraGCRemovedFileCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_checkFilesDeletedExistStatement);
    deleteStatement(m_updateFilesDeletedStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_checkFilesDeletedExistStatement = 0;
  m_updateFilesDeletedStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraGCRemovedFileCnv::ObjType() {
  return castor::stager::GCRemovedFile::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraGCRemovedFileCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraGCRemovedFileCnv::fillRep(castor::IAddress* address,
                                                   castor::IObject* object,
                                                   unsigned int type,
                                                   bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::GCRemovedFile* obj = 
    dynamic_cast<castor::stager::GCRemovedFile*>(object);
  try {
    switch (type) {
    case castor::OBJ_FilesDeleted :
      fillRepFilesDeleted(obj);
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
// fillRepFilesDeleted
//------------------------------------------------------------------------------
void castor::db::ora::OraGCRemovedFileCnv::fillRepFilesDeleted(castor::stager::GCRemovedFile* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  if (0 != obj->request()) {
    // Check checkFilesDeletedExist statement
    if (0 == m_checkFilesDeletedExistStatement) {
      m_checkFilesDeletedExistStatement = createStatement(s_checkFilesDeletedExistStatementString);
    }
    // retrieve the object from the database
    m_checkFilesDeletedExistStatement->setDouble(1, obj->request()->id());
    oracle::occi::ResultSet *rset = m_checkFilesDeletedExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad;
      ad.setCnvSvcName("OraCnvSvc");
      ad.setCnvSvcType(castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->request(), false);
    }
    // Close resultset
    m_checkFilesDeletedExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateFilesDeletedStatement) {
    m_updateFilesDeletedStatement = createStatement(s_updateFilesDeletedStatementString);
  }
  // Update local object
  m_updateFilesDeletedStatement->setDouble(1, 0 == obj->request() ? 0 : obj->request()->id());
  m_updateFilesDeletedStatement->setDouble(2, obj->id());
  m_updateFilesDeletedStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraGCRemovedFileCnv::fillObj(castor::IAddress* address,
                                                   castor::IObject* object,
                                                   unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::GCRemovedFile* obj = 
    dynamic_cast<castor::stager::GCRemovedFile*>(object);
  switch (type) {
  case castor::OBJ_FilesDeleted :
    fillObjFilesDeleted(obj);
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
// fillObjFilesDeleted
//------------------------------------------------------------------------------
void castor::db::ora::OraGCRemovedFileCnv::fillObjFilesDeleted(castor::stager::GCRemovedFile* obj)
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
  u_signed64 requestId = (u_signed64)rset->getDouble(3);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->request() &&
      (0 == requestId ||
       obj->request()->id() != requestId)) {
    obj->request()->removeFiles(obj);
    obj->setRequest(0);
  }
  // Update object or create new one
  if (0 != requestId) {
    if (0 == obj->request()) {
      obj->setRequest
        (dynamic_cast<castor::stager::FilesDeleted*>
         (cnvSvc()->getObjFromId(requestId)));
    } else {
      cnvSvc()->updateObj(obj->request());
    }
    obj->request()->addFiles(obj);
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraGCRemovedFileCnv::createRep(castor::IAddress* address,
                                                     castor::IObject* object,
                                                     bool autocommit,
                                                     unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::GCRemovedFile* obj = 
    dynamic_cast<castor::stager::GCRemovedFile*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  if (0 != obj->id()) return;
  try {
    // Check whether the statements are ok
    if (0 == m_insertStatement) {
      m_insertStatement = createStatement(s_insertStatementString);
      m_insertStatement->registerOutParam(3, oracle::occi::OCCIDOUBLE);
    }
    if (0 == m_storeTypeStatement) {
      m_storeTypeStatement = createStatement(s_storeTypeStatementString);
    }
    // Now Save the current object
    m_insertStatement->setDouble(1, obj->diskCopyId());
    m_insertStatement->setDouble(2, (type == OBJ_FilesDeleted && obj->request() != 0) ? obj->request()->id() : 0);
    m_insertStatement->executeUpdate();
    obj->setId((u_signed64)m_insertStatement->getDouble(3));
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
                    << "  diskCopyId : " << obj->diskCopyId() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  request : " << obj->request() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraGCRemovedFileCnv::updateRep(castor::IAddress* address,
                                                     castor::IObject* object,
                                                     bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::GCRemovedFile* obj = 
    dynamic_cast<castor::stager::GCRemovedFile*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setDouble(1, obj->diskCopyId());
    m_updateStatement->setDouble(2, obj->id());
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
void castor::db::ora::OraGCRemovedFileCnv::deleteRep(castor::IAddress* address,
                                                     castor::IObject* object,
                                                     bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::GCRemovedFile* obj = 
    dynamic_cast<castor::stager::GCRemovedFile*>(object);
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
castor::IObject* castor::db::ora::OraGCRemovedFileCnv::createObj(castor::IAddress* address)
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
    castor::stager::GCRemovedFile* object = new castor::stager::GCRemovedFile();
    // Now retrieve and set members
    object->setDiskCopyId((u_signed64)rset->getDouble(1));
    object->setId((u_signed64)rset->getDouble(2));
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
void castor::db::ora::OraGCRemovedFileCnv::updateObj(castor::IObject* obj)
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
    castor::stager::GCRemovedFile* object = 
      dynamic_cast<castor::stager::GCRemovedFile*>(obj);
    object->setDiskCopyId((u_signed64)rset->getDouble(1));
    object->setId((u_signed64)rset->getDouble(2));
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

