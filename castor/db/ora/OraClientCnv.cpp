/******************************************************************************
 *                      castor/db/ora/OraClientCnv.cpp
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
#include "OraClientCnv.hpp"
#include "castor/BaseAddress.hpp"
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
#include "castor/rh/Client.hpp"
#include "castor/stager/Request.hpp"

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraClientCnv> s_factoryOraClientCnv;
const castor::IFactory<castor::IConverter>& OraClientCnvFactory = 
  s_factoryOraClientCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraClientCnv::s_insertStatementString =
"INSERT INTO rh_Client (ipAddress, port, id, request) VALUES (:1,:2,:3,:4)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraClientCnv::s_deleteStatementString =
"DELETE FROM rh_Client WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraClientCnv::s_selectStatementString =
"SELECT ipAddress, port, id, request FROM rh_Client WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraClientCnv::s_updateStatementString =
"UPDATE rh_Client SET ipAddress = :1, port = :2 WHERE id = :3";

/// SQL statement for type storage
const std::string castor::db::ora::OraClientCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraClientCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL select statement for member request
const std::string castor::db::ora::OraClientCnv::s_selectRequestStatementString =
"SELECT id from rh_Request WHERE client = :1";

/// SQL delete statement for member request
const std::string castor::db::ora::OraClientCnv::s_deleteRequestStatementString =
"UPDATE rh_Request SET client = 0 WHERE client = :1";

/// SQL remote update statement for member request
const std::string castor::db::ora::OraClientCnv::s_remoteUpdateRequestStatementString =
"UPDATE rh_Request SET client = : 1 WHERE id = :2";

/// SQL existence statement for member request
const std::string castor::db::ora::OraClientCnv::s_checkRequestExistStatementString =
"SELECT id from rh_Request WHERE id = :1";

/// SQL update statement for member request
const std::string castor::db::ora::OraClientCnv::s_updateRequestStatementString =
"UPDATE rh_Client SET request = : 1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraClientCnv::OraClientCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_selectRequestStatement(0),
  m_deleteRequestStatement(0),
  m_remoteUpdateRequestStatement(0),
  m_checkRequestExistStatement(0),
  m_updateRequestStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraClientCnv::~OraClientCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraClientCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_deleteRequestStatement);
    deleteStatement(m_selectRequestStatement);
    deleteStatement(m_remoteUpdateRequestStatement);
    deleteStatement(m_checkRequestExistStatement);
    deleteStatement(m_updateRequestStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_selectRequestStatement = 0;
  m_deleteRequestStatement = 0;
  m_remoteUpdateRequestStatement = 0;
  m_checkRequestExistStatement = 0;
  m_updateRequestStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraClientCnv::ObjType() {
  return castor::rh::Client::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraClientCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraClientCnv::fillRep(castor::IAddress* address,
                                            castor::IObject* object,
                                            unsigned int type,
                                            bool autocommit)
  throw (castor::exception::Exception) {
  castor::rh::Client* obj = 
    dynamic_cast<castor::rh::Client*>(object);
  switch (type) {
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
// fillRepRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraClientCnv::fillRepRequest(castor::rh::Client* obj)
  throw (castor::exception::Exception) {
  // Check selectRequest statement
  if (0 == m_selectRequestStatement) {
    m_selectRequestStatement = createStatement(s_selectRequestStatementString);
  }
  // retrieve the object from the database
  m_selectRequestStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectRequestStatement->executeQuery();
  if (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    u_signed64 requestId = (u_signed64)rset->getDouble(4);
    if (0 != requestId &&
        0 == obj->request() ||
        obj->request()->id() != requestId) {
      if (0 == m_deleteRequestStatement) {
        m_deleteRequestStatement = createStatement(s_deleteRequestStatementString);
      }
      m_deleteRequestStatement->setDouble(1, obj->id());
      m_deleteRequestStatement->executeUpdate();
    }
  }
  // Close resultset
  m_selectRequestStatement->closeResultSet(rset);
  if (0 != obj->request()) {
    // Check checkRequestExist statement
    if (0 == m_checkRequestExistStatement) {
      m_checkRequestExistStatement = createStatement(s_checkRequestExistStatementString);
    }
    // retrieve the object from the database
    m_checkRequestExistStatement->setDouble(1, obj->request()->id());
    oracle::occi::ResultSet *rset = m_checkRequestExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->request(), false, OBJ_IClient);
    } else {
      // Check remote update statement
      if (0 == m_remoteUpdateRequestStatement) {
        m_remoteUpdateRequestStatement = createStatement(s_remoteUpdateRequestStatementString);
      }
      // Update remote object
      m_remoteUpdateRequestStatement->setDouble(1, obj->id());
      m_remoteUpdateRequestStatement->setDouble(2, obj->request()->id());
      m_remoteUpdateRequestStatement->executeUpdate();
    }
    // Close resultset
    m_checkRequestExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateRequestStatement) {
    m_updateRequestStatement = createStatement(s_updateRequestStatementString);
  }
  // Update local object
  m_updateRequestStatement->setDouble(1, 0 == obj->request() ? 0 : obj->request()->id());
  m_updateRequestStatement->setDouble(2, obj->id());
  m_updateRequestStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraClientCnv::fillObj(castor::IAddress* address,
                                            castor::IObject* object,
                                            unsigned int type)
  throw (castor::exception::Exception) {
  castor::rh::Client* obj = 
    dynamic_cast<castor::rh::Client*>(object);
  switch (type) {
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
// fillObjRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraClientCnv::fillObjRequest(castor::rh::Client* obj)
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
  u_signed64 requestId = (u_signed64)rset->getDouble(4);
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
void castor::db::ora::OraClientCnv::createRep(castor::IAddress* address,
                                              castor::IObject* object,
                                              bool autocommit,
                                              unsigned int type)
  throw (castor::exception::Exception) {
  castor::rh::Client* obj = 
    dynamic_cast<castor::rh::Client*>(object);
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
    m_insertStatement->setInt(1, obj->ipAddress());
    m_insertStatement->setInt(2, obj->port());
    m_insertStatement->setDouble(3, obj->id());
    m_insertStatement->setDouble(4, (type == OBJ_Request && obj->request() != 0) ? obj->request()->id() : 0);
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
                    << "  ipAddress : " << obj->ipAddress() << std::endl
                    << "  port : " << obj->port() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  request : " << obj->request() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraClientCnv::updateRep(castor::IAddress* address,
                                              castor::IObject* object,
                                              bool autocommit)
  throw (castor::exception::Exception) {
  castor::rh::Client* obj = 
    dynamic_cast<castor::rh::Client*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setInt(1, obj->ipAddress());
    m_updateStatement->setInt(2, obj->port());
    m_updateStatement->setDouble(3, obj->id());
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
void castor::db::ora::OraClientCnv::deleteRep(castor::IAddress* address,
                                              castor::IObject* object,
                                              bool autocommit)
  throw (castor::exception::Exception) {
  castor::rh::Client* obj = 
    dynamic_cast<castor::rh::Client*>(object);
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
castor::IObject* castor::db::ora::OraClientCnv::createObj(castor::IAddress* address)
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
    castor::rh::Client* object = new castor::rh::Client();
    // Now retrieve and set members
    object->setIpAddress(rset->getInt(1));
    object->setPort(rset->getInt(2));
    object->setId((u_signed64)rset->getDouble(3));
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
void castor::db::ora::OraClientCnv::updateObj(castor::IObject* obj)
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
    castor::rh::Client* object = 
      dynamic_cast<castor::rh::Client*>(obj);
    object->setIpAddress(rset->getInt(1));
    object->setPort(rset->getInt(2));
    object->setId((u_signed64)rset->getDouble(3));
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

