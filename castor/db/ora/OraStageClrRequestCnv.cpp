/******************************************************************************
 *                      castor/db/ora/OraStageClrRequestCnv.cpp
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
 * @(#)$RCSfile: OraStageClrRequestCnv.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2004/10/14 09:40:15 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "OraStageClrRequestCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/IConverter.hpp"
#include "castor/IFactory.hpp"
#include "castor/IObject.hpp"
#include "castor/db/DbAddress.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/stager/StageClrRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraStageClrRequestCnv> s_factoryOraStageClrRequestCnv;
const castor::IFactory<castor::IConverter>& OraStageClrRequestCnvFactory = 
  s_factoryOraStageClrRequestCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraStageClrRequestCnv::s_insertStatementString =
"INSERT INTO rh_StageClrRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, id, svcClass, client) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraStageClrRequestCnv::s_deleteStatementString =
"DELETE FROM rh_StageClrRequest WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraStageClrRequestCnv::s_selectStatementString =
"SELECT flags, userName, euid, egid, mask, pid, machine, svcClassName, id, svcClass, client FROM rh_StageClrRequest WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraStageClrRequestCnv::s_updateStatementString =
"UPDATE rh_StageClrRequest SET flags = :1, userName = :2, euid = :3, egid = :4, mask = :5, pid = :6, machine = :7, svcClassName = :8, svcClass = :9, client = :10 WHERE id = :11";

/// SQL statement for type storage
const std::string castor::db::ora::OraStageClrRequestCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraStageClrRequestCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL statement for request status insertion
const std::string castor::db::ora::OraStageClrRequestCnv::s_insertStatusStatementString =
"INSERT INTO rh_requestsStatus (id, status, creation, lastChange) VALUES (:1, 'NEW', SYSDATE, SYSDATE)";

/// SQL statement for request status deletion
const std::string castor::db::ora::OraStageClrRequestCnv::s_deleteStatusStatementString =
"DELETE FROM rh_requestsStatus WHERE id = :1";

/// SQL insert statement for member subRequests
const std::string castor::db::ora::OraStageClrRequestCnv::s_insertRequest2SubRequestStatementString =
"INSERT INTO rh_Request2SubRequest (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member subRequests
const std::string castor::db::ora::OraStageClrRequestCnv::s_deleteRequest2SubRequestStatementString =
"DELETE FROM rh_Request2SubRequest WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member subRequests
const std::string castor::db::ora::OraStageClrRequestCnv::s_Request2SubRequestStatementString =
"SELECT Child from rh_Request2SubRequest WHERE Parent = :1";

/// SQL insert statement for member client
const std::string castor::db::ora::OraStageClrRequestCnv::s_insertRequest2IClientStatementString =
"INSERT INTO rh_Request2IClient (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member client
const std::string castor::db::ora::OraStageClrRequestCnv::s_deleteRequest2IClientStatementString =
"DELETE FROM rh_Request2IClient WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member client
const std::string castor::db::ora::OraStageClrRequestCnv::s_Request2IClientStatementString =
"SELECT Child from rh_Request2IClient WHERE Parent = :1";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraStageClrRequestCnv::OraStageClrRequestCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_insertStatusStatement(0),
  m_deleteStatusStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_insertRequest2SubRequestStatement(0),
  m_deleteRequest2SubRequestStatement(0),
  m_Request2SubRequestStatement(0),
  m_insertRequest2IClientStatement(0),
  m_deleteRequest2IClientStatement(0),
  m_Request2IClientStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraStageClrRequestCnv::~OraStageClrRequestCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_insertStatusStatement);
    deleteStatement(m_deleteStatusStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_insertRequest2SubRequestStatement);
    deleteStatement(m_Request2SubRequestStatement);
    deleteStatement(m_insertRequest2IClientStatement);
    deleteStatement(m_Request2IClientStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_insertStatusStatement = 0;
  m_deleteStatusStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertRequest2SubRequestStatement = 0;
  m_deleteRequest2SubRequestStatement = 0;
  m_Request2SubRequestStatement = 0;
  m_insertRequest2IClientStatement = 0;
  m_deleteRequest2IClientStatement = 0;
  m_Request2IClientStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraStageClrRequestCnv::ObjType() {
  return castor::stager::StageClrRequest::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraStageClrRequestCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::fillRep(castor::IAddress* address,
                                                     castor::IObject* object,
                                                     unsigned int type,
                                                     bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::StageClrRequest* obj = 
    dynamic_cast<castor::stager::StageClrRequest*>(object);
  switch (type) {
  case castor::OBJ_SvcClass :
    fillRepSvcClass(obj);
    break;
  case castor::OBJ_SubRequest :
    fillRepSubRequest(obj);
    break;
  case castor::OBJ_IClient :
    fillRepIClient(obj);
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
// fillRepSvcClass
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::fillRepSvcClass(castor::stager::StageClrRequest* obj)
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
  u_signed64 svcClassId = (u_signed64)rset->getDouble(10);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(svcClassId, " ", 0);
  // Check whether old object should be deleted
  if (0 != svcClassId &&
      0 != obj->svcClass() &&
      obj->svcClass()->id() != svcClassId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    svcClassId = 0;
  }
  // Update object or create new one
  if (svcClassId == 0) {
    if (0 != obj->svcClass()) {
      cnvSvc()->createRep(&ad, obj->svcClass(), false);
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->svcClass(), false);
  }
}

//------------------------------------------------------------------------------
// fillRepSubRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::fillRepSubRequest(castor::stager::StageClrRequest* obj)
  throw (castor::exception::Exception) {
  // check select statement
  if (0 == m_Request2SubRequestStatement) {
    m_Request2SubRequestStatement = createStatement(s_Request2SubRequestStatementString);
  }
  // Get current database data
  std::set<int> subRequestsList;
  m_Request2SubRequestStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_Request2SubRequestStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    subRequestsList.insert(rset->getInt(1));
  }
  m_Request2SubRequestStatement->closeResultSet(rset);
  // update segments and create new ones
  for (std::vector<castor::stager::SubRequest*>::iterator it = obj->subRequests().begin();
       it != obj->subRequests().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = subRequestsList.find((*it)->id())) == subRequestsList.end()) {
      cnvSvc()->createRep(0, *it, false);
      if (0 == m_insertRequest2SubRequestStatement) {
        m_insertRequest2SubRequestStatement = createStatement(s_insertRequest2SubRequestStatementString);
      }
      m_insertRequest2SubRequestStatement->setDouble(1, obj->id());
      m_insertRequest2SubRequestStatement->setDouble(2, (*it)->id());
      m_insertRequest2SubRequestStatement->executeUpdate();
    } else {
      subRequestsList.erase(item);
      cnvSvc()->updateRep(0, *it, false);
    }
  }
  // Delete old data
  for (std::set<int>::iterator it = subRequestsList.begin();
       it != subRequestsList.end();
       it++) {
    castor::db::DbAddress ad(*it, " ", 0);
    cnvSvc()->deleteRepByAddress(&ad, false);
    if (0 == m_deleteRequest2SubRequestStatement) {
      m_deleteRequest2SubRequestStatement = createStatement(s_deleteRequest2SubRequestStatementString);
    }
    m_deleteRequest2SubRequestStatement->setDouble(1, obj->id());
    m_deleteRequest2SubRequestStatement->setDouble(2, *it);
    m_deleteRequest2SubRequestStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepIClient
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::fillRepIClient(castor::stager::StageClrRequest* obj)
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
  u_signed64 clientId = (u_signed64)rset->getDouble(11);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(clientId, " ", 0);
  // Check whether old object should be deleted
  if (0 != clientId &&
      0 != obj->client() &&
      obj->client()->id() != clientId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    clientId = 0;
    if (0 == m_deleteRequest2IClientStatement) {
      m_deleteRequest2IClientStatement = createStatement(s_deleteRequest2IClientStatementString);
    }
    m_deleteRequest2IClientStatement->setDouble(1, obj->id());
    m_deleteRequest2IClientStatement->setDouble(2, obj->client()->id());
    m_deleteRequest2IClientStatement->executeUpdate();
  }
  // Update object or create new one
  if (clientId == 0) {
    if (0 != obj->client()) {
      cnvSvc()->createRep(&ad, obj->client(), false);
      if (0 == m_insertRequest2IClientStatement) {
        m_insertRequest2IClientStatement = createStatement(s_insertRequest2IClientStatementString);
      }
      m_insertRequest2IClientStatement->setDouble(1, obj->id());
      m_insertRequest2IClientStatement->setDouble(2, obj->client()->id());
      m_insertRequest2IClientStatement->executeUpdate();
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->client(), false);
  }
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::fillObj(castor::IAddress* address,
                                                     castor::IObject* object,
                                                     unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::StageClrRequest* obj = 
    dynamic_cast<castor::stager::StageClrRequest*>(object);
  switch (type) {
  case castor::OBJ_SvcClass :
    fillObjSvcClass(obj);
    break;
  case castor::OBJ_SubRequest :
    fillObjSubRequest(obj);
    break;
  case castor::OBJ_IClient :
    fillObjIClient(obj);
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
void castor::db::ora::OraStageClrRequestCnv::fillObjSvcClass(castor::stager::StageClrRequest* obj)
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
  u_signed64 svcClassId = (u_signed64)rset->getDouble(10);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->svcClass() &&
      (0 == svcClassId ||
       obj->svcClass()->id() != svcClassId)) {
    delete obj->svcClass();
    obj->setSvcClass(0);
  }
  // Update object or create new one
  if (0 != svcClassId) {
    if (0 == obj->svcClass()) {
      obj->setSvcClass
        (dynamic_cast<castor::stager::SvcClass*>
         (cnvSvc()->getObjFromId(svcClassId)));
    } else if (obj->svcClass()->id() == svcClassId) {
      cnvSvc()->updateObj(obj->svcClass());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjSubRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::fillObjSubRequest(castor::stager::StageClrRequest* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_Request2SubRequestStatement) {
    m_Request2SubRequestStatement = createStatement(s_Request2SubRequestStatementString);
  }
  // retrieve the object from the database
  std::set<int> subRequestsList;
  m_Request2SubRequestStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_Request2SubRequestStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    subRequestsList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_Request2SubRequestStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::SubRequest*> toBeDeleted;
  for (std::vector<castor::stager::SubRequest*>::iterator it = obj->subRequests().begin();
       it != obj->subRequests().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = subRequestsList.find((*it)->id())) == subRequestsList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      subRequestsList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::SubRequest*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeSubRequests(*it);
    delete (*it);
  }
  // Create new objects
  for (std::set<int>::iterator it = subRequestsList.begin();
       it != subRequestsList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    obj->addSubRequests(dynamic_cast<castor::stager::SubRequest*>(item));
  }
}

//------------------------------------------------------------------------------
// fillObjIClient
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::fillObjIClient(castor::stager::StageClrRequest* obj)
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
  u_signed64 clientId = (u_signed64)rset->getDouble(11);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->client() &&
      (0 == clientId ||
       obj->client()->id() != clientId)) {
    delete obj->client();
    obj->setClient(0);
  }
  // Update object or create new one
  if (0 != clientId) {
    if (0 == obj->client()) {
      obj->setClient
        (dynamic_cast<castor::IClient*>
         (cnvSvc()->getObjFromId(clientId)));
    } else if (obj->client()->id() == clientId) {
      cnvSvc()->updateObj(obj->client());
    }
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::createRep(castor::IAddress* address,
                                                       castor::IObject* object,
                                                       bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::StageClrRequest* obj = 
    dynamic_cast<castor::stager::StageClrRequest*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_insertStatement) {
      m_insertStatement = createStatement(s_insertStatementString);
    }
    if (0 == m_insertStatusStatement) {
      m_insertStatusStatement = createStatement(s_insertStatusStatementString);
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
    m_insertStatusStatement->setDouble(1, obj->id());
    m_insertStatusStatement->executeUpdate();
    m_insertStatement->setDouble(1, obj->flags());
    m_insertStatement->setString(2, obj->userName());
    m_insertStatement->setInt(3, obj->euid());
    m_insertStatement->setInt(4, obj->egid());
    m_insertStatement->setInt(5, obj->mask());
    m_insertStatement->setInt(6, obj->pid());
    m_insertStatement->setString(7, obj->machine());
    m_insertStatement->setString(8, obj->svcClassName());
    m_insertStatement->setDouble(9, obj->id());
    m_insertStatement->setDouble(10, obj->svcClass() ? obj->svcClass()->id() : 0);
    m_insertStatement->setDouble(11, obj->client() ? obj->client()->id() : 0);
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
                    << "  flags : " << obj->flags() << std::endl
                    << "  userName : " << obj->userName() << std::endl
                    << "  euid : " << obj->euid() << std::endl
                    << "  egid : " << obj->egid() << std::endl
                    << "  mask : " << obj->mask() << std::endl
                    << "  pid : " << obj->pid() << std::endl
                    << "  machine : " << obj->machine() << std::endl
                    << "  svcClassName : " << obj->svcClassName() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  svcClass : " << obj->svcClass() << std::endl
                    << "  client : " << obj->client() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraStageClrRequestCnv::updateRep(castor::IAddress* address,
                                                       castor::IObject* object,
                                                       bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::StageClrRequest* obj = 
    dynamic_cast<castor::stager::StageClrRequest*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setDouble(1, obj->flags());
    m_updateStatement->setString(2, obj->userName());
    m_updateStatement->setInt(3, obj->euid());
    m_updateStatement->setInt(4, obj->egid());
    m_updateStatement->setInt(5, obj->mask());
    m_updateStatement->setInt(6, obj->pid());
    m_updateStatement->setString(7, obj->machine());
    m_updateStatement->setString(8, obj->svcClassName());
    m_updateStatement->setDouble(9, obj->svcClass() ? obj->svcClass()->id() : 0);
    m_updateStatement->setDouble(10, obj->client() ? obj->client()->id() : 0);
    m_updateStatement->setDouble(11, obj->id());
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
void castor::db::ora::OraStageClrRequestCnv::deleteRep(castor::IAddress* address,
                                                       castor::IObject* object,
                                                       bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::StageClrRequest* obj = 
    dynamic_cast<castor::stager::StageClrRequest*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_deleteStatement) {
      m_deleteStatement = createStatement(s_deleteStatementString);
    }
    if (0 == m_deleteStatusStatement) {
      m_deleteStatusStatement = createStatement(s_deleteStatusStatementString);
    }
    if (0 == m_deleteTypeStatement) {
      m_deleteTypeStatement = createStatement(s_deleteTypeStatementString);
    }
    // Now Delete the object
    m_deleteTypeStatement->setDouble(1, obj->id());
    m_deleteTypeStatement->executeUpdate();
    m_deleteStatement->setDouble(1, obj->id());
    m_deleteStatement->executeUpdate();
    m_deleteStatusStatement->setDouble(1, obj->id());
    m_deleteStatusStatement->executeUpdate();
    for (std::vector<castor::stager::SubRequest*>::iterator it = obj->subRequests().begin();
         it != obj->subRequests().end();
         it++) {
      cnvSvc()->deleteRep(0, *it, false);
    }
    if (obj->client() != 0) {
      cnvSvc()->deleteRep(0, obj->client(), false);
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
castor::IObject* castor::db::ora::OraStageClrRequestCnv::createObj(castor::IAddress* address)
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
    castor::stager::StageClrRequest* object = new castor::stager::StageClrRequest();
    // Now retrieve and set members
    object->setFlags((u_signed64)rset->getDouble(1));
    object->setUserName(rset->getString(2));
    object->setEuid(rset->getInt(3));
    object->setEgid(rset->getInt(4));
    object->setMask(rset->getInt(5));
    object->setPid(rset->getInt(6));
    object->setMachine(rset->getString(7));
    object->setSvcClassName(rset->getString(8));
    object->setId((u_signed64)rset->getDouble(9));
    u_signed64 svcClassId = (u_signed64)rset->getDouble(10);
    u_signed64 clientId = (u_signed64)rset->getDouble(11);
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
void castor::db::ora::OraStageClrRequestCnv::updateObj(castor::IObject* obj)
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
    castor::stager::StageClrRequest* object = 
      dynamic_cast<castor::stager::StageClrRequest*>(obj);
    object->setFlags((u_signed64)rset->getDouble(1));
    object->setUserName(rset->getString(2));
    object->setEuid(rset->getInt(3));
    object->setEgid(rset->getInt(4));
    object->setMask(rset->getInt(5));
    object->setPid(rset->getInt(6));
    object->setMachine(rset->getString(7));
    object->setSvcClassName(rset->getString(8));
    object->setId((u_signed64)rset->getDouble(9));
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

