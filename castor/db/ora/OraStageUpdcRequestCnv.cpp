/******************************************************************************
 *                      castor/db/ora/OraStageUpdcRequestCnv.cpp
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
 * @(#)$RCSfile: OraStageUpdcRequestCnv.cpp,v $ $Revision: 1.12 $ $Release$ $Date: 2004/10/25 07:48:55 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "OraStageUpdcRequestCnv.hpp"
#include "castor/BaseAddress.hpp"
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
#include "castor/stager/ReqId.hpp"
#include "castor/stager/StageUpdcRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraStageUpdcRequestCnv> s_factoryOraStageUpdcRequestCnv;
const castor::IFactory<castor::IConverter>& OraStageUpdcRequestCnvFactory = 
  s_factoryOraStageUpdcRequestCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_insertStatementString =
"INSERT INTO rh_StageUpdcRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, id, svcClass, client) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_deleteStatementString =
"DELETE FROM rh_StageUpdcRequest WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_selectStatementString =
"SELECT flags, userName, euid, egid, mask, pid, machine, svcClassName, id, svcClass, client FROM rh_StageUpdcRequest WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_updateStatementString =
"UPDATE rh_StageUpdcRequest SET flags = :1, userName = :2, euid = :3, egid = :4, mask = :5, pid = :6, machine = :7, svcClassName = :8 WHERE id = :9";

/// SQL statement for type storage
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL statement for request status insertion
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_insertStatusStatementString =
"INSERT INTO rh_requestsStatus (id, status, creation, lastChange) VALUES (:1, 'NEW', SYSDATE, SYSDATE)";

/// SQL statement for request status deletion
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_deleteStatusStatementString =
"DELETE FROM rh_requestsStatus WHERE id = :1";

/// SQL select statement for member reqids
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_selectReqIdStatementString =
"SELECT id from rh_ReqId WHERE request = :1";

/// SQL delete statement for member reqids
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_deleteReqIdStatementString =
"UPDATE rh_ReqId SET request = 0 WHERE request = :1";

/// SQL existence statement for member svcClass
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_checkSvcClassExistStatementString =
"SELECT id from rh_SvcClass WHERE id = :1";

/// SQL update statement for member svcClass
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_updateSvcClassStatementString =
"UPDATE rh_StageUpdcRequest SET svcClass = : 1 WHERE id = :2";

/// SQL select statement for member subRequests
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_selectSubRequestStatementString =
"SELECT id from rh_SubRequest WHERE request = :1";

/// SQL delete statement for member subRequests
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_deleteSubRequestStatementString =
"UPDATE rh_SubRequest SET request = 0 WHERE request = :1";

/// SQL select statement for member client
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_selectIClientStatementString =
"SELECT id from rh_IClient WHERE request = :1";

/// SQL delete statement for member client
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_deleteIClientStatementString =
"UPDATE rh_IClient SET request = 0 WHERE request = :1";

/// SQL existence statement for member client
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_checkIClientExistStatementString =
"SELECT id from rh_IClient WHERE id = :1";

/// SQL update statement for member client
const std::string castor::db::ora::OraStageUpdcRequestCnv::s_updateIClientStatementString =
"UPDATE rh_StageUpdcRequest SET client = : 1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraStageUpdcRequestCnv::OraStageUpdcRequestCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_insertStatusStatement(0),
  m_deleteStatusStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_selectReqIdStatement(0),
  m_deleteReqIdStatement(0),
  m_checkSvcClassExistStatement(0),
  m_updateSvcClassStatement(0),
  m_selectSubRequestStatement(0),
  m_deleteSubRequestStatement(0),
  m_selectIClientStatement(0),
  m_deleteIClientStatement(0),
  m_checkIClientExistStatement(0),
  m_updateIClientStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraStageUpdcRequestCnv::~OraStageUpdcRequestCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraStageUpdcRequestCnv::reset() throw() {
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
    deleteStatement(m_deleteReqIdStatement);
    deleteStatement(m_selectReqIdStatement);
    deleteStatement(m_checkSvcClassExistStatement);
    deleteStatement(m_updateSvcClassStatement);
    deleteStatement(m_deleteSubRequestStatement);
    deleteStatement(m_selectSubRequestStatement);
    deleteStatement(m_deleteIClientStatement);
    deleteStatement(m_selectIClientStatement);
    deleteStatement(m_checkIClientExistStatement);
    deleteStatement(m_updateIClientStatement);
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
  m_selectReqIdStatement = 0;
  m_deleteReqIdStatement = 0;
  m_checkSvcClassExistStatement = 0;
  m_updateSvcClassStatement = 0;
  m_selectSubRequestStatement = 0;
  m_deleteSubRequestStatement = 0;
  m_selectIClientStatement = 0;
  m_deleteIClientStatement = 0;
  m_checkIClientExistStatement = 0;
  m_updateIClientStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraStageUpdcRequestCnv::ObjType() {
  return castor::stager::StageUpdcRequest::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraStageUpdcRequestCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraStageUpdcRequestCnv::fillRep(castor::IAddress* address,
                                                      castor::IObject* object,
                                                      unsigned int type,
                                                      bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::StageUpdcRequest* obj = 
    dynamic_cast<castor::stager::StageUpdcRequest*>(object);
  switch (type) {
  case castor::OBJ_ReqId :
    fillRepReqId(obj);
    break;
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
// fillRepReqId
//------------------------------------------------------------------------------
void castor::db::ora::OraStageUpdcRequestCnv::fillRepReqId(castor::stager::StageUpdcRequest* obj)
  throw (castor::exception::Exception) {
  // check select statement
  if (0 == m_selectReqIdStatement) {
    m_selectReqIdStatement = createStatement(s_selectReqIdStatementString);
  }
  // Get current database data
  std::set<int> reqidsList;
  m_selectReqIdStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectReqIdStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    reqidsList.insert(rset->getInt(1));
  }
  m_selectReqIdStatement->closeResultSet(rset);
  // update reqids and create new ones
  for (std::vector<castor::stager::ReqId*>::iterator it = obj->reqids().begin();
       it != obj->reqids().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = reqidsList.find((*it)->id())) == reqidsList.end()) {
      cnvSvc()->createRep(0, *it, false, OBJ_ReqIdRequest);
    } else {
      reqidsList.erase(item);
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = reqidsList.begin();
       it != reqidsList.end();
       it++) {
    if (0 == m_deleteReqIdStatement) {
      m_deleteReqIdStatement = createStatement(s_deleteReqIdStatementString);
    }
    m_deleteReqIdStatement->setDouble(1, obj->id());
    m_deleteReqIdStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepSvcClass
//------------------------------------------------------------------------------
void castor::db::ora::OraStageUpdcRequestCnv::fillRepSvcClass(castor::stager::StageUpdcRequest* obj)
  throw (castor::exception::Exception) {
  if (0 != obj->svcClass()) {
    // Check checkSvcClassExist statement
    if (0 == m_checkSvcClassExistStatement) {
      m_checkSvcClassExistStatement = createStatement(s_checkSvcClassExistStatementString);
    }
    // retrieve the object from the database
    m_checkSvcClassExistStatement->setDouble(1, obj->svcClass()->id());
    oracle::occi::ResultSet *rset = m_checkSvcClassExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->svcClass(), false);
    }
    // Close resultset
    m_selectStatement->closeResultSet(rset);
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
// fillRepSubRequest
//------------------------------------------------------------------------------
void castor::db::ora::OraStageUpdcRequestCnv::fillRepSubRequest(castor::stager::StageUpdcRequest* obj)
  throw (castor::exception::Exception) {
  // check select statement
  if (0 == m_selectSubRequestStatement) {
    m_selectSubRequestStatement = createStatement(s_selectSubRequestStatementString);
  }
  // Get current database data
  std::set<int> subRequestsList;
  m_selectSubRequestStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectSubRequestStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    subRequestsList.insert(rset->getInt(1));
  }
  m_selectSubRequestStatement->closeResultSet(rset);
  // update subRequests and create new ones
  for (std::vector<castor::stager::SubRequest*>::iterator it = obj->subRequests().begin();
       it != obj->subRequests().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = subRequestsList.find((*it)->id())) == subRequestsList.end()) {
      cnvSvc()->createRep(0, *it, false, OBJ_Request);
    } else {
      subRequestsList.erase(item);
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = subRequestsList.begin();
       it != subRequestsList.end();
       it++) {
    if (0 == m_deleteSubRequestStatement) {
      m_deleteSubRequestStatement = createStatement(s_deleteSubRequestStatementString);
    }
    m_deleteSubRequestStatement->setDouble(1, obj->id());
    m_deleteSubRequestStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepIClient
//------------------------------------------------------------------------------
void castor::db::ora::OraStageUpdcRequestCnv::fillRepIClient(castor::stager::StageUpdcRequest* obj)
  throw (castor::exception::Exception) {
  // Check selectIClient statement
  if (0 == m_selectIClientStatement) {
    m_selectIClientStatement = createStatement(s_selectIClientStatementString);
  }
  // retrieve the object from the database
  m_selectIClientStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectIClientStatement->executeQuery();
  if (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    u_signed64 clientId = (u_signed64)rset->getDouble(11);
    if (0 != clientId &&
        0 == obj->client() ||
        obj->client()->id() != clientId) {
      if (0 == m_deleteIClientStatement) {
        m_deleteIClientStatement = createStatement(s_deleteIClientStatementString);
      }
      m_deleteIClientStatement->setDouble(1, obj->id());
      m_deleteIClientStatement->executeUpdate();
    }
  }
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  if (0 != obj->client()) {
    // Check checkIClientExist statement
    if (0 == m_checkIClientExistStatement) {
      m_checkIClientExistStatement = createStatement(s_checkIClientExistStatementString);
    }
    // retrieve the object from the database
    m_checkIClientExistStatement->setDouble(1, obj->client()->id());
    oracle::occi::ResultSet *rset = m_checkIClientExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->client(), false, OBJ_Request);
    }
    // Close resultset
    m_selectStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateIClientStatement) {
    m_updateIClientStatement = createStatement(s_updateIClientStatementString);
  }
  // Update local object
  m_updateIClientStatement->setDouble(1, 0 == obj->client() ? 0 : obj->client()->id());
  m_updateIClientStatement->setDouble(2, obj->id());
  m_updateIClientStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraStageUpdcRequestCnv::fillObj(castor::IAddress* address,
                                                      castor::IObject* object,
                                                      unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::StageUpdcRequest* obj = 
    dynamic_cast<castor::stager::StageUpdcRequest*>(object);
  switch (type) {
  case castor::OBJ_ReqId :
    fillObjReqId(obj);
    break;
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
// fillObjReqId
//------------------------------------------------------------------------------
void castor::db::ora::OraStageUpdcRequestCnv::fillObjReqId(castor::stager::StageUpdcRequest* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectReqIdStatement) {
    m_selectReqIdStatement = createStatement(s_selectReqIdStatementString);
  }
  // retrieve the object from the database
  std::set<int> reqidsList;
  m_selectReqIdStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectReqIdStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    reqidsList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectReqIdStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::ReqId*> toBeDeleted;
  for (std::vector<castor::stager::ReqId*>::iterator it = obj->reqids().begin();
       it != obj->reqids().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = reqidsList.find((*it)->id())) == reqidsList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      reqidsList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::ReqId*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeReqids(*it);
    delete (*it);
  }
  // Create new objects
  for (std::set<int>::iterator it = reqidsList.begin();
       it != reqidsList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    obj->addReqids(dynamic_cast<castor::stager::ReqId*>(item));
  }
}

//------------------------------------------------------------------------------
// fillObjSvcClass
//------------------------------------------------------------------------------
void castor::db::ora::OraStageUpdcRequestCnv::fillObjSvcClass(castor::stager::StageUpdcRequest* obj)
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
void castor::db::ora::OraStageUpdcRequestCnv::fillObjSubRequest(castor::stager::StageUpdcRequest* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectSubRequestStatement) {
    m_selectSubRequestStatement = createStatement(s_selectSubRequestStatementString);
  }
  // retrieve the object from the database
  std::set<int> subRequestsList;
  m_selectSubRequestStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectSubRequestStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    subRequestsList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectSubRequestStatement->closeResultSet(rset);
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
void castor::db::ora::OraStageUpdcRequestCnv::fillObjIClient(castor::stager::StageUpdcRequest* obj)
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
void castor::db::ora::OraStageUpdcRequestCnv::createRep(castor::IAddress* address,
                                                        castor::IObject* object,
                                                        bool autocommit,
                                                        unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::StageUpdcRequest* obj = 
    dynamic_cast<castor::stager::StageUpdcRequest*>(object);
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
    m_insertStatement->setDouble(10, (type == OBJ_SvcClass && obj->svcClass() != 0) ? obj->svcClass()->id() : 0);
    m_insertStatement->setDouble(11, (type == OBJ_IClient && obj->client() != 0) ? obj->client()->id() : 0);
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
void castor::db::ora::OraStageUpdcRequestCnv::updateRep(castor::IAddress* address,
                                                        castor::IObject* object,
                                                        bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::StageUpdcRequest* obj = 
    dynamic_cast<castor::stager::StageUpdcRequest*>(object);
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
    m_updateStatement->setDouble(9, obj->id());
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
void castor::db::ora::OraStageUpdcRequestCnv::deleteRep(castor::IAddress* address,
                                                        castor::IObject* object,
                                                        bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::StageUpdcRequest* obj = 
    dynamic_cast<castor::stager::StageUpdcRequest*>(object);
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
    for (std::vector<castor::stager::ReqId*>::iterator it = obj->reqids().begin();
         it != obj->reqids().end();
         it++) {
      cnvSvc()->deleteRep(0, *it, false);
    }
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
castor::IObject* castor::db::ora::OraStageUpdcRequestCnv::createObj(castor::IAddress* address)
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
    castor::stager::StageUpdcRequest* object = new castor::stager::StageUpdcRequest();
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
void castor::db::ora::OraStageUpdcRequestCnv::updateObj(castor::IObject* obj)
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
    castor::stager::StageUpdcRequest* object = 
      dynamic_cast<castor::stager::StageUpdcRequest*>(obj);
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

