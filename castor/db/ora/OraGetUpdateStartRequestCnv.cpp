/******************************************************************************
 *                      castor/db/ora/OraGetUpdateStartRequestCnv.cpp
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
#include "OraGetUpdateStartRequestCnv.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/ICnvFactory.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/IObject.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/SvcClass.hpp"

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraGetUpdateStartRequestCnv> s_factoryOraGetUpdateStartRequestCnv;
const castor::ICnvFactory& OraGetUpdateStartRequestCnvFactory = 
  s_factoryOraGetUpdateStartRequestCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_insertStatementString =
"INSERT INTO GetUpdateStartRequest (subreqId, diskServer, fileSystem, flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,ids_seq.nextval,:16,:17) RETURNING id INTO :18";

/// SQL statement for request deletion
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_deleteStatementString =
"DELETE FROM GetUpdateStartRequest WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_selectStatementString =
"SELECT subreqId, diskServer, fileSystem, flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client FROM GetUpdateStartRequest WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_updateStatementString =
"UPDATE GetUpdateStartRequest SET subreqId = :1, diskServer = :2, fileSystem = :3, flags = :4, userName = :5, euid = :6, egid = :7, mask = :8, pid = :9, machine = :10, svcClassName = :11, userTag = :12, reqId = :13, lastModificationTime = :14 WHERE id = :15";

/// SQL statement for type storage
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_storeTypeStatementString =
"INSERT INTO Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_deleteTypeStatementString =
"DELETE FROM Id2Type WHERE id = :1";

/// SQL statement for request status insertion
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_insertStatusStatementString =
"INSERT INTO requestsStatus (id, status, creation, lastChange) VALUES (:1, 'NEW', SYSDATE, SYSDATE)";

/// SQL statement for request status deletion
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_deleteStatusStatementString =
"DELETE FROM requestsStatus WHERE id = :1";

/// SQL existence statement for member svcClass
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_checkSvcClassExistStatementString =
"SELECT id from SvcClass WHERE id = :1";

/// SQL update statement for member svcClass
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_updateSvcClassStatementString =
"UPDATE GetUpdateStartRequest SET svcClass = :1 WHERE id = :2";

/// SQL update statement for member client
const std::string castor::db::ora::OraGetUpdateStartRequestCnv::s_updateIClientStatementString =
"UPDATE GetUpdateStartRequest SET client = :1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraGetUpdateStartRequestCnv::OraGetUpdateStartRequestCnv(castor::ICnvSvc* cnvSvc) :
  OraBaseCnv(cnvSvc),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_insertStatusStatement(0),
  m_deleteStatusStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_checkSvcClassExistStatement(0),
  m_updateSvcClassStatement(0),
  m_updateIClientStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraGetUpdateStartRequestCnv::~OraGetUpdateStartRequestCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraGetUpdateStartRequestCnv::reset() throw() {
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
    deleteStatement(m_checkSvcClassExistStatement);
    deleteStatement(m_updateSvcClassStatement);
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
  m_checkSvcClassExistStatement = 0;
  m_updateSvcClassStatement = 0;
  m_updateIClientStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraGetUpdateStartRequestCnv::ObjType() {
  return castor::stager::GetUpdateStartRequest::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraGetUpdateStartRequestCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraGetUpdateStartRequestCnv::fillRep(castor::IAddress* address,
                                                           castor::IObject* object,
                                                           unsigned int type,
                                                           bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::GetUpdateStartRequest* obj = 
    dynamic_cast<castor::stager::GetUpdateStartRequest*>(object);
  try {
    switch (type) {
    case castor::OBJ_SvcClass :
      fillRepSvcClass(obj);
      break;
    case castor::OBJ_IClient :
      fillRepIClient(obj);
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
void castor::db::ora::OraGetUpdateStartRequestCnv::fillRepSvcClass(castor::stager::GetUpdateStartRequest* obj)
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
// fillRepIClient
//------------------------------------------------------------------------------
void castor::db::ora::OraGetUpdateStartRequestCnv::fillRepIClient(castor::stager::GetUpdateStartRequest* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
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
void castor::db::ora::OraGetUpdateStartRequestCnv::fillObj(castor::IAddress* address,
                                                           castor::IObject* object,
                                                           unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::GetUpdateStartRequest* obj = 
    dynamic_cast<castor::stager::GetUpdateStartRequest*>(object);
  switch (type) {
  case castor::OBJ_SvcClass :
    fillObjSvcClass(obj);
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
void castor::db::ora::OraGetUpdateStartRequestCnv::fillObjSvcClass(castor::stager::GetUpdateStartRequest* obj)
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
  u_signed64 svcClassId = (u_signed64)rset->getDouble(17);
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
// fillObjIClient
//------------------------------------------------------------------------------
void castor::db::ora::OraGetUpdateStartRequestCnv::fillObjIClient(castor::stager::GetUpdateStartRequest* obj)
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
  u_signed64 clientId = (u_signed64)rset->getDouble(18);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->client() &&
      (0 == clientId ||
       obj->client()->id() != clientId)) {
    obj->setClient(0);
  }
  // Update object or create new one
  if (0 != clientId) {
    if (0 == obj->client()) {
      obj->setClient
        (dynamic_cast<castor::IClient*>
         (cnvSvc()->getObjFromId(clientId)));
    } else {
      cnvSvc()->updateObj(obj->client());
    }
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraGetUpdateStartRequestCnv::createRep(castor::IAddress* address,
                                                             castor::IObject* object,
                                                             bool autocommit,
                                                             unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::GetUpdateStartRequest* obj = 
    dynamic_cast<castor::stager::GetUpdateStartRequest*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  if (0 != obj->id()) return;
  try {
    // Check whether the statements are ok
    if (0 == m_insertStatement) {
      m_insertStatement = createStatement(s_insertStatementString);
      m_insertStatement->registerOutParam(18, oracle::occi::OCCIDOUBLE);
    }
    if (0 == m_insertStatusStatement) {
      m_insertStatusStatement = createStatement(s_insertStatusStatementString);
    }
    if (0 == m_storeTypeStatement) {
      m_storeTypeStatement = createStatement(s_storeTypeStatementString);
    }
    // Now Save the current object
    m_insertStatement->setDouble(1, obj->subreqId());
    m_insertStatement->setString(2, obj->diskServer());
    m_insertStatement->setString(3, obj->fileSystem());
    m_insertStatement->setDouble(4, obj->flags());
    m_insertStatement->setString(5, obj->userName());
    m_insertStatement->setInt(6, obj->euid());
    m_insertStatement->setInt(7, obj->egid());
    m_insertStatement->setInt(8, obj->mask());
    m_insertStatement->setInt(9, obj->pid());
    m_insertStatement->setString(10, obj->machine());
    m_insertStatement->setString(11, obj->svcClassName());
    m_insertStatement->setString(12, obj->userTag());
    m_insertStatement->setString(13, obj->reqId());
    m_insertStatement->setInt(14, time(0));
    m_insertStatement->setInt(15, time(0));
    m_insertStatement->setDouble(16, (type == OBJ_SvcClass && obj->svcClass() != 0) ? obj->svcClass()->id() : 0);
    m_insertStatement->setDouble(17, (type == OBJ_IClient && obj->client() != 0) ? obj->client()->id() : 0);
    m_insertStatement->executeUpdate();
    obj->setId((u_signed64)m_insertStatement->getDouble(18));
    m_storeTypeStatement->setDouble(1, obj->id());
    m_storeTypeStatement->setInt(2, obj->type());
    m_storeTypeStatement->executeUpdate();
    m_insertStatusStatement->setDouble(1, obj->id());
    m_insertStatusStatement->executeUpdate();
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
                    << "  subreqId : " << obj->subreqId() << std::endl
                    << "  diskServer : " << obj->diskServer() << std::endl
                    << "  fileSystem : " << obj->fileSystem() << std::endl
                    << "  flags : " << obj->flags() << std::endl
                    << "  userName : " << obj->userName() << std::endl
                    << "  euid : " << obj->euid() << std::endl
                    << "  egid : " << obj->egid() << std::endl
                    << "  mask : " << obj->mask() << std::endl
                    << "  pid : " << obj->pid() << std::endl
                    << "  machine : " << obj->machine() << std::endl
                    << "  svcClassName : " << obj->svcClassName() << std::endl
                    << "  userTag : " << obj->userTag() << std::endl
                    << "  reqId : " << obj->reqId() << std::endl
                    << "  creationTime : " << obj->creationTime() << std::endl
                    << "  lastModificationTime : " << obj->lastModificationTime() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  svcClass : " << obj->svcClass() << std::endl
                    << "  client : " << obj->client() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraGetUpdateStartRequestCnv::updateRep(castor::IAddress* address,
                                                             castor::IObject* object,
                                                             bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::GetUpdateStartRequest* obj = 
    dynamic_cast<castor::stager::GetUpdateStartRequest*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setDouble(1, obj->subreqId());
    m_updateStatement->setString(2, obj->diskServer());
    m_updateStatement->setString(3, obj->fileSystem());
    m_updateStatement->setDouble(4, obj->flags());
    m_updateStatement->setString(5, obj->userName());
    m_updateStatement->setInt(6, obj->euid());
    m_updateStatement->setInt(7, obj->egid());
    m_updateStatement->setInt(8, obj->mask());
    m_updateStatement->setInt(9, obj->pid());
    m_updateStatement->setString(10, obj->machine());
    m_updateStatement->setString(11, obj->svcClassName());
    m_updateStatement->setString(12, obj->userTag());
    m_updateStatement->setString(13, obj->reqId());
    m_updateStatement->setInt(14, time(0));
    m_updateStatement->setDouble(15, obj->id());
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
void castor::db::ora::OraGetUpdateStartRequestCnv::deleteRep(castor::IAddress* address,
                                                             castor::IObject* object,
                                                             bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::GetUpdateStartRequest* obj = 
    dynamic_cast<castor::stager::GetUpdateStartRequest*>(object);
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
castor::IObject* castor::db::ora::OraGetUpdateStartRequestCnv::createObj(castor::IAddress* address)
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
    castor::stager::GetUpdateStartRequest* object = new castor::stager::GetUpdateStartRequest();
    // Now retrieve and set members
    object->setSubreqId((u_signed64)rset->getDouble(1));
    object->setDiskServer(rset->getString(2));
    object->setFileSystem(rset->getString(3));
    object->setFlags((u_signed64)rset->getDouble(4));
    object->setUserName(rset->getString(5));
    object->setEuid(rset->getInt(6));
    object->setEgid(rset->getInt(7));
    object->setMask(rset->getInt(8));
    object->setPid(rset->getInt(9));
    object->setMachine(rset->getString(10));
    object->setSvcClassName(rset->getString(11));
    object->setUserTag(rset->getString(12));
    object->setReqId(rset->getString(13));
    object->setCreationTime((u_signed64)rset->getDouble(14));
    object->setLastModificationTime((u_signed64)rset->getDouble(15));
    object->setId((u_signed64)rset->getDouble(16));
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
void castor::db::ora::OraGetUpdateStartRequestCnv::updateObj(castor::IObject* obj)
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
    castor::stager::GetUpdateStartRequest* object = 
      dynamic_cast<castor::stager::GetUpdateStartRequest*>(obj);
    object->setSubreqId((u_signed64)rset->getDouble(1));
    object->setDiskServer(rset->getString(2));
    object->setFileSystem(rset->getString(3));
    object->setFlags((u_signed64)rset->getDouble(4));
    object->setUserName(rset->getString(5));
    object->setEuid(rset->getInt(6));
    object->setEgid(rset->getInt(7));
    object->setMask(rset->getInt(8));
    object->setPid(rset->getInt(9));
    object->setMachine(rset->getString(10));
    object->setSvcClassName(rset->getString(11));
    object->setUserTag(rset->getString(12));
    object->setReqId(rset->getString(13));
    object->setCreationTime((u_signed64)rset->getDouble(14));
    object->setLastModificationTime((u_signed64)rset->getDouble(15));
    object->setId((u_signed64)rset->getDouble(16));
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

