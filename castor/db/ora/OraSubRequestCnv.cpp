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
#include "castor/stager/Request.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include <list>

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
"INSERT INTO rh_SubRequest (retryCounter, fileName, protocol, poolName, xsize, id, request, status) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraSubRequestCnv::s_deleteStatementString =
"DELETE FROM rh_SubRequest WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraSubRequestCnv::s_selectStatementString =
"SELECT retryCounter, fileName, protocol, poolName, xsize, id, request, status FROM rh_SubRequest WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraSubRequestCnv::s_updateStatementString =
"UPDATE rh_SubRequest SET retryCounter = :1, fileName = :2, protocol = :3, poolName = :4, xsize = :5, request = :6, status = :7 WHERE id = :8";

/// SQL statement for type storage
const std::string castor::db::ora::OraSubRequestCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraSubRequestCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL insert statement for member request
const std::string castor::db::ora::OraSubRequestCnv::s_insertRequest2SubRequestStatementString =
"INSERT INTO rh_Request2SubRequest (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member request
const std::string castor::db::ora::OraSubRequestCnv::s_deleteRequest2SubRequestStatementString =
"DELETE FROM rh_Request2SubRequest WHERE Parent = :1 AND Child = :2";

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
  m_insertRequest2SubRequestStatement(0),
  m_deleteRequest2SubRequestStatement(0) {}

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
    deleteStatement(m_insertRequest2SubRequestStatement);
    deleteStatement(m_deleteRequest2SubRequestStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertRequest2SubRequestStatement = 0;
  m_deleteRequest2SubRequestStatement = 0;
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
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSubRequestCnv::createRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  castor::ObjectSet& alreadyDone,
                                                  bool autocommit,
                                                  bool recursive)
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
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Set ids of all objects
    int nids = obj->id() == 0 ? 1 : 0;
    // check which objects need to be saved/updated and keeps a list of them
    std::list<castor::IObject*> toBeSaved;
    std::list<castor::IObject*> toBeUpdated;
    if (alreadyDone.find(obj->request()) == alreadyDone.end() &&
        obj->request() != 0) {
      if (0 == obj->request()->id()) {
        if (!recursive) {
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "CreateNoRep called on type SubRequest while its request does not exist in the database.";
          throw ex;
        }
        toBeSaved.push_back(obj->request());
        nids++;
      } else {
        if (recursive) {
          toBeUpdated.push_back(obj->request());
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
    m_insertStatement->setInt(1, obj->retryCounter());
    m_insertStatement->setString(2, obj->fileName());
    m_insertStatement->setString(3, obj->protocol());
    m_insertStatement->setString(4, obj->poolName());
    m_insertStatement->setDouble(5, obj->xsize());
    m_insertStatement->setDouble(6, obj->id());
    m_insertStatement->setDouble(7, obj->request() ? obj->request()->id() : 0);
    m_insertStatement->setDouble(8, (int)obj->status());
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
    // Deal with request
    if (0 != obj->request()) {
      if (0 == m_insertRequest2SubRequestStatement) {
        m_insertRequest2SubRequestStatement = createStatement(s_insertRequest2SubRequestStatementString);
      }
      m_insertRequest2SubRequestStatement->setDouble(1, obj->request()->id());
      m_insertRequest2SubRequestStatement->setDouble(2, obj->id());
      m_insertRequest2SubRequestStatement->executeUpdate();
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
                    << "  retryCounter : " << obj->retryCounter() << std::endl
                    << "  fileName : " << obj->fileName() << std::endl
                    << "  protocol : " << obj->protocol() << std::endl
                    << "  poolName : " << obj->poolName() << std::endl
                    << "  xsize : " << obj->xsize() << std::endl
                    << "  id : " << obj->id() << std::endl
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
                                                  castor::ObjectSet& alreadyDone,
                                                  bool autocommit,
                                                  bool recursive)
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
      // Dealing with request
      {
        u_signed64 requestId = rset->getInt(7);
        castor::db::DbAddress ad(requestId, " ", 0);
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
        if (requestId == 0) {
          if (0 != obj->request()) {
            if (alreadyDone.find(obj->request()) == alreadyDone.end()) {
              cnvSvc()->createRep(&ad, obj->request(), alreadyDone, false, true);
              if (0 == m_insertRequest2SubRequestStatement) {
                m_insertRequest2SubRequestStatement = createStatement(s_insertRequest2SubRequestStatementString);
              }
              m_insertRequest2SubRequestStatement->setDouble(1, obj->request()->id());
              m_insertRequest2SubRequestStatement->setDouble(2, obj->id());
              m_insertRequest2SubRequestStatement->executeUpdate();
            }
          }
        } else {
          if (alreadyDone.find(obj->request()) == alreadyDone.end()) {
            cnvSvc()->updateRep(&ad, obj->request(), alreadyDone, false, recursive);
          }
        }
      }
      m_selectStatement->closeResultSet(rset);
    }
    // Now Update the current object
    m_updateStatement->setInt(1, obj->retryCounter());
    m_updateStatement->setString(2, obj->fileName());
    m_updateStatement->setString(3, obj->protocol());
    m_updateStatement->setString(4, obj->poolName());
    m_updateStatement->setDouble(5, obj->xsize());
    m_updateStatement->setDouble(6, obj->request() ? obj->request()->id() : 0);
    m_updateStatement->setDouble(7, (int)obj->status());
    m_updateStatement->setDouble(8, obj->id());
    m_updateStatement->executeUpdate();
    if (recursive) {
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
void castor::db::ora::OraSubRequestCnv::deleteRep(castor::IAddress* address,
                                                  castor::IObject* object,
                                                  castor::ObjectSet& alreadyDone,
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
    // Mark the current object as done
    alreadyDone.insert(obj);
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
castor::IObject* castor::db::ora::OraSubRequestCnv::createObj(castor::IAddress* address,
                                                              castor::ObjectCatalog& newlyCreated)
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
    castor::stager::SubRequest* object = new castor::stager::SubRequest();
    // Now retrieve and set members
    object->setRetryCounter(rset->getInt(1));
    object->setFileName(rset->getString(2));
    object->setProtocol(rset->getString(3));
    object->setPoolName(rset->getString(4));
    object->setXsize(rset->getDouble(5));
    object->setId(rset->getDouble(6));
    newlyCreated[object->id()] = object;
    u_signed64 requestId = rset->getInt(7);
    IObject* objRequest = cnvSvc()->getObjFromId(requestId, newlyCreated);
    object->setRequest(dynamic_cast<castor::stager::Request*>(objRequest));
    object->setStatus((enum castor::stager::SubRequestStatusCodes)rset->getInt(8));
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
void castor::db::ora::OraSubRequestCnv::updateObj(castor::IObject* obj,
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
    castor::stager::SubRequest* object = 
      dynamic_cast<castor::stager::SubRequest*>(obj);
    object->setRetryCounter(rset->getInt(1));
    object->setFileName(rset->getString(2));
    object->setProtocol(rset->getString(3));
    object->setPoolName(rset->getString(4));
    object->setXsize(rset->getDouble(5));
    object->setId(rset->getDouble(6));
    alreadyDone[obj->id()] = obj;
    // Dealing with request
    u_signed64 requestId = rset->getInt(7);
    if (0 != object->request() &&
        (0 == requestId ||
         object->request()->id() != requestId)) {
      delete object->request();
      object->setRequest(0);
    }
    if (0 != requestId) {
      if (0 == object->request()) {
        object->setRequest
          (dynamic_cast<castor::stager::Request*>
           (cnvSvc()->getObjFromId(requestId, alreadyDone)));
      } else if (object->request()->id() == requestId) {
        if (alreadyDone.find(object->request()->id()) == alreadyDone.end()) {
          cnvSvc()->updateObj(object->request(), alreadyDone);
        }
      }
    }
    object->setStatus((enum castor::stager::SubRequestStatusCodes)rset->getInt(8));
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

