/******************************************************************************
 *                      castor/db/ora/OraStreamCnv.cpp
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
#include "OraStreamCnv.hpp"
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
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "castor/stager/TapePool.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraStreamCnv> s_factoryOraStreamCnv;
const castor::IFactory<castor::IConverter>& OraStreamCnvFactory = 
  s_factoryOraStreamCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraStreamCnv::s_insertStatementString =
"INSERT INTO rh_Stream (initialSizeToTransfer, id, tape, tapePool, status) VALUES (:1,:2,:3,:4,:5)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraStreamCnv::s_deleteStatementString =
"DELETE FROM rh_Stream WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraStreamCnv::s_selectStatementString =
"SELECT initialSizeToTransfer, id, tape, tapePool, status FROM rh_Stream WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraStreamCnv::s_updateStatementString =
"UPDATE rh_Stream SET initialSizeToTransfer = :1, status = :2 WHERE id = :3";

/// SQL statement for type storage
const std::string castor::db::ora::OraStreamCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraStreamCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL insert statement for member tapeCopy
const std::string castor::db::ora::OraStreamCnv::s_insertTapeCopyStatementString =
"INSERT INTO rh_Stream2TapeCopy (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member tapeCopy
const std::string castor::db::ora::OraStreamCnv::s_deleteTapeCopyStatementString =
"DELETE FROM rh_Stream2TapeCopy WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member tapeCopy
const std::string castor::db::ora::OraStreamCnv::s_selectTapeCopyStatementString =
"SELECT Child from rh_Stream2TapeCopy WHERE Parent = :1";

/// SQL select statement for member tape
const std::string castor::db::ora::OraStreamCnv::s_selectTapeStatementString =
"SELECT id from rh_Tape WHERE stream = :1";

/// SQL delete statement for member tape
const std::string castor::db::ora::OraStreamCnv::s_deleteTapeStatementString =
"UPDATE rh_Tape SET stream = 0 WHERE stream = :1";

/// SQL remote update statement for member tape
const std::string castor::db::ora::OraStreamCnv::s_remoteUpdateTapeStatementString =
"UPDATE rh_Tape SET stream = : 1 WHERE id = :2";

/// SQL existence statement for member tape
const std::string castor::db::ora::OraStreamCnv::s_checkTapeExistStatementString =
"SELECT id from rh_Tape WHERE id = :1";

/// SQL update statement for member tape
const std::string castor::db::ora::OraStreamCnv::s_updateTapeStatementString =
"UPDATE rh_Stream SET tape = : 1 WHERE id = :2";

/// SQL existence statement for member tapePool
const std::string castor::db::ora::OraStreamCnv::s_checkTapePoolExistStatementString =
"SELECT id from rh_TapePool WHERE id = :1";

/// SQL update statement for member tapePool
const std::string castor::db::ora::OraStreamCnv::s_updateTapePoolStatementString =
"UPDATE rh_Stream SET tapePool = : 1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraStreamCnv::OraStreamCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_insertTapeCopyStatement(0),
  m_deleteTapeCopyStatement(0),
  m_selectTapeCopyStatement(0),
  m_selectTapeStatement(0),
  m_deleteTapeStatement(0),
  m_remoteUpdateTapeStatement(0),
  m_checkTapeExistStatement(0),
  m_updateTapeStatement(0),
  m_checkTapePoolExistStatement(0),
  m_updateTapePoolStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraStreamCnv::~OraStreamCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_insertTapeCopyStatement);
    deleteStatement(m_deleteTapeCopyStatement);
    deleteStatement(m_selectTapeCopyStatement);
    deleteStatement(m_deleteTapeStatement);
    deleteStatement(m_selectTapeStatement);
    deleteStatement(m_remoteUpdateTapeStatement);
    deleteStatement(m_checkTapeExistStatement);
    deleteStatement(m_updateTapeStatement);
    deleteStatement(m_checkTapePoolExistStatement);
    deleteStatement(m_updateTapePoolStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertTapeCopyStatement = 0;
  m_deleteTapeCopyStatement = 0;
  m_selectTapeCopyStatement = 0;
  m_selectTapeStatement = 0;
  m_deleteTapeStatement = 0;
  m_remoteUpdateTapeStatement = 0;
  m_checkTapeExistStatement = 0;
  m_updateTapeStatement = 0;
  m_checkTapePoolExistStatement = 0;
  m_updateTapePoolStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraStreamCnv::ObjType() {
  return castor::stager::Stream::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraStreamCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillRep(castor::IAddress* address,
                                            castor::IObject* object,
                                            unsigned int type,
                                            bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::Stream* obj = 
    dynamic_cast<castor::stager::Stream*>(object);
  try {
    switch (type) {
    case castor::OBJ_TapeCopy :
      fillRepTapeCopy(obj);
      break;
    case castor::OBJ_Tape :
      fillRepTape(obj);
      break;
    case castor::OBJ_TapePool :
      fillRepTapePool(obj);
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
// fillRepTapeCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillRepTapeCopy(castor::stager::Stream* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // check select statement
  if (0 == m_selectTapeCopyStatement) {
    m_selectTapeCopyStatement = createStatement(s_selectTapeCopyStatementString);
  }
  // Get current database data
  std::set<int> tapeCopyList;
  m_selectTapeCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectTapeCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    tapeCopyList.insert(rset->getInt(1));
  }
  m_selectTapeCopyStatement->closeResultSet(rset);
  // update tapeCopy and create new ones
  for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->tapeCopy().begin();
       it != obj->tapeCopy().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = tapeCopyList.find((*it)->id())) == tapeCopyList.end()) {
      cnvSvc()->createRep(0, *it, false);
      if (0 == m_insertTapeCopyStatement) {
        m_insertTapeCopyStatement = createStatement(s_insertTapeCopyStatementString);
      }
      m_insertTapeCopyStatement->setDouble(1, obj->id());
      m_insertTapeCopyStatement->setDouble(2, (*it)->id());
      m_insertTapeCopyStatement->executeUpdate();
    } else {
      tapeCopyList.erase(item);
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = tapeCopyList.begin();
       it != tapeCopyList.end();
       it++) {
    if (0 == m_deleteTapeCopyStatement) {
      m_deleteTapeCopyStatement = createStatement(s_deleteTapeCopyStatementString);
    }
    m_deleteTapeCopyStatement->setDouble(1, obj->id());
    m_deleteTapeCopyStatement->setDouble(2, *it);
    m_deleteTapeCopyStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepTape
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillRepTape(castor::stager::Stream* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // Check selectTape statement
  if (0 == m_selectTapeStatement) {
    m_selectTapeStatement = createStatement(s_selectTapeStatementString);
  }
  // retrieve the object from the database
  m_selectTapeStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectTapeStatement->executeQuery();
  if (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    u_signed64 tapeId = (u_signed64)rset->getDouble(1);
    if (0 != tapeId &&
        0 == obj->tape() ||
        obj->tape()->id() != tapeId) {
      if (0 == m_deleteTapeStatement) {
        m_deleteTapeStatement = createStatement(s_deleteTapeStatementString);
      }
      m_deleteTapeStatement->setDouble(1, obj->id());
      m_deleteTapeStatement->executeUpdate();
    }
  }
  // Close resultset
  m_selectTapeStatement->closeResultSet(rset);
  if (0 != obj->tape()) {
    // Check checkTapeExist statement
    if (0 == m_checkTapeExistStatement) {
      m_checkTapeExistStatement = createStatement(s_checkTapeExistStatementString);
    }
    // retrieve the object from the database
    m_checkTapeExistStatement->setDouble(1, obj->tape()->id());
    oracle::occi::ResultSet *rset = m_checkTapeExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->tape(), false, OBJ_Stream);
    } else {
      // Check remote update statement
      if (0 == m_remoteUpdateTapeStatement) {
        m_remoteUpdateTapeStatement = createStatement(s_remoteUpdateTapeStatementString);
      }
      // Update remote object
      m_remoteUpdateTapeStatement->setDouble(1, obj->id());
      m_remoteUpdateTapeStatement->setDouble(2, obj->tape()->id());
      m_remoteUpdateTapeStatement->executeUpdate();
    }
    // Close resultset
    m_checkTapeExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateTapeStatement) {
    m_updateTapeStatement = createStatement(s_updateTapeStatementString);
  }
  // Update local object
  m_updateTapeStatement->setDouble(1, 0 == obj->tape() ? 0 : obj->tape()->id());
  m_updateTapeStatement->setDouble(2, obj->id());
  m_updateTapeStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillRepTapePool
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillRepTapePool(castor::stager::Stream* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  if (0 != obj->tapePool()) {
    // Check checkTapePoolExist statement
    if (0 == m_checkTapePoolExistStatement) {
      m_checkTapePoolExistStatement = createStatement(s_checkTapePoolExistStatementString);
    }
    // retrieve the object from the database
    m_checkTapePoolExistStatement->setDouble(1, obj->tapePool()->id());
    oracle::occi::ResultSet *rset = m_checkTapePoolExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->tapePool(), false);
    }
    // Close resultset
    m_checkTapePoolExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateTapePoolStatement) {
    m_updateTapePoolStatement = createStatement(s_updateTapePoolStatementString);
  }
  // Update local object
  m_updateTapePoolStatement->setDouble(1, 0 == obj->tapePool() ? 0 : obj->tapePool()->id());
  m_updateTapePoolStatement->setDouble(2, obj->id());
  m_updateTapePoolStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillObj(castor::IAddress* address,
                                            castor::IObject* object,
                                            unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::Stream* obj = 
    dynamic_cast<castor::stager::Stream*>(object);
  switch (type) {
  case castor::OBJ_TapeCopy :
    fillObjTapeCopy(obj);
    break;
  case castor::OBJ_Tape :
    fillObjTape(obj);
    break;
  case castor::OBJ_TapePool :
    fillObjTapePool(obj);
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
// fillObjTapeCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillObjTapeCopy(castor::stager::Stream* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectTapeCopyStatement) {
    m_selectTapeCopyStatement = createStatement(s_selectTapeCopyStatementString);
  }
  // retrieve the object from the database
  std::set<int> tapeCopyList;
  m_selectTapeCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectTapeCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    tapeCopyList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectTapeCopyStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::TapeCopy*> toBeDeleted;
  for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->tapeCopy().begin();
       it != obj->tapeCopy().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = tapeCopyList.find((*it)->id())) == tapeCopyList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      tapeCopyList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::TapeCopy*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeTapeCopy(*it);
    delete (*it);
  }
  // Create new objects
  for (std::set<int>::iterator it = tapeCopyList.begin();
       it != tapeCopyList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    obj->addTapeCopy(dynamic_cast<castor::stager::TapeCopy*>(item));
  }
}

//------------------------------------------------------------------------------
// fillObjTape
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillObjTape(castor::stager::Stream* obj)
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
  u_signed64 tapeId = (u_signed64)rset->getDouble(3);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->tape() &&
      (0 == tapeId ||
       obj->tape()->id() != tapeId)) {
    delete obj->tape();
    obj->setTape(0);
  }
  // Update object or create new one
  if (0 != tapeId) {
    if (0 == obj->tape()) {
      obj->setTape
        (dynamic_cast<castor::stager::Tape*>
         (cnvSvc()->getObjFromId(tapeId)));
    } else if (obj->tape()->id() == tapeId) {
      cnvSvc()->updateObj(obj->tape());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjTapePool
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillObjTapePool(castor::stager::Stream* obj)
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
  u_signed64 tapePoolId = (u_signed64)rset->getDouble(4);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->tapePool() &&
      (0 == tapePoolId ||
       obj->tapePool()->id() != tapePoolId)) {
    delete obj->tapePool();
    obj->setTapePool(0);
  }
  // Update object or create new one
  if (0 != tapePoolId) {
    if (0 == obj->tapePool()) {
      obj->setTapePool
        (dynamic_cast<castor::stager::TapePool*>
         (cnvSvc()->getObjFromId(tapePoolId)));
    } else if (obj->tapePool()->id() == tapePoolId) {
      cnvSvc()->updateObj(obj->tapePool());
    }
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::createRep(castor::IAddress* address,
                                              castor::IObject* object,
                                              bool autocommit,
                                              unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::Stream* obj = 
    dynamic_cast<castor::stager::Stream*>(object);
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
    m_insertStatement->setDouble(1, obj->initialSizeToTransfer());
    m_insertStatement->setDouble(2, obj->id());
    m_insertStatement->setDouble(3, (type == OBJ_Tape && obj->tape() != 0) ? obj->tape()->id() : 0);
    m_insertStatement->setDouble(4, (type == OBJ_TapePool && obj->tapePool() != 0) ? obj->tapePool()->id() : 0);
    m_insertStatement->setInt(5, (int)obj->status());
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
                    << "  initialSizeToTransfer : " << obj->initialSizeToTransfer() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  tape : " << obj->tape() << std::endl
                    << "  tapePool : " << obj->tapePool() << std::endl
                    << "  status : " << obj->status() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::updateRep(castor::IAddress* address,
                                              castor::IObject* object,
                                              bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::Stream* obj = 
    dynamic_cast<castor::stager::Stream*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setDouble(1, obj->initialSizeToTransfer());
    m_updateStatement->setInt(2, (int)obj->status());
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
void castor::db::ora::OraStreamCnv::deleteRep(castor::IAddress* address,
                                              castor::IObject* object,
                                              bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::Stream* obj = 
    dynamic_cast<castor::stager::Stream*>(object);
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
castor::IObject* castor::db::ora::OraStreamCnv::createObj(castor::IAddress* address)
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
    castor::stager::Stream* object = new castor::stager::Stream();
    // Now retrieve and set members
    object->setInitialSizeToTransfer((u_signed64)rset->getDouble(1));
    object->setId((u_signed64)rset->getDouble(2));
    object->setStatus((enum castor::stager::StreamStatusCodes)rset->getInt(5));
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
void castor::db::ora::OraStreamCnv::updateObj(castor::IObject* obj)
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
    castor::stager::Stream* object = 
      dynamic_cast<castor::stager::Stream*>(obj);
    object->setInitialSizeToTransfer((u_signed64)rset->getDouble(1));
    object->setId((u_signed64)rset->getDouble(2));
    object->setStatus((enum castor::stager::StreamStatusCodes)rset->getInt(5));
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

