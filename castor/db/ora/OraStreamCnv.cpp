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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "OraStreamCnv.hpp"
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
#include "castor/stager/Stream.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
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
"INSERT INTO rh_Stream (initialSizeToTransfer, id, tapePool, status) VALUES (:1,:2,:3,:4)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraStreamCnv::s_deleteStatementString =
"DELETE FROM rh_Stream WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraStreamCnv::s_selectStatementString =
"SELECT initialSizeToTransfer, id, tapePool, status FROM rh_Stream WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraStreamCnv::s_updateStatementString =
"UPDATE rh_Stream SET initialSizeToTransfer = :1, tapePool = :2, status = :3 WHERE id = :4";

/// SQL statement for type storage
const std::string castor::db::ora::OraStreamCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraStreamCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL insert statement for member 
const std::string castor::db::ora::OraStreamCnv::s_insertStream2TapeCopyStatementString =
"INSERT INTO rh_Stream2TapeCopy (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member 
const std::string castor::db::ora::OraStreamCnv::s_deleteStream2TapeCopyStatementString =
"DELETE FROM rh_Stream2TapeCopy WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member 
const std::string castor::db::ora::OraStreamCnv::s_Stream2TapeCopyStatementString =
"SELECT Child from rh_Stream2TapeCopy WHERE Parent = :1";

/// SQL insert statement for member tapePool
const std::string castor::db::ora::OraStreamCnv::s_insertTapePool2StreamStatementString =
"INSERT INTO rh_TapePool2Stream (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member tapePool
const std::string castor::db::ora::OraStreamCnv::s_deleteTapePool2StreamStatementString =
"DELETE FROM rh_TapePool2Stream WHERE Parent = :1 AND Child = :2";

/// SQL insert statement for member status
const std::string castor::db::ora::OraStreamCnv::s_insertStream2StreamStatusCodesStatementString =
"INSERT INTO rh_Stream2StreamStatusCodes (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member status
const std::string castor::db::ora::OraStreamCnv::s_deleteStream2StreamStatusCodesStatementString =
"DELETE FROM rh_Stream2StreamStatusCodes WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member status
const std::string castor::db::ora::OraStreamCnv::s_Stream2StreamStatusCodesStatementString =
"SELECT Child from rh_Stream2StreamStatusCodes WHERE Parent = :1";

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
  m_insertStream2TapeCopyStatement(0),
  m_deleteStream2TapeCopyStatement(0),
  m_Stream2TapeCopyStatement(0),
  m_insertTapePool2StreamStatement(0),
  m_deleteTapePool2StreamStatement(0),
  m_insertStream2StreamStatusCodesStatement(0),
  m_deleteStream2StreamStatusCodesStatement(0),
  m_Stream2StreamStatusCodesStatement(0) {}

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
    deleteStatement(m_insertStream2TapeCopyStatement);
    deleteStatement(m_Stream2TapeCopyStatement);
    deleteStatement(m_insertTapePool2StreamStatement);
    deleteStatement(m_deleteTapePool2StreamStatement);
    deleteStatement(m_insertStream2StreamStatusCodesStatement);
    deleteStatement(m_Stream2StreamStatusCodesStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertStream2TapeCopyStatement = 0;
  m_deleteStream2TapeCopyStatement = 0;
  m_Stream2TapeCopyStatement = 0;
  m_insertTapePool2StreamStatement = 0;
  m_deleteTapePool2StreamStatement = 0;
  m_insertStream2StreamStatusCodesStatement = 0;
  m_deleteStream2StreamStatusCodesStatement = 0;
  m_Stream2StreamStatusCodesStatement = 0;
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
  switch (type) {
  case castor::OBJ_TapeCopy :
    fillRepTapeCopy(obj);
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
}

//------------------------------------------------------------------------------
// fillRepTapeCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillRepTapeCopy(castor::stager::Stream* obj)
  throw (castor::exception::Exception) {
  // check select statement
  if (0 == m_Stream2TapeCopyStatement) {
    m_Stream2TapeCopyStatement = createStatement(s_Stream2TapeCopyStatementString);
  }
  // Get current database data
  std::set<int> List;
  m_Stream2TapeCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_Stream2TapeCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    List.insert(rset->getInt(1));
  }
  m_Stream2TapeCopyStatement->closeResultSet(rset);
  // update segments and create new ones
  for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->().begin();
       it != obj->().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = List.find((*it)->id())) == List.end()) {
      cnvSvc()->createRep(0, *it, false);
    } else {
      List.erase(item);
      cnvSvc()->updateRep(0, *it, false);
    }
  }
  // Delete old data
  for (std::set<int>::iterator it = List.begin();
       it != List.end();
       it++) {
    castor::db::DbAddress ad(*it, " ", 0);
    cnvSvc()->deleteRepByAddress(&ad, false);
  }
}

//------------------------------------------------------------------------------
// fillRepTapePool
//------------------------------------------------------------------------------
void castor::db::ora::OraStreamCnv::fillRepTapePool(castor::stager::Stream* obj)
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
  u_signed64 tapePoolId = (u_signed64)rset->getDouble(3);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(tapePoolId, " ", 0);
  // Check whether old object should be deleted
  if (0 != tapePoolId &&
      0 != obj->tapePool() &&
      obj->tapePool()->id() != tapePoolId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    tapePoolId = 0;
    if (0 == m_deleteTapePool2StreamStatement) {
      m_deleteTapePool2StreamStatement = createStatement(s_deleteTapePool2StreamStatementString);
    }
    m_deleteTapePool2StreamStatement->setDouble(1, obj->tapePool()->id());
    m_deleteTapePool2StreamStatement->setDouble(2, obj->id());
    m_deleteTapePool2StreamStatement->executeUpdate();
  }
  // Update object or create new one
  if (tapePoolId == 0) {
    if (0 != obj->tapePool()) {
      cnvSvc()->createRep(&ad, obj->tapePool(), false);
      if (0 == m_insertTapePool2StreamStatement) {
        m_insertTapePool2StreamStatement = createStatement(s_insertTapePool2StreamStatementString);
      }
      m_insertTapePool2StreamStatement->setDouble(1, obj->tapePool()->id());
      m_insertTapePool2StreamStatement->setDouble(2, obj->id());
      m_insertTapePool2StreamStatement->executeUpdate();
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->tapePool(), false);
  }
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
  if (0 == m_Stream2TapeCopyStatement) {
    m_Stream2TapeCopyStatement = createStatement(s_Stream2TapeCopyStatementString);
  }
  // retrieve the object from the database
  std::set<int> List;
  m_Stream2TapeCopyStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_Stream2TapeCopyStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    List.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_Stream2TapeCopyStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::TapeCopy*> toBeDeleted;
  for (std::vector<castor::stager::TapeCopy*>::iterator it = obj->().begin();
       it != obj->().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = List.find((*it)->id())) == List.end()) {
      toBeDeleted.push_back(*it);
    } else {
      List.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::TapeCopy*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->remove