/******************************************************************************
 *                      castor/db/ora/OraFileClassCnv.cpp
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
#include "OraFileClassCnv.hpp"
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
#include "castor/stager/FileClass.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraFileClassCnv> s_factoryOraFileClassCnv;
const castor::ICnvFactory& OraFileClassCnvFactory = 
  s_factoryOraFileClassCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraFileClassCnv::s_insertStatementString =
"INSERT INTO FileClass (name, minFileSize, maxFileSize, nbCopies, id) VALUES (:1,:2,:3,:4,:5)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraFileClassCnv::s_deleteStatementString =
"DELETE FROM FileClass WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraFileClassCnv::s_selectStatementString =
"SELECT name, minFileSize, maxFileSize, nbCopies, id FROM FileClass WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraFileClassCnv::s_updateStatementString =
"UPDATE FileClass SET name = :1, minFileSize = :2, maxFileSize = :3, nbCopies = :4 WHERE id = :5";

/// SQL statement for type storage
const std::string castor::db::ora::OraFileClassCnv::s_storeTypeStatementString =
"INSERT INTO Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraFileClassCnv::s_deleteTypeStatementString =
"DELETE FROM Id2Type WHERE id = :1";

/// SQL select statement for member 
const std::string castor::db::ora::OraFileClassCnv::s_selectCastorFileStatementString =
"SELECT id from CastorFile WHERE fileClass = :1";

/// SQL delete statement for member 
const std::string castor::db::ora::OraFileClassCnv::s_deleteCastorFileStatementString =
"UPDATE CastorFile SET fileClass = 0 WHERE id = :1";

/// SQL remote update statement for member 
const std::string castor::db::ora::OraFileClassCnv::s_remoteUpdateCastorFileStatementString =
"UPDATE CastorFile SET fileClass = :1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraFileClassCnv::OraFileClassCnv(castor::ICnvSvc* cnvSvc) :
  OraBaseCnv(cnvSvc),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_selectCastorFileStatement(0),
  m_deleteCastorFileStatement(0),
  m_remoteUpdateCastorFileStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraFileClassCnv::~OraFileClassCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraFileClassCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_deleteCastorFileStatement);
    deleteStatement(m_selectCastorFileStatement);
    deleteStatement(m_remoteUpdateCastorFileStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_selectCastorFileStatement = 0;
  m_deleteCastorFileStatement = 0;
  m_remoteUpdateCastorFileStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraFileClassCnv::ObjType() {
  return castor::stager::FileClass::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraFileClassCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraFileClassCnv::fillRep(castor::IAddress* address,
                                               castor::IObject* object,
                                               unsigned int type,
                                               bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::FileClass* obj = 
    dynamic_cast<castor::stager::FileClass*>(object);
  try {
    switch (type) {
    case castor::OBJ_CastorFile :
      fillRepCastorFile(obj);
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
// fillRepCastorFile
//------------------------------------------------------------------------------
void castor::db::ora::OraFileClassCnv::fillRepCastorFile(castor::stager::FileClass* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // check select statement
  if (0 == m_selectCastorFileStatement) {
    m_selectCastorFileStatement = createStatement(s_selectCastorFileStatementString);
  }
  // Get current database data
  std::set<int> List;
  m_selectCastorFileStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectCastorFileStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    List.insert(rset->getInt(1));
  }
  m_selectCastorFileStatement->closeResultSet(rset);
  // update  and create new ones
  for (std::vector<castor::stager::CastorFile*>::iterator it = obj->().begin();
       it != obj->().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = List.find((*it)->id())) == List.end()) {
      cnvSvc()->createRep(0, *it, false, OBJ_FileClass);
    } else {
      // Check remote update statement
      if (0 == m_remoteUpdateCastorFileStatement) {
        m_remoteUpdateCastorFileStatement = createStatement(s_remoteUpdateCastorFileStatementString);
      }
      // Update remote object
      m_remoteUpdateCastorFileStatement->setDouble(1, obj->id());
      m_remoteUpdateCastorFileStatement->setDouble(2, (*it)->id());
      m_remoteUpdateCastorFileStatement->executeUpdate();
      List.erase(item);
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = List.begin();
       it != List.end();
       it++) {
    if (0 == m_deleteCastorFileStatement) {
      m_deleteCastorFileStatement = createStatement(s_deleteCastorFileStatementString);
    }
    m_deleteCastorFileStatement->setDouble(1, *it);
    m_deleteCastorFileStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraFileClassCnv::fillObj(castor::IAddress* address,
                                               castor::IObject* object,
                                               unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::FileClass* obj = 
    dynamic_cast<castor::stager::FileClass*>(object);
  switch (type) {
  case castor::OBJ_CastorFile :
    fillObjCastorFile(obj);
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
// fillObjCastorFile
//------------------------------------------------------------------------------
void castor::db::ora::OraFileClassCnv::fillObjCastorFile(castor::stager::FileClass* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectCastorFileStatement) {
    m_selectCastorFileStatement = createStatement(s_selectCastorFileStatementString);
  }
  // retrieve the object from the database
  std::set<int> List;
  m_selectCastorFileStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectCastorFileStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    List.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectCastorFileStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::CastorFile*> toBeDeleted;
  for (std::vector<castor::stager::CastorFile*>::iterator it = obj->().begin();
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
  for (std::vector<castor::stager::CastorFile*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->remove