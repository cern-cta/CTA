/******************************************************************************
 *                      castor/db/ora/OraTapeCopyCnv.cpp
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
#include "OraTapeCopyCnv.hpp"
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
#include "castor/stager/Segment.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include <set>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraTapeCopyCnv> s_factoryOraTapeCopyCnv;
const castor::ICnvFactory& OraTapeCopyCnvFactory = 
  s_factoryOraTapeCopyCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraTapeCopyCnv::s_insertStatementString =
"INSERT INTO TapeCopy (copyNb, id, castorFile, status) VALUES (:1,:2,:3,:4)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteStatementString =
"DELETE FROM TapeCopy WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraTapeCopyCnv::s_selectStatementString =
"SELECT copyNb, id, castorFile, status FROM TapeCopy WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraTapeCopyCnv::s_updateStatementString =
"UPDATE TapeCopy SET copyNb = :1, status = :2 WHERE id = :3";

/// SQL statement for type storage
const std::string castor::db::ora::OraTapeCopyCnv::s_storeTypeStatementString =
"INSERT INTO Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteTypeStatementString =
"DELETE FROM Id2Type WHERE id = :1";

/// SQL insert statement for member stream
const std::string castor::db::ora::OraTapeCopyCnv::s_insertStreamStatementString =
"INSERT INTO Stream2TapeCopy (Child, Parent) VALUES (:1, :2)";

/// SQL delete statement for member stream
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteStreamStatementString =
"DELETE FROM Stream2TapeCopy WHERE Child = :1 AND Parent = :2";

/// SQL select statement for member stream
const std::string castor::db::ora::OraTapeCopyCnv::s_selectStreamStatementString =
"SELECT Parent from Stream2TapeCopy WHERE Child = :1";

/// SQL select statement for member segments
const std::string castor::db::ora::OraTapeCopyCnv::s_selectSegmentStatementString =
"SELECT id from Segment WHERE copy = :1";

/// SQL delete statement for member segments
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteSegmentStatementString =
"UPDATE Segment SET copy = 0 WHERE id = :1";

/// SQL remote update statement for member segments
const std::string castor::db::ora::OraTapeCopyCnv::s_remoteUpdateSegmentStatementString =
"UPDATE Segment SET copy = :1 WHERE id = :2";

/// SQL existence statement for member castorFile
const std::string castor::db::ora::OraTapeCopyCnv::s_checkCastorFileExistStatementString =
"SELECT id from CastorFile WHERE id = :1";

/// SQL update statement for member castorFile
const std::string castor::db::ora::OraTapeCopyCnv::s_updateCastorFileStatementString =
"UPDATE TapeCopy SET castorFile = : 1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraTapeCopyCnv::OraTapeCopyCnv(castor::ICnvSvc* cnvSvc) :
  OraBaseCnv(cnvSvc),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_insertStreamStatement(0),
  m_deleteStreamStatement(0),
  m_selectStreamStatement(0),
  m_selectSegmentStatement(0),
  m_deleteSegmentStatement(0),
  m_remoteUpdateSegmentStatement(0),
  m_checkCastorFileExistStatement(0),
  m_updateCastorFileStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraTapeCopyCnv::~OraTapeCopyCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_insertStreamStatement);
    deleteStatement(m_deleteStreamStatement);
    deleteStatement(m_selectStreamStatement);
    deleteStatement(m_deleteSegmentStatement);
    deleteStatement(m_selectSegmentStatement);
    deleteStatement(m_remoteUpdateSegmentStatement);
    deleteStatement(m_checkCastorFileExistStatement);
    deleteStatement(m_updateCastorFileStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertStreamStatement = 0;
  m_deleteStreamStatement = 0;
  m_selectStreamStatement = 0;
  m_selectSegmentStatement = 0;
  m_deleteSegmentStatement = 0;
  m_remoteUpdateSegmentStatement = 0;
  m_checkCastorFileExistStatement = 0;
  m_updateCastorFileStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraTapeCopyCnv::ObjType() {
  return castor::stager::TapeCopy::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraTapeCopyCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillRep(castor::IAddress* address,
                                              castor::IObject* object,
                                              unsigned int type,
                                              bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::TapeCopy* obj = 
    dynamic_cast<castor::stager::TapeCopy*>(object);
  try {
    switch (type) {
    case castor::OBJ_Stream :
      fillRepStream(obj);
      break;
    case castor::OBJ_Segment :
      fillRepSegment(obj);
      break;
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
// fillRepStream
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillRepStream(castor::stager::TapeCopy* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // check select statement
  if (0 == m_selectStreamStatement) {
    m_selectStreamStatement = createStatement(s_selectStreamStatementString);
  }
  // Get current database data
  std::set<int> streamList;
  m_selectStreamStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectStreamStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    streamList.insert(rset->getInt(1));
  }
  m_selectStreamStatement->closeResultSet(rset);
  // update stream and create new ones
  for (std::vector<castor::stager::Stream*>::iterator it = obj->stream().begin();
       it != obj->stream().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = streamList.find((*it)->id())) == streamList.end()) {
      cnvSvc()->createRep(0, *it, false);
      if (0 == m_insertStreamStatement) {
        m_insertStreamStatement = createStatement(s_insertStreamStatementString);
      }
      m_insertStreamStatement->setDouble(1, obj->id());
      m_insertStreamStatement->setDouble(2, (*it)->id());
      m_insertStreamStatement->executeUpdate();
    } else {
      streamList.erase(item);
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = streamList.begin();
       it != streamList.end();
       it++) {
    if (0 == m_deleteStreamStatement) {
      m_deleteStreamStatement = createStatement(s_deleteStreamStatementString);
    }
    m_deleteStreamStatement->setDouble(1, obj->id());
    m_deleteStreamStatement->setDouble(2, *it);
    m_deleteStreamStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepSegment
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillRepSegment(castor::stager::TapeCopy* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // check select statement
  if (0 == m_selectSegmentStatement) {
    m_selectSegmentStatement = createStatement(s_selectSegmentStatementString);
  }
  // Get current database data
  std::set<int> segmentsList;
  m_selectSegmentStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectSegmentStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    segmentsList.insert(rset->getInt(1));
  }
  m_selectSegmentStatement->closeResultSet(rset);
  // update segments and create new ones
  for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
       it != obj->segments().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = segmentsList.find((*it)->id())) == segmentsList.end()) {
      cnvSvc()->createRep(0, *it, false, OBJ_TapeCopy);
    } else {
      // Check remote update statement
      if (0 == m_remoteUpdateSegmentStatement) {
        m_remoteUpdateSegmentStatement = createStatement(s_remoteUpdateSegmentStatementString);
      }
      // Update remote object
      m_remoteUpdateSegmentStatement->setDouble(1, obj->id());
      m_remoteUpdateSegmentStatement->setDouble(2, (*it)->id());
      m_remoteUpdateSegmentStatement->executeUpdate();
      segmentsList.erase(item);
    }
  }
  // Delete old links
  for (std::set<int>::iterator it = segmentsList.begin();
       it != segmentsList.end();
       it++) {
    if (0 == m_deleteSegmentStatement) {
      m_deleteSegmentStatement = createStatement(s_deleteSegmentStatementString);
    }
    m_deleteSegmentStatement->setDouble(1, *it);
    m_deleteSegmentStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepCastorFile
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillRepCastorFile(castor::stager::TapeCopy* obj)
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
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillObj(castor::IAddress* address,
                                              castor::IObject* object,
                                              unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::TapeCopy* obj = 
    dynamic_cast<castor::stager::TapeCopy*>(object);
  switch (type) {
  case castor::OBJ_Stream :
    fillObjStream(obj);
    break;
  case castor::OBJ_Segment :
    fillObjSegment(obj);
    break;
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
// fillObjStream
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillObjStream(castor::stager::TapeCopy* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectStreamStatement) {
    m_selectStreamStatement = createStatement(s_selectStreamStatementString);
  }
  // retrieve the object from the database
  std::set<int> streamList;
  m_selectStreamStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectStreamStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    streamList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectStreamStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::Stream*> toBeDeleted;
  for (std::vector<castor::stager::Stream*>::iterator it = obj->stream().begin();
       it != obj->stream().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = streamList.find((*it)->id())) == streamList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      streamList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::Stream*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeStream(*it);
    (*it)->removeTapeCopy(obj);
  }
  // Create new objects
  for (std::set<int>::iterator it = streamList.begin();
       it != streamList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    castor::stager::Stream* remoteObj = 
      dynamic_cast<castor::stager::Stream*>(item);
    obj->addStream(remoteObj);
    remoteObj->addTapeCopy(obj);
  }
}

//------------------------------------------------------------------------------
// fillObjSegment
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillObjSegment(castor::stager::TapeCopy* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_selectSegmentStatement) {
    m_selectSegmentStatement = createStatement(s_selectSegmentStatementString);
  }
  // retrieve the object from the database
  std::set<int> segmentsList;
  m_selectSegmentStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_selectSegmentStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    segmentsList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_selectSegmentStatement->closeResultSet(rset);
  // Update objects and mark old ones for deletion
  std::vector<castor::stager::Segment*> toBeDeleted;
  for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
       it != obj->segments().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = segmentsList.find((*it)->id())) == segmentsList.end()) {
      toBeDeleted.push_back(*it);
    } else {
      segmentsList.erase(item);
      cnvSvc()->updateObj((*it));
    }
  }
  // Delete old objects
  for (std::vector<castor::stager::Segment*>::iterator it = toBeDeleted.begin();
       it != toBeDeleted.end();
       it++) {
    obj->removeSegments(*it);
    (*it)->setCopy(0);
  }
  // Create new objects
  for (std::set<int>::iterator it = segmentsList.begin();
       it != segmentsList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    castor::stager::Segment* remoteObj = 
      dynamic_cast<castor::stager::Segment*>(item);
    obj->addSegments(remoteObj);
    remoteObj->setCopy(obj);
  }
}

//------------------------------------------------------------------------------
// fillObjCastorFile
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillObjCastorFile(castor::stager::TapeCopy* obj)
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
  u_signed64 castorFileId = (u_signed64)rset->getDouble(3);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->castorFile() &&
      (0 == castorFileId ||
       obj->castorFile()->id() != castorFileId)) {
    obj->castorFile()->removeTapeCopies(obj);
    obj->setCastorFile(0);
  }
  // Update object or create new one
  if (0 != castorFileId) {
    if (0 == obj->castorFile()) {
      obj->setCastorFile
        (dynamic_cast<castor::stager::CastorFile*>
         (cnvSvc()->getObjFromId(castorFileId)));
    } else {
      cnvSvc()->updateObj(obj->castorFile());
    }
    obj->castorFile()->addTapeCopies(obj);
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::createRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                bool autocommit,
                                                unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::TapeCopy* obj = 
    dynamic_cast<castor::stager::TapeCopy*>(object);
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
    m_insertStatement->setInt(1, obj->copyNb());
    m_insertStatement->setDouble(2, obj->id());
    m_insertStatement->setDouble(3, (type == OBJ_CastorFile && obj->castorFile() != 0) ? obj->castorFile()->id() : 0);
    m_insertStatement->setInt(4, (int)obj->status());
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
                    << "  copyNb : " << obj->copyNb() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  castorFile : " << obj->castorFile() << std::endl
                    << "  status : " << obj->status() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::updateRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::TapeCopy* obj = 
    dynamic_cast<castor::stager::TapeCopy*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setInt(1, obj->copyNb());
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
void castor::db::ora::OraTapeCopyCnv::deleteRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::TapeCopy* obj = 
    dynamic_cast<castor::stager::TapeCopy*>(object);
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
    for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
         it != obj->segments().end();
         it++) {
      cnvSvc()->deleteRep(0, *it, false);
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
castor::IObject* castor::db::ora::OraTapeCopyCnv::createObj(castor::IAddress* address)
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
    castor::stager::TapeCopy* object = new castor::stager::TapeCopy();
    // Now retrieve and set members
    object->setCopyNb(rset->getInt(1));
    object->setId((u_signed64)rset->getDouble(2));
    object->setStatus((enum castor::stager::TapeCopyStatusCodes)rset->getInt(4));
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
void castor::db::ora::OraTapeCopyCnv::updateObj(castor::IObject* obj)
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
    castor::stager::TapeCopy* object = 
      dynamic_cast<castor::stager::TapeCopy*>(obj);
    object->setCopyNb(rset->getInt(1));
    object->setId((u_signed64)rset->getDouble(2));
    object->setStatus((enum castor::stager::TapeCopyStatusCodes)rset->getInt(4));
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

