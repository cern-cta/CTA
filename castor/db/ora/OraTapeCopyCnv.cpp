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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "OraTapeCopyCnv.hpp"
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
const castor::IFactory<castor::IConverter>& OraTapeCopyCnvFactory = 
  s_factoryOraTapeCopyCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraTapeCopyCnv::s_insertStatementString =
"INSERT INTO rh_TapeCopy (id, stream, castorFile, status) VALUES (:1,:2,:3,:4)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteStatementString =
"DELETE FROM rh_TapeCopy WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraTapeCopyCnv::s_selectStatementString =
"SELECT id, stream, castorFile, status FROM rh_TapeCopy WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraTapeCopyCnv::s_updateStatementString =
"UPDATE rh_TapeCopy SET stream = :1, castorFile = :2, status = :3 WHERE id = :4";

/// SQL statement for type storage
const std::string castor::db::ora::OraTapeCopyCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL insert statement for member segments
const std::string castor::db::ora::OraTapeCopyCnv::s_insertTapeCopy2SegmentStatementString =
"INSERT INTO rh_TapeCopy2Segment (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member segments
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteTapeCopy2SegmentStatementString =
"DELETE FROM rh_TapeCopy2Segment WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member segments
const std::string castor::db::ora::OraTapeCopyCnv::s_TapeCopy2SegmentStatementString =
"SELECT Child from rh_TapeCopy2Segment WHERE Parent = :1";

/// SQL insert statement for member castorFile
const std::string castor::db::ora::OraTapeCopyCnv::s_insertCastorFile2TapeCopyStatementString =
"INSERT INTO rh_CastorFile2TapeCopy (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member castorFile
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteCastorFile2TapeCopyStatementString =
"DELETE FROM rh_CastorFile2TapeCopy WHERE Parent = :1 AND Child = :2";

/// SQL insert statement for member status
const std::string castor::db::ora::OraTapeCopyCnv::s_insertTapeCopy2TapeCopyStatusCodesStatementString =
"INSERT INTO rh_TapeCopy2TapeCopyStatusCodes (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member status
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteTapeCopy2TapeCopyStatusCodesStatementString =
"DELETE FROM rh_TapeCopy2TapeCopyStatusCodes WHERE Parent = :1 AND Child = :2";

/// SQL select statement for member status
const std::string castor::db::ora::OraTapeCopyCnv::s_TapeCopy2TapeCopyStatusCodesStatementString =
"SELECT Child from rh_TapeCopy2TapeCopyStatusCodes WHERE Parent = :1";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraTapeCopyCnv::OraTapeCopyCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_insertTapeCopy2SegmentStatement(0),
  m_deleteTapeCopy2SegmentStatement(0),
  m_TapeCopy2SegmentStatement(0),
  m_insertCastorFile2TapeCopyStatement(0),
  m_deleteCastorFile2TapeCopyStatement(0),
  m_insertTapeCopy2TapeCopyStatusCodesStatement(0),
  m_deleteTapeCopy2TapeCopyStatusCodesStatement(0),
  m_TapeCopy2TapeCopyStatusCodesStatement(0) {}

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
    deleteStatement(m_insertTapeCopy2SegmentStatement);
    deleteStatement(m_TapeCopy2SegmentStatement);
    deleteStatement(m_insertCastorFile2TapeCopyStatement);
    deleteStatement(m_deleteCastorFile2TapeCopyStatement);
    deleteStatement(m_insertTapeCopy2TapeCopyStatusCodesStatement);
    deleteStatement(m_TapeCopy2TapeCopyStatusCodesStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertTapeCopy2SegmentStatement = 0;
  m_deleteTapeCopy2SegmentStatement = 0;
  m_TapeCopy2SegmentStatement = 0;
  m_insertCastorFile2TapeCopyStatement = 0;
  m_deleteCastorFile2TapeCopyStatement = 0;
  m_insertTapeCopy2TapeCopyStatusCodesStatement = 0;
  m_deleteTapeCopy2TapeCopyStatusCodesStatement = 0;
  m_TapeCopy2TapeCopyStatusCodesStatement = 0;
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
// fillRepStream
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillRepStream(castor::stager::TapeCopy* obj)
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
  u_signed64 streamId = (u_signed64)rset->getDouble(1);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(streamId, " ", 0);
  // Check whether old object should be deleted
  if (0 != streamId &&
      0 != obj->stream() &&
      obj->stream()->id() != streamId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    streamId = 0;
  }
  // Update object or create new one
  if (streamId == 0) {
    if (0 != obj->stream()) {
      cnvSvc()->createRep(&ad, obj->stream(), false);
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->stream(), false);
  }
}

//------------------------------------------------------------------------------
// fillRepSegment
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillRepSegment(castor::stager::TapeCopy* obj)
  throw (castor::exception::Exception) {
  // check select statement
  if (0 == m_TapeCopy2SegmentStatement) {
    m_TapeCopy2SegmentStatement = createStatement(s_TapeCopy2SegmentStatementString);
  }
  // Get current database data
  std::set<int> segmentsList;
  m_TapeCopy2SegmentStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_TapeCopy2SegmentStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    segmentsList.insert(rset->getInt(1));
  }
  m_TapeCopy2SegmentStatement->closeResultSet(rset);
  // update segments and create new ones
  for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
       it != obj->segments().end();
       it++) {
    std::set<int>::iterator item;
    if ((item = segmentsList.find((*it)->id())) == segmentsList.end()) {
      cnvSvc()->createRep(0, *it, false);
      if (0 == m_insertTapeCopy2SegmentStatement) {
        m_insertTapeCopy2SegmentStatement = createStatement(s_insertTapeCopy2SegmentStatementString);
      }
      m_insertTapeCopy2SegmentStatement->setDouble(1, obj->id());
      m_insertTapeCopy2SegmentStatement->setDouble(2, (*it)->id());
      m_insertTapeCopy2SegmentStatement->executeUpdate();
    } else {
      segmentsList.erase(item);
      cnvSvc()->updateRep(0, *it, false);
    }
  }
  // Delete old data
  for (std::set<int>::iterator it = segmentsList.begin();
       it != segmentsList.end();
       it++) {
    castor::db::DbAddress ad(*it, " ", 0);
    cnvSvc()->deleteRepByAddress(&ad, false);
    if (0 == m_deleteTapeCopy2SegmentStatement) {
      m_deleteTapeCopy2SegmentStatement = createStatement(s_deleteTapeCopy2SegmentStatementString);
    }
    m_deleteTapeCopy2SegmentStatement->setDouble(1, obj->id());
    m_deleteTapeCopy2SegmentStatement->setDouble(2, *it);
    m_deleteTapeCopy2SegmentStatement->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// fillRepCastorFile
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillRepCastorFile(castor::stager::TapeCopy* obj)
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
  u_signed64 castorFileId = (u_signed64)rset->getDouble(2);
  // Close resultset
  m_selectStatement->closeResultSet(rset);
  castor::db::DbAddress ad(castorFileId, " ", 0);
  // Check whether old object should be deleted
  if (0 != castorFileId &&
      0 != obj->castorFile() &&
      obj->castorFile()->id() != castorFileId) {
    cnvSvc()->deleteRepByAddress(&ad, false);
    castorFileId = 0;
    if (0 == m_deleteCastorFile2TapeCopyStatement) {
      m_deleteCastorFile2TapeCopyStatement = createStatement(s_deleteCastorFile2TapeCopyStatementString);
    }
    m_deleteCastorFile2TapeCopyStatement->setDouble(1, obj->castorFile()->id());
    m_deleteCastorFile2TapeCopyStatement->setDouble(2, obj->id());
    m_deleteCastorFile2TapeCopyStatement->executeUpdate();
  }
  // Update object or create new one
  if (castorFileId == 0) {
    if (0 != obj->castorFile()) {
      cnvSvc()->createRep(&ad, obj->castorFile(), false);
      if (0 == m_insertCastorFile2TapeCopyStatement) {
        m_insertCastorFile2TapeCopyStatement = createStatement(s_insertCastorFile2TapeCopyStatementString);
      }
      m_insertCastorFile2TapeCopyStatement->setDouble(1, obj->castorFile()->id());
      m_insertCastorFile2TapeCopyStatement->setDouble(2, obj->id());
      m_insertCastorFile2TapeCopyStatement->executeUpdate();
    }
  } else {
    cnvSvc()->updateRep(&ad, obj->castorFile(), false);
  }
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
  u_signed64 streamId = (u_signed64)rset->getDouble(0);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->stream() &&
      (0 == streamId ||
       obj->stream()->id() != streamId)) {
    delete obj->stream();
    obj->setStream(0);
  }
  // Update object or create new one
  if (0 != streamId) {
    if (0 == obj->stream()) {
      obj->setStream
        (dynamic_cast<castor::stager::Stream*>
         (cnvSvc()->getObjFromId(streamId)));
    } else if (obj->stream()->id() == streamId) {
      cnvSvc()->updateObj(obj->stream());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjSegment
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::fillObjSegment(castor::stager::TapeCopy* obj)
  throw (castor::exception::Exception) {
  // Check select statement
  if (0 == m_TapeCopy2SegmentStatement) {
    m_TapeCopy2SegmentStatement = createStatement(s_TapeCopy2SegmentStatementString);
  }
  // retrieve the object from the database
  std::set<int> segmentsList;
  m_TapeCopy2SegmentStatement->setDouble(1, obj->id());
  oracle::occi::ResultSet *rset = m_TapeCopy2SegmentStatement->executeQuery();
  while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
    segmentsList.insert(rset->getInt(1));
  }
  // Close ResultSet
  m_TapeCopy2SegmentStatement->closeResultSet(rset);
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
    delete (*it);
  }
  // Create new objects
  for (std::set<int>::iterator it = segmentsList.begin();
       it != segmentsList.end();
       it++) {
    IObject* item = cnvSvc()->getObjFromId(*it);
    obj->addSegments(dynamic_cast<castor::stager::Segment*>(item));
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
  u_signed64 castorFileId = (u_signed64)rset->getDouble(1);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->castorFile() &&
      (0 == castorFileId ||
       obj->castorFile()->id() != castorFileId)) {
    delete obj->castorFile();
    obj->setCastorFile(0);
  }
  // Update object or create new one
  if (0 != castorFileId) {
    if (0 == obj->castorFile()) {
      obj->setCastorFile
        (dynamic_cast<castor::stager::CastorFile*>
         (cnvSvc()->getObjFromId(castorFileId)));
    } else if (obj->castorFile()->id() == castorFileId) {
      cnvSvc()->updateObj(obj->castorFile());
    }
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::createRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::TapeCopy* obj = 
    dynamic_cast<castor::stager::TapeCopy*>(object);
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
    m_insertStatement->setDouble(1, obj->id());
    m_insertStatement->setDouble(2, obj->stream() ? obj->stream()->id() : 0);
    m_insertStatement->setDouble(3, obj->castorFile() ? obj->castorFile()->id() : 0);
    m_insertStatement->setDouble(4, (int)obj->status());
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
                    << "  id : " << obj->id() << std::endl
                    << "  stream : " << obj->stream() << std::endl
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
    m_updateStatement->setDouble(1, obj->stream() ? obj->stream()->id() : 0);
    m_updateStatement->setDouble(2, obj->castorFile() ? obj->castorFile()->id() : 0);
    m_updateStatement->setDouble(3, (int)obj->status());
    m_updateStatement->setDouble(4, obj->id());
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
    // Delete link to castorFile object
    if (0 != obj->castorFile()) {
      // Check whether the statement is ok
      if (0 == m_deleteCastorFile2TapeCopyStatement) {
        m_deleteCastorFile2TapeCopyStatement = createStatement(s_deleteCastorFile2TapeCopyStatementString);
      }
      // Delete links to objects
      m_deleteCastorFile2TapeCopyStatement->setDouble(1, obj->castorFile()->id());
      m_deleteCastorFile2TapeCopyStatement->setDouble(2, obj->id());
      m_deleteCastorFile2TapeCopyStatement->executeUpdate();
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
    object->setId((u_signed64)rset->getDouble(1));
    u_signed64 streamId = (u_signed64)rset->getDouble(2);
    u_signed64 castorFileId = (u_signed64)rset->getDouble(3);
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
    object->setId((u_signed64)rset->getDouble(1));
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

