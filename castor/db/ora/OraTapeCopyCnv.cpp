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
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include <list>
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
"INSERT INTO rh_TapeCopy (id, castorFile, status) VALUES (:1,:2,:3)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteStatementString =
"DELETE FROM rh_TapeCopy WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraTapeCopyCnv::s_selectStatementString =
"SELECT id, castorFile, status FROM rh_TapeCopy WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraTapeCopyCnv::s_updateStatementString =
"UPDATE rh_TapeCopy SET castorFile = :1, status = :2 WHERE id = :3";

/// SQL statement for type storage
const std::string castor::db::ora::OraTapeCopyCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL select statement for member segments
const std::string castor::db::ora::OraTapeCopyCnv::s_TapeCopy2SegmentStatementString =
"SELECT Child from rh_TapeCopy2Segment WHERE Parent = :1";

/// SQL insert statement for member castorFile
const std::string castor::db::ora::OraTapeCopyCnv::s_insertCastorFile2TapeCopyStatementString =
"INSERT INTO rh_CastorFile2TapeCopy (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member castorFile
const std::string castor::db::ora::OraTapeCopyCnv::s_deleteCastorFile2TapeCopyStatementString =
"DELETE FROM rh_CastorFile2TapeCopy WHERE Parent = :1 AND Child = :2";

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
  m_TapeCopy2SegmentStatement(0),
  m_insertCastorFile2TapeCopyStatement(0),
  m_deleteCastorFile2TapeCopyStatement(0) {}

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
    deleteStatement(m_TapeCopy2SegmentStatement);
    deleteStatement(m_insertCastorFile2TapeCopyStatement);
    deleteStatement(m_deleteCastorFile2TapeCopyStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_TapeCopy2SegmentStatement = 0;
  m_insertCastorFile2TapeCopyStatement = 0;
  m_deleteCastorFile2TapeCopyStatement = 0;
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
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraTapeCopyCnv::createRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                castor::ObjectSet& alreadyDone,
                                                bool autocommit,
                                                bool recursive)
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
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Set ids of all objects
    int nids = obj->id() == 0 ? 1 : 0;
    // check which objects need to be saved/updated and keeps a list of them
    std::list<castor::IObject*> toBeSaved;
    std::list<castor::IObject*> toBeUpdated;
    if (recursive) {
      for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
           it != obj->segments().end();
           it++) {
        if (alreadyDone.find(*it) == alreadyDone.end()) {
          if (0 == (*it)->id()) {
            toBeSaved.push_back(*it);
            nids++;
          } else {
            toBeUpdated.push_back(*it);
          }
        }
      }
    }
    if (alreadyDone.find(obj->castorFile()) == alreadyDone.end() &&
        obj->castorFile() != 0) {
      if (0 == obj->castorFile()->id()) {
        if (!recursive) {
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "CreateNoRep called on type TapeCopy while its castorFile does not exist in the database.";
          throw ex;
        }
        toBeSaved.push_back(obj->castorFile());
        nids++;
      } else {
        if (recursive) {
          toBeUpdated.push_back(obj->castorFile());
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
    m_insertStatement->setDouble(1, obj->id());
    m_insertStatement->setDouble(2, obj->castorFile() ? obj->castorFile()->id() : 0);
    m_insertStatement->setDouble(3, (int)obj->status());
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
    // Deal with castorFile
    if (0 != obj->castorFile()) {
      if (0 == m_insertCastorFile2TapeCopyStatement) {
        m_insertCastorFile2TapeCopyStatement = createStatement(s_insertCastorFile2TapeCopyStatementString);
      }
      m_insertCastorFile2TapeCopyStatement->setDouble(1, obj->castorFile()->id());
      m_insertCastorFile2TapeCopyStatement->setDouble(2, obj->id());
      m_insertCastorFile2TapeCopyStatement->executeUpdate();
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
                                                castor::ObjectSet& alreadyDone,
                                                bool autocommit,
                                                bool recursive)
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
      // Dealing with castorFile
      {
        u_signed64 castorFileId = (unsigned long long)rset->getDouble(2);
        castor::db::DbAddress ad(castorFileId, " ", 0);
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
        if (castorFileId == 0) {
          if (0 != obj->castorFile()) {
            if (alreadyDone.find(obj->castorFile()) == alreadyDone.end()) {
              cnvSvc()->createRep(&ad, obj->castorFile(), alreadyDone, false, true);
              if (0 == m_insertCastorFile2TapeCopyStatement) {
                m_insertCastorFile2TapeCopyStatement = createStatement(s_insertCastorFile2TapeCopyStatementString);
              }
              m_insertCastorFile2TapeCopyStatement->setDouble(1, obj->castorFile()->id());
              m_insertCastorFile2TapeCopyStatement->setDouble(2, obj->id());
              m_insertCastorFile2TapeCopyStatement->executeUpdate();
            }
          }
        } else {
          if (alreadyDone.find(obj->castorFile()) == alreadyDone.end()) {
            cnvSvc()->updateRep(&ad, obj->castorFile(), alreadyDone, false, recursive);
          }
        }
      }
      m_selectStatement->closeResultSet(rset);
    }
    // Now Update the current object
    m_updateStatement->setDouble(1, obj->castorFile() ? obj->castorFile()->id() : 0);
    m_updateStatement->setDouble(2, (int)obj->status());
    m_updateStatement->setDouble(3, obj->id());
    m_updateStatement->executeUpdate();
    if (recursive) {
      // Dealing with segments
      {
        if (0 == m_TapeCopy2SegmentStatement) {
          m_TapeCopy2SegmentStatement = createStatement(s_TapeCopy2SegmentStatementString);
        }
        std::set<int> segmentsList;
        m_TapeCopy2SegmentStatement->setDouble(1, obj->id());
        oracle::occi::ResultSet *rset = m_TapeCopy2SegmentStatement->executeQuery();
        while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
          segmentsList.insert(rset->getInt(1));
        }
        m_TapeCopy2SegmentStatement->closeResultSet(rset);
        for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
             it != obj->segments().end();
             it++) {
          std::set<int>::iterator item;
          if ((item = segmentsList.find((*it)->id())) == segmentsList.end()) {
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->createRep(0, *it, alreadyDone, false, true);
            }
          } else {
            segmentsList.erase(item);
            if (alreadyDone.find(*it) == alreadyDone.end()) {
              cnvSvc()->updateRep(0, *it, alreadyDone, false, recursive);
            }
          }
        }
        for (std::set<int>::iterator it = segmentsList.begin();
             it != segmentsList.end();
             it++) {
          castor::db::DbAddress ad(*it, " ", 0);
          cnvSvc()->deleteRepByAddress(&ad, false);
        }
      }
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
void castor::db::ora::OraTapeCopyCnv::deleteRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                castor::ObjectSet& alreadyDone,
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
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Now Delete the object
    m_deleteTypeStatement->setDouble(1, obj->id());
    m_deleteTypeStatement->executeUpdate();
    m_deleteStatement->setDouble(1, obj->id());
    m_deleteStatement->executeUpdate();
    for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
         it != obj->segments().end();
         it++) {
      if (alreadyDone.find(*it) == alreadyDone.end()) {
        cnvSvc()->deleteRep(0, *it, alreadyDone, false);
      }
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
castor::IObject* castor::db::ora::OraTapeCopyCnv::createObj(castor::IAddress* address,
                                                            castor::ObjectCatalog& newlyCreated,
                                                            bool recursive)
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
    castor::stager::TapeCopy* object = new castor::stager::TapeCopy();
    // Now retrieve and set members
    object->setId((unsigned long long)rset->getDouble(1));
    newlyCreated[object->id()] = object;
    object->setStatus((enum castor::stager::TapeCopyStatusCodes)rset->getInt(2));
    if (recursive) {
      u_signed64 castorFileId = (unsigned long long)rset->getDouble(3);
      IObject* objCastorFile = cnvSvc()->getObjFromId(castorFileId, newlyCreated);
      object->setCastorFile(dynamic_cast<castor::stager::CastorFile*>(objCastorFile));
    }
    m_selectStatement->closeResultSet(rset);
    if (recursive) {
      // Get ids of objs to retrieve
      if (0 == m_TapeCopy2SegmentStatement) {
        m_TapeCopy2SegmentStatement = createStatement(s_TapeCopy2SegmentStatementString);
      }
      m_TapeCopy2SegmentStatement->setDouble(1, ad->id());
      rset = m_TapeCopy2SegmentStatement->executeQuery();
      while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
        IObject* obj = cnvSvc()->getObjFromId(rset->getInt(1), newlyCreated);
        object->addSegments(dynamic_cast<castor::stager::Segment*>(obj));
      }
      m_TapeCopy2SegmentStatement->closeResultSet(rset);
    }
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
void castor::db::ora::OraTapeCopyCnv::updateObj(castor::IObject* obj,
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
    castor::stager::TapeCopy* object = 
      dynamic_cast<castor::stager::TapeCopy*>(obj);
    object->setId((unsigned long long)rset->getDouble(1));
    alreadyDone[obj->id()] = obj;
    // Dealing with castorFile
    u_signed64 castorFileId = (unsigned long long)rset->getDouble(2);
    if (0 != object->castorFile() &&
        (0 == castorFileId ||
         object->castorFile()->id() != castorFileId)) {
      delete object->castorFile();
      object->setCastorFile(0);
    }
    if (0 != castorFileId) {
      if (0 == object->castorFile()) {
        object->setCastorFile
          (dynamic_cast<castor::stager::CastorFile*>
           (cnvSvc()->getObjFromId(castorFileId, alreadyDone)));
      } else if (object->castorFile()->id() == castorFileId) {
        if (alreadyDone.find(object->castorFile()->id()) == alreadyDone.end()) {
          cnvSvc()->updateObj(object->castorFile(), alreadyDone);
        }
      }
    }
    object->setStatus((enum castor::stager::TapeCopyStatusCodes)rset->getInt(3));
    m_selectStatement->closeResultSet(rset);
    // Deal with segments
    if (0 == m_TapeCopy2SegmentStatement) {
      m_TapeCopy2SegmentStatement = createStatement(s_TapeCopy2SegmentStatementString);
    }
    std::set<int> segmentsList;
    m_TapeCopy2SegmentStatement->setDouble(1, obj->id());
    rset = m_TapeCopy2SegmentStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      segmentsList.insert(rset->getInt(1));
    }
    m_TapeCopy2SegmentStatement->closeResultSet(rset);
    {
      std::vector<castor::stager::Segment*> toBeDeleted;
      for (std::vector<castor::stager::Segment*>::iterator it = object->segments().begin();
           it != object->segments().end();
           it++) {
        std::set<int>::iterator item;
        if ((item = segmentsList.find((*it)->id())) == segmentsList.end()) {
          toBeDeleted.push_back(*it);
        } else {
          segmentsList.erase(item);
          cnvSvc()->updateObj((*it), alreadyDone);
        }
      }
      for (std::vector<castor::stager::Segment*>::iterator it = toBeDeleted.begin();
           it != toBeDeleted.end();
           it++) {
        object->removeSegments(*it);
        delete (*it);
      }
    }
    for (std::set<int>::iterator it = segmentsList.begin();
         it != segmentsList.end();
         it++) {
      IObject* item = cnvSvc()->getObjFromId(*it, alreadyDone);
      object->addSegments(dynamic_cast<castor::stager::Segment*>(item));
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

