/******************************************************************************
 *                      castor/db/ora/OraSegmentCnv.cpp
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
#include "Cuuid.h"
#include "OraSegmentCnv.hpp"
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
#include "castor/stager/Cuuid.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapeCopy.hpp"
#include <list>
#include <string>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraSegmentCnv> s_factoryOraSegmentCnv;
const castor::IFactory<castor::IConverter>& OraSegmentCnvFactory = 
  s_factoryOraSegmentCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraSegmentCnv::s_insertStatementString =
"INSERT INTO rh_Segment (blockid, fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum, errMsgTxt, errorCode, severity, id, tape, copy, stgReqId, status) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraSegmentCnv::s_deleteStatementString =
"DELETE FROM rh_Segment WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraSegmentCnv::s_selectStatementString =
"SELECT blockid, fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum, errMsgTxt, errorCode, severity, id, tape, copy, stgReqId, status FROM rh_Segment WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraSegmentCnv::s_updateStatementString =
"UPDATE rh_Segment SET blockid = :1, fseq = :2, offset = :3, bytes_in = :4, bytes_out = :5, host_bytes = :6, segmCksumAlgorithm = :7, segmCksum = :8, errMsgTxt = :9, errorCode = :10, severity = :11, tape = :12, copy = :13, stgReqId = :14, status = :15 WHERE id = :16";

/// SQL statement for type storage
const std::string castor::db::ora::OraSegmentCnv::s_storeTypeStatementString =
"INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraSegmentCnv::s_deleteTypeStatementString =
"DELETE FROM rh_Id2Type WHERE id = :1";

/// SQL insert statement for member tape
const std::string castor::db::ora::OraSegmentCnv::s_insertTape2SegmentStatementString =
"INSERT INTO rh_Tape2Segment (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member tape
const std::string castor::db::ora::OraSegmentCnv::s_deleteTape2SegmentStatementString =
"DELETE FROM rh_Tape2Segment WHERE Parent = :1 AND Child = :2";

/// SQL insert statement for member copy
const std::string castor::db::ora::OraSegmentCnv::s_insertTapeCopy2SegmentStatementString =
"INSERT INTO rh_TapeCopy2Segment (Parent, Child) VALUES (:1, :2)";

/// SQL delete statement for member copy
const std::string castor::db::ora::OraSegmentCnv::s_deleteTapeCopy2SegmentStatementString =
"DELETE FROM rh_TapeCopy2Segment WHERE Parent = :1 AND Child = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraSegmentCnv::OraSegmentCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0),
  m_insertTape2SegmentStatement(0),
  m_deleteTape2SegmentStatement(0),
  m_insertTapeCopy2SegmentStatement(0),
  m_deleteTapeCopy2SegmentStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraSegmentCnv::~OraSegmentCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
    deleteStatement(m_insertTape2SegmentStatement);
    deleteStatement(m_deleteTape2SegmentStatement);
    deleteStatement(m_insertTapeCopy2SegmentStatement);
    deleteStatement(m_deleteTapeCopy2SegmentStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_insertTape2SegmentStatement = 0;
  m_deleteTape2SegmentStatement = 0;
  m_insertTapeCopy2SegmentStatement = 0;
  m_deleteTapeCopy2SegmentStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraSegmentCnv::ObjType() {
  return castor::stager::Segment::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraSegmentCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::createRep(castor::IAddress* address,
                                               castor::IObject* object,
                                               castor::ObjectSet& alreadyDone,
                                               bool autocommit,
                                               bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::Segment* obj = 
    dynamic_cast<castor::stager::Segment*>(object);
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
    if (alreadyDone.find(obj->tape()) == alreadyDone.end() &&
        obj->tape() != 0) {
      if (0 == obj->tape()->id()) {
        if (!recursive) {
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "CreateNoRep called on type Segment while its tape does not exist in the database.";
          throw ex;
        }
        toBeSaved.push_back(obj->tape());
        nids++;
      } else {
        if (recursive) {
          toBeUpdated.push_back(obj->tape());
        }
      }
    }
    if (alreadyDone.find(obj->copy()) == alreadyDone.end() &&
        obj->copy() != 0) {
      if (0 == obj->copy()->id()) {
        if (!recursive) {
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "CreateNoRep called on type Segment while its copy does not exist in the database.";
          throw ex;
        }
        toBeSaved.push_back(obj->copy());
        nids++;
      } else {
        if (recursive) {
          toBeUpdated.push_back(obj->copy());
        }
      }
    }
    if (recursive) {
      if (alreadyDone.find(obj->stgReqId()) == alreadyDone.end() &&
          obj->stgReqId() != 0) {
        if (0 == obj->stgReqId()->id()) {
          toBeSaved.push_back(obj->stgReqId());
          nids++;
        } else {
          toBeUpdated.push_back(obj->stgReqId());
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
    std::string blockidS((const char*)obj->blockid(), 4);
    m_insertStatement->setString(1, blockidS);
    m_insertStatement->setInt(2, obj->fseq());
    m_insertStatement->setDouble(3, obj->offset());
    m_insertStatement->setDouble(4, obj->bytes_in());
    m_insertStatement->setDouble(5, obj->bytes_out());
    m_insertStatement->setDouble(6, obj->host_bytes());
    m_insertStatement->setString(7, obj->segmCksumAlgorithm());
    m_insertStatement->setInt(8, obj->segmCksum());
    m_insertStatement->setString(9, obj->errMsgTxt());
    m_insertStatement->setInt(10, obj->errorCode());
    m_insertStatement->setInt(11, obj->severity());
    m_insertStatement->setDouble(12, obj->id());
    m_insertStatement->setDouble(13, obj->tape() ? obj->tape()->id() : 0);
    m_insertStatement->setDouble(14, obj->copy() ? obj->copy()->id() : 0);
    m_insertStatement->setDouble(15, obj->stgReqId() ? obj->stgReqId()->id() : 0);
    m_insertStatement->setDouble(16, (int)obj->status());
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
    // Deal with tape
    if (0 != obj->tape()) {
      if (0 == m_insertTape2SegmentStatement) {
        m_insertTape2SegmentStatement = createStatement(s_insertTape2SegmentStatementString);
      }
      m_insertTape2SegmentStatement->setDouble(1, obj->tape()->id());
      m_insertTape2SegmentStatement->setDouble(2, obj->id());
      m_insertTape2SegmentStatement->executeUpdate();
    }
    // Deal with copy
    if (0 != obj->copy()) {
      if (0 == m_insertTapeCopy2SegmentStatement) {
        m_insertTapeCopy2SegmentStatement = createStatement(s_insertTapeCopy2SegmentStatementString);
      }
      m_insertTapeCopy2SegmentStatement->setDouble(1, obj->copy()->id());
      m_insertTapeCopy2SegmentStatement->setDouble(2, obj->id());
      m_insertTapeCopy2SegmentStatement->executeUpdate();
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
                    << "  blockid : " << obj->blockid() << std::endl
                    << "  fseq : " << obj->fseq() << std::endl
                    << "  offset : " << obj->offset() << std::endl
                    << "  bytes_in : " << obj->bytes_in() << std::endl
                    << "  bytes_out : " << obj->bytes_out() << std::endl
                    << "  host_bytes : " << obj->host_bytes() << std::endl
                    << "  segmCksumAlgorithm : " << obj->segmCksumAlgorithm() << std::endl
                    << "  segmCksum : " << obj->segmCksum() << std::endl
                    << "  errMsgTxt : " << obj->errMsgTxt() << std::endl
                    << "  errorCode : " << obj->errorCode() << std::endl
                    << "  severity : " << obj->severity() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  tape : " << obj->tape() << std::endl
                    << "  copy : " << obj->copy() << std::endl
                    << "  stgReqId : " << obj->stgReqId() << std::endl
                    << "  status : " << obj->status() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::updateRep(castor::IAddress* address,
                                               castor::IObject* object,
                                               castor::ObjectSet& alreadyDone,
                                               bool autocommit,
                                               bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::Segment* obj = 
    dynamic_cast<castor::stager::Segment*>(object);
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
      // Dealing with tape
      {
        u_signed64 tapeId = (unsigned long long)rset->getDouble(13);
        castor::db::DbAddress ad(tapeId, " ", 0);
        if (0 != tapeId &&
            0 != obj->tape() &&
            obj->tape()->id() != tapeId) {
          cnvSvc()->deleteRepByAddress(&ad, false);
          tapeId = 0;
          if (0 == m_deleteTape2SegmentStatement) {
            m_deleteTape2SegmentStatement = createStatement(s_deleteTape2SegmentStatementString);
          }
          m_deleteTape2SegmentStatement->setDouble(1, obj->tape()->id());
          m_deleteTape2SegmentStatement->setDouble(2, obj->id());
          m_deleteTape2SegmentStatement->executeUpdate();
        }
        if (tapeId == 0) {
          if (0 != obj->tape()) {
            if (alreadyDone.find(obj->tape()) == alreadyDone.end()) {
              cnvSvc()->createRep(&ad, obj->tape(), alreadyDone, false, true);
              if (0 == m_insertTape2SegmentStatement) {
                m_insertTape2SegmentStatement = createStatement(s_insertTape2SegmentStatementString);
              }
              m_insertTape2SegmentStatement->setDouble(1, obj->tape()->id());
              m_insertTape2SegmentStatement->setDouble(2, obj->id());
              m_insertTape2SegmentStatement->executeUpdate();
            }
          }
        } else {
          if (alreadyDone.find(obj->tape()) == alreadyDone.end()) {
            cnvSvc()->updateRep(&ad, obj->tape(), alreadyDone, false, recursive);
          }
        }
      }
      // Dealing with copy
      {
        u_signed64 copyId = (unsigned long long)rset->getDouble(14);
        castor::db::DbAddress ad(copyId, " ", 0);
        if (0 != copyId &&
            0 != obj->copy() &&
            obj->copy()->id() != copyId) {
          cnvSvc()->deleteRepByAddress(&ad, false);
          copyId = 0;
          if (0 == m_deleteTapeCopy2SegmentStatement) {
            m_deleteTapeCopy2SegmentStatement = createStatement(s_deleteTapeCopy2SegmentStatementString);
          }
          m_deleteTapeCopy2SegmentStatement->setDouble(1, obj->copy()->id());
          m_deleteTapeCopy2SegmentStatement->setDouble(2, obj->id());
          m_deleteTapeCopy2SegmentStatement->executeUpdate();
        }
        if (copyId == 0) {
          if (0 != obj->copy()) {
            if (alreadyDone.find(obj->copy()) == alreadyDone.end()) {
              cnvSvc()->createRep(&ad, obj->copy(), alreadyDone, false, true);
              if (0 == m_insertTapeCopy2SegmentStatement) {
                m_insertTapeCopy2SegmentStatement = createStatement(s_insertTapeCopy2SegmentStatementString);
              }
              m_insertTapeCopy2SegmentStatement->setDouble(1, obj->copy()->id());
              m_insertTapeCopy2SegmentStatement->setDouble(2, obj->id());
              m_insertTapeCopy2SegmentStatement->executeUpdate();
            }
          }
        } else {
          if (alreadyDone.find(obj->copy()) == alreadyDone.end()) {
            cnvSvc()->updateRep(&ad, obj->copy(), alreadyDone, false, recursive);
          }
        }
      }
      // Dealing with stgReqId
      {
        u_signed64 stgReqIdId = (unsigned long long)rset->getDouble(15);
        castor::db::DbAddress ad(stgReqIdId, " ", 0);
        if (0 != stgReqIdId &&
            0 != obj->stgReqId() &&
            obj->stgReqId()->id() != stgReqIdId) {
          cnvSvc()->deleteRepByAddress(&ad, false);
          stgReqIdId = 0;
        }
        if (stgReqIdId == 0) {
          if (0 != obj->stgReqId()) {
            if (alreadyDone.find(obj->stgReqId()) == alreadyDone.end()) {
              cnvSvc()->createRep(&ad, obj->stgReqId(), alreadyDone, false, true);
            }
          }
        } else {
          if (alreadyDone.find(obj->stgReqId()) == alreadyDone.end()) {
            cnvSvc()->updateRep(&ad, obj->stgReqId(), alreadyDone, false, recursive);
          }
        }
      }
      m_selectStatement->closeResultSet(rset);
    }
    // Now Update the current object
    std::string blockidS((const char*)obj->blockid(), 4);
    m_updateStatement->setString(1, blockidS);
    m_updateStatement->setInt(2, obj->fseq());
    m_updateStatement->setDouble(3, obj->offset());
    m_updateStatement->setDouble(4, obj->bytes_in());
    m_updateStatement->setDouble(5, obj->bytes_out());
    m_updateStatement->setDouble(6, obj->host_bytes());
    m_updateStatement->setString(7, obj->segmCksumAlgorithm());
    m_updateStatement->setInt(8, obj->segmCksum());
    m_updateStatement->setString(9, obj->errMsgTxt());
    m_updateStatement->setInt(10, obj->errorCode());
    m_updateStatement->setInt(11, obj->severity());
    m_updateStatement->setDouble(12, obj->tape() ? obj->tape()->id() : 0);
    m_updateStatement->setDouble(13, obj->copy() ? obj->copy()->id() : 0);
    m_updateStatement->setDouble(14, obj->stgReqId() ? obj->stgReqId()->id() : 0);
    m_updateStatement->setDouble(15, (int)obj->status());
    m_updateStatement->setDouble(16, obj->id());
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
void castor::db::ora::OraSegmentCnv::deleteRep(castor::IAddress* address,
                                               castor::IObject* object,
                                               castor::ObjectSet& alreadyDone,
                                               bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::Segment* obj = 
    dynamic_cast<castor::stager::Segment*>(object);
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
    if (alreadyDone.find(obj->stgReqId()) == alreadyDone.end() &&
        obj->stgReqId() != 0) {
      cnvSvc()->deleteRep(0, obj->stgReqId(), alreadyDone, false);
    }
    // Delete link to tape object
    if (0 != obj->tape()) {
      // Check whether the statement is ok
      if (0 == m_deleteTape2SegmentStatement) {
        m_deleteTape2SegmentStatement = createStatement(s_deleteTape2SegmentStatementString);
      }
      // Delete links to objects
      m_deleteTape2SegmentStatement->setDouble(1, obj->tape()->id());
      m_deleteTape2SegmentStatement->setDouble(2, obj->id());
      m_deleteTape2SegmentStatement->executeUpdate();
    }
    // Delete link to copy object
    if (0 != obj->copy()) {
      // Check whether the statement is ok
      if (0 == m_deleteTapeCopy2SegmentStatement) {
        m_deleteTapeCopy2SegmentStatement = createStatement(s_deleteTapeCopy2SegmentStatementString);
      }
      // Delete links to objects
      m_deleteTapeCopy2SegmentStatement->setDouble(1, obj->copy()->id());
      m_deleteTapeCopy2SegmentStatement->setDouble(2, obj->id());
      m_deleteTapeCopy2SegmentStatement->executeUpdate();
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
castor::IObject* castor::db::ora::OraSegmentCnv::createObj(castor::IAddress* address,
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
    castor::stager::Segment* object = new castor::stager::Segment();
    // Now retrieve and set members
    object->setBlockid((unsigned char*)rset->getString(1).data());
    object->setFseq(rset->getInt(2));
    object->setOffset((unsigned long long)rset->getDouble(3));
    object->setBytes_in((unsigned long long)rset->getDouble(4));
    object->setBytes_out((unsigned long long)rset->getDouble(5));
    object->setHost_bytes((unsigned long long)rset->getDouble(6));
    object->setSegmCksumAlgorithm(rset->getString(7));
    object->setSegmCksum(rset->getInt(8));
    object->setErrMsgTxt(rset->getString(9));
    object->setErrorCode(rset->getInt(10));
    object->setSeverity(rset->getInt(11));
    object->setId((unsigned long long)rset->getDouble(12));
    newlyCreated[object->id()] = object;
    u_signed64 tapeId = (unsigned long long)rset->getDouble(13);
    IObject* objTape = cnvSvc()->getObjFromId(tapeId, newlyCreated);
    object->setTape(dynamic_cast<castor::stager::Tape*>(objTape));
    u_signed64 copyId = (unsigned long long)rset->getDouble(14);
    IObject* objCopy = cnvSvc()->getObjFromId(copyId, newlyCreated);
    object->setCopy(dynamic_cast<castor::stager::TapeCopy*>(objCopy));
    u_signed64 stgReqIdId = (unsigned long long)rset->getDouble(15);
    IObject* objStgReqId = cnvSvc()->getObjFromId(stgReqIdId, newlyCreated);
    object->setStgReqId(dynamic_cast<castor::stager::Cuuid*>(objStgReqId));
    object->setStatus((enum castor::stager::SegmentStatusCodes)rset->getInt(16));
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
void castor::db::ora::OraSegmentCnv::updateObj(castor::IObject* obj,
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
    castor::stager::Segment* object = 
      dynamic_cast<castor::stager::Segment*>(obj);
    object->setBlockid((unsigned char*)rset->getString(1).data());
    object->setFseq(rset->getInt(2));
    object->setOffset((unsigned long long)rset->getDouble(3));
    object->setBytes_in((unsigned long long)rset->getDouble(4));
    object->setBytes_out((unsigned long long)rset->getDouble(5));
    object->setHost_bytes((unsigned long long)rset->getDouble(6));
    object->setSegmCksumAlgorithm(rset->getString(7));
    object->setSegmCksum(rset->getInt(8));
    object->setErrMsgTxt(rset->getString(9));
    object->setErrorCode(rset->getInt(10));
    object->setSeverity(rset->getInt(11));
    object->setId((unsigned long long)rset->getDouble(12));
    alreadyDone[obj->id()] = obj;
    // Dealing with tape
    u_signed64 tapeId = (unsigned long long)rset->getDouble(13);
    if (0 != object->tape() &&
        (0 == tapeId ||
         object->tape()->id() != tapeId)) {
      delete object->tape();
      object->setTape(0);
    }
    if (0 != tapeId) {
      if (0 == object->tape()) {
        object->setTape
          (dynamic_cast<castor::stager::Tape*>
           (cnvSvc()->getObjFromId(tapeId, alreadyDone)));
      } else if (object->tape()->id() == tapeId) {
        if (alreadyDone.find(object->tape()->id()) == alreadyDone.end()) {
          cnvSvc()->updateObj(object->tape(), alreadyDone);
        }
      }
    }
    // Dealing with copy
    u_signed64 copyId = (unsigned long long)rset->getDouble(14);
    if (0 != object->copy() &&
        (0 == copyId ||
         object->copy()->id() != copyId)) {
      delete object->copy();
      object->setCopy(0);
    }
    if (0 != copyId) {
      if (0 == object->copy()) {
        object->setCopy
          (dynamic_cast<castor::stager::TapeCopy*>
           (cnvSvc()->getObjFromId(copyId, alreadyDone)));
      } else if (object->copy()->id() == copyId) {
        if (alreadyDone.find(object->copy()->id()) == alreadyDone.end()) {
          cnvSvc()->updateObj(object->copy(), alreadyDone);
        }
      }
    }
    // Dealing with stgReqId
    u_signed64 stgReqIdId = (unsigned long long)rset->getDouble(15);
    if (0 != object->stgReqId() &&
        (0 == stgReqIdId ||
         object->stgReqId()->id() != stgReqIdId)) {
      delete object->stgReqId();
      object->setStgReqId(0);
    }
    if (0 != stgReqIdId) {
      if (0 == object->stgReqId()) {
        object->setStgReqId
          (dynamic_cast<castor::stager::Cuuid*>
           (cnvSvc()->getObjFromId(stgReqIdId, alreadyDone)));
      } else if (object->stgReqId()->id() == stgReqIdId) {
        if (alreadyDone.find(object->stgReqId()->id()) == alreadyDone.end()) {
          cnvSvc()->updateObj(object->stgReqId(), alreadyDone);
        }
      }
    }
    object->setStatus((enum castor::stager::SegmentStatusCodes)rset->getInt(16));
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

