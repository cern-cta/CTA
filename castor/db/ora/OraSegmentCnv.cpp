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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "OraSegmentCnv.hpp"
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
#include "castor/stager/Segment.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapeCopy.hpp"
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
"INSERT INTO Segment (blockid, fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum, errMsgTxt, errorCode, severity, id, tape, copy, status) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraSegmentCnv::s_deleteStatementString =
"DELETE FROM Segment WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraSegmentCnv::s_selectStatementString =
"SELECT blockid, fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum, errMsgTxt, errorCode, severity, id, tape, copy, status FROM Segment WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraSegmentCnv::s_updateStatementString =
"UPDATE Segment SET blockid = :1, fseq = :2, offset = :3, bytes_in = :4, bytes_out = :5, host_bytes = :6, segmCksumAlgorithm = :7, segmCksum = :8, errMsgTxt = :9, errorCode = :10, severity = :11, status = :12 WHERE id = :13";

/// SQL statement for type storage
const std::string castor::db::ora::OraSegmentCnv::s_storeTypeStatementString =
"INSERT INTO Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraSegmentCnv::s_deleteTypeStatementString =
"DELETE FROM Id2Type WHERE id = :1";

/// SQL existence statement for member tape
const std::string castor::db::ora::OraSegmentCnv::s_checkTapeExistStatementString =
"SELECT id from Tape WHERE id = :1";

/// SQL update statement for member tape
const std::string castor::db::ora::OraSegmentCnv::s_updateTapeStatementString =
"UPDATE Segment SET tape = : 1 WHERE id = :2";

/// SQL existence statement for member copy
const std::string castor::db::ora::OraSegmentCnv::s_checkTapeCopyExistStatementString =
"SELECT id from TapeCopy WHERE id = :1";

/// SQL update statement for member copy
const std::string castor::db::ora::OraSegmentCnv::s_updateTapeCopyStatementString =
"UPDATE Segment SET copy = : 1 WHERE id = :2";

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
  m_checkTapeExistStatement(0),
  m_updateTapeStatement(0),
  m_checkTapeCopyExistStatement(0),
  m_updateTapeCopyStatement(0) {}

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
    deleteStatement(m_checkTapeExistStatement);
    deleteStatement(m_updateTapeStatement);
    deleteStatement(m_checkTapeCopyExistStatement);
    deleteStatement(m_updateTapeCopyStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
  m_checkTapeExistStatement = 0;
  m_updateTapeStatement = 0;
  m_checkTapeCopyExistStatement = 0;
  m_updateTapeCopyStatement = 0;
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
// fillRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::fillRep(castor::IAddress* address,
                                             castor::IObject* object,
                                             unsigned int type,
                                             bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::Segment* obj = 
    dynamic_cast<castor::stager::Segment*>(object);
  try {
    switch (type) {
    case castor::OBJ_Tape :
      fillRepTape(obj);
      break;
    case castor::OBJ_TapeCopy :
      fillRepTapeCopy(obj);
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
// fillRepTape
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::fillRepTape(castor::stager::Segment* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
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
      cnvSvc()->createRep(&ad, obj->tape(), false);
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
// fillRepTapeCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::fillRepTapeCopy(castor::stager::Segment* obj)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  if (0 != obj->copy()) {
    // Check checkTapeCopyExist statement
    if (0 == m_checkTapeCopyExistStatement) {
      m_checkTapeCopyExistStatement = createStatement(s_checkTapeCopyExistStatementString);
    }
    // retrieve the object from the database
    m_checkTapeCopyExistStatement->setDouble(1, obj->copy()->id());
    oracle::occi::ResultSet *rset = m_checkTapeCopyExistStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
      cnvSvc()->createRep(&ad, obj->copy(), false);
    }
    // Close resultset
    m_checkTapeCopyExistStatement->closeResultSet(rset);
  }
  // Check update statement
  if (0 == m_updateTapeCopyStatement) {
    m_updateTapeCopyStatement = createStatement(s_updateTapeCopyStatementString);
  }
  // Update local object
  m_updateTapeCopyStatement->setDouble(1, 0 == obj->copy() ? 0 : obj->copy()->id());
  m_updateTapeCopyStatement->setDouble(2, obj->id());
  m_updateTapeCopyStatement->executeUpdate();
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::fillObj(castor::IAddress* address,
                                             castor::IObject* object,
                                             unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::Segment* obj = 
    dynamic_cast<castor::stager::Segment*>(object);
  switch (type) {
  case castor::OBJ_Tape :
    fillObjTape(obj);
    break;
  case castor::OBJ_TapeCopy :
    fillObjTapeCopy(obj);
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
// fillObjTape
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::fillObjTape(castor::stager::Segment* obj)
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
  u_signed64 tapeId = (u_signed64)rset->getDouble(13);
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
// fillObjTapeCopy
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::fillObjTapeCopy(castor::stager::Segment* obj)
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
  u_signed64 copyId = (u_signed64)rset->getDouble(14);
  // Close ResultSet
  m_selectStatement->closeResultSet(rset);
  // Check whether something should be deleted
  if (0 != obj->copy() &&
      (0 == copyId ||
       obj->copy()->id() != copyId)) {
    delete obj->copy();
    obj->setCopy(0);
  }
  // Update object or create new one
  if (0 != copyId) {
    if (0 == obj->copy()) {
      obj->setCopy
        (dynamic_cast<castor::stager::TapeCopy*>
         (cnvSvc()->getObjFromId(copyId)));
    } else if (obj->copy()->id() == copyId) {
      cnvSvc()->updateObj(obj->copy());
    }
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::createRep(castor::IAddress* address,
                                               castor::IObject* object,
                                               bool autocommit,
                                               unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::Segment* obj = 
    dynamic_cast<castor::stager::Segment*>(object);
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
    m_insertStatement->setDouble(13, (type == OBJ_Tape && obj->tape() != 0) ? obj->tape()->id() : 0);
    m_insertStatement->setDouble(14, (type == OBJ_TapeCopy && obj->copy() != 0) ? obj->copy()->id() : 0);
    m_insertStatement->setInt(15, (int)obj->status());
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
                    << "  status : " << obj->status() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraSegmentCnv::updateRep(castor::IAddress* address,
                                               castor::IObject* object,
                                               bool autocommit)
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
    // Update the current object
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
    m_updateStatement->setInt(12, (int)obj->status());
    m_updateStatement->setDouble(13, obj->id());
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
void castor::db::ora::OraSegmentCnv::deleteRep(castor::IAddress* address,
                                               castor::IObject* object,
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
castor::IObject* castor::db::ora::OraSegmentCnv::createObj(castor::IAddress* address)
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
    castor::stager::Segment* object = new castor::stager::Segment();
    // Now retrieve and set members
    object->setBlockid((unsigned char*)rset->getString(1).data());
    object->setFseq(rset->getInt(2));
    object->setOffset((u_signed64)rset->getDouble(3));
    object->setBytes_in((u_signed64)rset->getDouble(4));
    object->setBytes_out((u_signed64)rset->getDouble(5));
    object->setHost_bytes((u_signed64)rset->getDouble(6));
    object->setSegmCksumAlgorithm(rset->getString(7));
    object->setSegmCksum(rset->getInt(8));
    object->setErrMsgTxt(rset->getString(9));
    object->setErrorCode(rset->getInt(10));
    object->setSeverity(rset->getInt(11));
    object->setId((u_signed64)rset->getDouble(12));
    object->setStatus((enum castor::stager::SegmentStatusCodes)rset->getInt(15));
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
void castor::db::ora::OraSegmentCnv::updateObj(castor::IObject* obj)
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
    castor::stager::Segment* object = 
      dynamic_cast<castor::stager::Segment*>(obj);
    object->setBlockid((unsigned char*)rset->getString(1).data());
    object->setFseq(rset->getInt(2));
    object->setOffset((u_signed64)rset->getDouble(3));
    object->setBytes_in((u_signed64)rset->getDouble(4));
    object->setBytes_out((u_signed64)rset->getDouble(5));
    object->setHost_bytes((u_signed64)rset->getDouble(6));
    object->setSegmCksumAlgorithm(rset->getString(7));
    object->setSegmCksum(rset->getInt(8));
    object->setErrMsgTxt(rset->getString(9));
    object->setErrorCode(rset->getInt(10));
    object->setSeverity(rset->getInt(11));
    object->setId((u_signed64)rset->getDouble(12));
    object->setStatus((enum castor::stager::SegmentStatusCodes)rset->getInt(15));
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

