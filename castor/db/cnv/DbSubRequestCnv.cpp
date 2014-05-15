/******************************************************************************
 *                      castor/db/cnv/DbSubRequestCnv.cpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "DbSubRequestCnv.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/IObject.hpp"
#include "castor/VectorAddress.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/OutOfMemory.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include <stdlib.h>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class - should never be used
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::cnv::DbSubRequestCnv>* s_factoryDbSubRequestCnv =
  new castor::CnvFactory<castor::db::cnv::DbSubRequestCnv>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::cnv::DbSubRequestCnv::s_insertStatementString =
"INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, reqType, id, diskcopy, castorFile, status, request, getNextStatus) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,ids_seq.nextval,:17,:18,:19,:20,:21) RETURNING id INTO :23";

/// SQL statement for request bulk insertion
const std::string castor::db::cnv::DbSubRequestCnv::s_bulkInsertStatementString =
"INSERT /* bulk */ INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, reqType, id, diskcopy, castorFile, status, request, getNextStatus) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,ids_seq.nextval,:17,:18,:19,:20,:21,:22) RETURNING id INTO :23";

/// SQL statement for request deletion
const std::string castor::db::cnv::DbSubRequestCnv::s_deleteStatementString =
"DELETE FROM SubRequest WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::cnv::DbSubRequestCnv::s_selectStatementString =
"SELECT retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, reqType, id, diskcopy, castorFile, status, request, getNextStatus FROM SubRequest WHERE id = :1";

/// SQL statement for bulk request selection
const std::string castor::db::cnv::DbSubRequestCnv::s_bulkSelectStatementString =
"DECLARE \
   TYPE RecordType IS RECORD (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048), xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER, modeBits NUMBER, creationTime INTEGER, lastModificationTime INTEGER, answered NUMBER, errorCode NUMBER, errorMessage VARCHAR2(2048), requestedFileSystems VARCHAR2(2048), svcHandler VARCHAR2(2048), reqType NUMBER, id INTEGER, diskcopy INTEGER, castorFile INTEGER, status INTEGER, request INTEGER, getNextStatus INTEGER); \
   TYPE CurType IS REF CURSOR RETURN RecordType; \
   PROCEDURE bulkSelect(ids IN castor.\"cnumList\", \
                        objs OUT CurType) AS \
   BEGIN \
     FORALL i IN ids.FIRST..ids.LAST \
       INSERT INTO bulkSelectHelper VALUES(ids(i)); \
     OPEN objs FOR SELECT retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, reqType, id, diskcopy, castorFile, status, request, getNextStatus \
                     FROM SubRequest t, bulkSelectHelper h \
                    WHERE t.id = h.objId; \
     DELETE FROM bulkSelectHelper; \
   END; \
 BEGIN \
   bulkSelect(:1, :2); \
 END;";

/// SQL statement for request update
const std::string castor::db::cnv::DbSubRequestCnv::s_updateStatementString =
"UPDATE SubRequest SET retryCounter = :1, fileName = :2, protocol = :3, xsize = :4, priority = :5, subreqId = :6, flags = :7, modeBits = :8, lastModificationTime = :9, answered = :10, errorCode = :11, errorMessage = :12, requestedFileSystems = :13, svcHandler = :14, reqType = :15, status = :16, getNextStatus = :17 WHERE id = :18";

/// SQL existence statement for member castorFile
const std::string castor::db::cnv::DbSubRequestCnv::s_checkCastorFileExistStatementString =
"SELECT id FROM CastorFile WHERE id = :1";

/// SQL update statement for member castorFile
const std::string castor::db::cnv::DbSubRequestCnv::s_updateCastorFileStatementString =
"UPDATE SubRequest SET castorFile = :1 WHERE id = :2";

/// SQL update statement for member request
const std::string castor::db::cnv::DbSubRequestCnv::s_updateFileRequestStatementString =
"UPDATE SubRequest SET request = :1 WHERE id = :2";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::cnv::DbSubRequestCnv::DbSubRequestCnv(castor::ICnvSvc* cnvSvc) :
  DbBaseCnv(cnvSvc),
  m_insertStatement(0),
  m_bulkInsertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_bulkSelectStatement(0),
  m_updateStatement(0),
  m_checkCastorFileExistStatement(0),
  m_updateCastorFileStatement(0),
  m_updateFileRequestStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::cnv::DbSubRequestCnv::~DbSubRequestCnv() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    if(m_insertStatement) delete m_insertStatement;
    if(m_bulkInsertStatement) delete m_bulkInsertStatement;
    if(m_deleteStatement) delete m_deleteStatement;
    if(m_selectStatement) delete m_selectStatement;
    if(m_bulkSelectStatement) delete m_bulkSelectStatement;
    if(m_updateStatement) delete m_updateStatement;
    if(m_checkCastorFileExistStatement) delete m_checkCastorFileExistStatement;
    if(m_updateCastorFileStatement) delete m_updateCastorFileStatement;
    if(m_updateFileRequestStatement) delete m_updateFileRequestStatement;
  } catch (castor::exception::Exception& ignored) {};
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
unsigned int castor::db::cnv::DbSubRequestCnv::ObjType() {
  return castor::stager::SubRequest::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
unsigned int castor::db::cnv::DbSubRequestCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::fillRep(castor::IAddress*,
                                               castor::IObject* object,
                                               unsigned int type,
                                               bool endTransaction)
   {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
  try {
    switch (type) {
    case castor::OBJ_CastorFile :
      fillRepCastorFile(obj);
      break;
    case castor::OBJ_FileRequest :
      fillRepFileRequest(obj);
      break;
    default :
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "fillRep called for type " << type 
                      << " on object of type " << obj->type() 
                      << ". This is meaningless.";
      throw ex;
    }
    if (endTransaction) {
      cnvSvc()->commit();
    }
  } catch (castor::exception::SQLError& e) {
    castor::exception::Exception ex;
    ex.getMessage() << "Error in fillRep for type " << type
                    << std::endl << e.getMessage().str() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// fillRepCastorFile
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::fillRepCastorFile(castor::stager::SubRequest* obj)
   {
  if (0 != obj->castorFile()) {
    // Check checkCastorFileExist statement
    if (0 == m_checkCastorFileExistStatement) {
      m_checkCastorFileExistStatement = createStatement(s_checkCastorFileExistStatementString);
    }
    // retrieve the object from the database
    m_checkCastorFileExistStatement->setUInt64(1, obj->castorFile()->id());
    castor::db::IDbResultSet *rset = m_checkCastorFileExistStatement->executeQuery();
    if (!rset->next()) {
      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      cnvSvc()->createRep(&ad, obj->castorFile(), false);
    }
    // Close resultset
    delete rset;
  }
  // Check update statement
  if (0 == m_updateCastorFileStatement) {
    m_updateCastorFileStatement = createStatement(s_updateCastorFileStatementString);
  }
  // Update local object
  m_updateCastorFileStatement->setUInt64(1, 0 == obj->castorFile() ? 0 : obj->castorFile()->id());
  m_updateCastorFileStatement->setUInt64(2, obj->id());
  m_updateCastorFileStatement->execute();
}

//------------------------------------------------------------------------------
// fillRepFileRequest
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::fillRepFileRequest(castor::stager::SubRequest* obj)
   {
  // Check update statement
  if (0 == m_updateFileRequestStatement) {
    m_updateFileRequestStatement = createStatement(s_updateFileRequestStatementString);
  }
  // Update local object
  m_updateFileRequestStatement->setUInt64(1, 0 == obj->request() ? 0 : obj->request()->id());
  m_updateFileRequestStatement->setUInt64(2, obj->id());
  m_updateFileRequestStatement->execute();
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::fillObj(castor::IAddress*,
                                               castor::IObject* object,
                                               unsigned int type,
                                               bool endTransaction)
   {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
  switch (type) {
  case castor::OBJ_CastorFile :
    fillObjCastorFile(obj);
    break;
  case castor::OBJ_FileRequest :
    fillObjFileRequest(obj);
    break;
  default :
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "fillObj called on type " << type 
                    << " on object of type " << obj->type() 
                    << ". This is meaningless.";
    throw ex;
  }
  if (endTransaction) {
    cnvSvc()->commit();
  }
}

//------------------------------------------------------------------------------
// fillObjCastorFile
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::fillObjCastorFile(castor::stager::SubRequest* obj)
   {
  // Check whether the statement is ok
  if (0 == m_selectStatement) {
    m_selectStatement = createStatement(s_selectStatementString);
  }
  // retrieve the object from the database
  m_selectStatement->setUInt64(1, obj->id());
  castor::db::IDbResultSet *rset = m_selectStatement->executeQuery();
  if (!rset->next()) {
    castor::exception::NoEntry ex;
    ex.getMessage() << "No object found for id :" << obj->id();
    throw ex;
  }
  u_signed64 castorFileId = rset->getInt64(19);
  // Close ResultSet
  delete rset;
  // Check whether something should be deleted
  if (0 != obj->castorFile() &&
      (0 == castorFileId ||
       obj->castorFile()->id() != castorFileId)) {
    obj->setCastorFile(0);
  }
  // Update object or create new one
  if (0 != castorFileId) {
    if (0 == obj->castorFile()) {
      obj->setCastorFile
        (dynamic_cast<castor::stager::CastorFile*>
         (cnvSvc()->getObjFromId(castorFileId, OBJ_CastorFile)));
    } else {
      cnvSvc()->updateObj(obj->castorFile());
    }
  }
}

//------------------------------------------------------------------------------
// fillObjFileRequest
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::fillObjFileRequest(castor::stager::SubRequest* obj)
   {
  // Check whether the statement is ok
  if (0 == m_selectStatement) {
    m_selectStatement = createStatement(s_selectStatementString);
  }
  // retrieve the object from the database
  m_selectStatement->setUInt64(1, obj->id());
  castor::db::IDbResultSet *rset = m_selectStatement->executeQuery();
  if (!rset->next()) {
    castor::exception::NoEntry ex;
    ex.getMessage() << "No object found for id :" << obj->id();
    throw ex;
  }
  u_signed64 requestId = rset->getInt64(21);
  // Close ResultSet
  delete rset;
  // Check whether something should be deleted
  if (0 != obj->request() &&
      (0 == requestId ||
       obj->request()->id() != requestId)) {
    obj->request()->removeSubRequests(obj);
    obj->setRequest(0);
  }
  // Update object or create new one
  if (0 != requestId) {
    if (0 == obj->request()) {
      obj->setRequest
        (dynamic_cast<castor::stager::FileRequest*>
         (cnvSvc()->getObjFromId(requestId, OBJ_FileRequest)));
    } else {
      cnvSvc()->updateObj(obj->request());
    }
    obj->request()->addSubRequests(obj);
  }
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::createRep(castor::IAddress*,
                                                 castor::IObject* object,
                                                 bool endTransaction,
                                                 unsigned int type)
   {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  if (0 != obj->id()) return;
  try {
    // Check whether the statements are ok
    if (0 == m_insertStatement) {
      m_insertStatement = createStatement(s_insertStatementString);
      m_insertStatement->registerOutParam(23, castor::db::DBTYPE_UINT64);
    }
    // Now Save the current object
    m_insertStatement->setInt(1, obj->retryCounter());
    m_insertStatement->setString(2, obj->fileName());
    m_insertStatement->setString(3, obj->protocol());
    m_insertStatement->setUInt64(4, obj->xsize());
    m_insertStatement->setInt(5, obj->priority());
    m_insertStatement->setString(6, obj->subreqId());
    m_insertStatement->setInt(7, obj->flags());
    m_insertStatement->setInt(8, obj->modeBits());
    m_insertStatement->setInt(9, time(0));
    m_insertStatement->setInt(10, time(0));
    m_insertStatement->setInt(11, obj->answered());
    m_insertStatement->setInt(12, obj->errorCode());
    m_insertStatement->setString(13, obj->errorMessage());
    m_insertStatement->setString(14, obj->requestedFileSystems());
    m_insertStatement->setString(15, obj->svcHandler());
    m_insertStatement->setInt(16, obj->reqType());
    m_insertStatement->setUInt64(17, 0);
    m_insertStatement->setUInt64(18, (type == OBJ_CastorFile && obj->castorFile() != 0) ? obj->castorFile()->id() : 0);
    m_insertStatement->setInt(19, (int)obj->status());
    m_insertStatement->setUInt64(20, (type == OBJ_FileRequest && obj->request() != 0) ? obj->request()->id() : 0);
    m_insertStatement->setInt(21, (int)obj->getNextStatus());
    m_insertStatement->execute();
    obj->setId(m_insertStatement->getUInt64(22));
    if (endTransaction) {
      cnvSvc()->commit();
    }
  } catch (castor::exception::SQLError& e) {
    // Always try to rollback
    try {
      if (endTransaction) cnvSvc()->rollback();
    } catch (castor::exception::Exception& ignored) {}
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Error in insert request :"
                    << std::endl << e.getMessage().str() << std::endl
                    << "Statement was : " << std::endl
                    << s_insertStatementString << std::endl
                    << " and parameters' values were :" << std::endl
                    << "  retryCounter : " << obj->retryCounter() << std::endl
                    << "  fileName : " << obj->fileName() << std::endl
                    << "  protocol : " << obj->protocol() << std::endl
                    << "  xsize : " << obj->xsize() << std::endl
                    << "  priority : " << obj->priority() << std::endl
                    << "  subreqId : " << obj->subreqId() << std::endl
                    << "  flags : " << obj->flags() << std::endl
                    << "  modeBits : " << obj->modeBits() << std::endl
                    << "  creationTime : " << obj->creationTime() << std::endl
                    << "  lastModificationTime : " << obj->lastModificationTime() << std::endl
                    << "  answered : " << obj->answered() << std::endl
                    << "  errorCode : " << obj->errorCode() << std::endl
                    << "  errorMessage : " << obj->errorMessage() << std::endl
                    << "  requestedFileSystems : " << obj->requestedFileSystems() << std::endl
                    << "  svcHandler : " << obj->svcHandler() << std::endl
                    << "  reqType : " << obj->reqType() << std::endl
                    << "  id : " << obj->id() << std::endl
                    << "  diskcopy : " << 0 << std::endl
                    << "  castorFile : " << (obj->castorFile() ? obj->castorFile()->id() : 0) << std::endl
                    << "  status : " << obj->status() << std::endl
                    << "  request : " << (obj->request() ? obj->request()->id() : 0) << std::endl
                    << "  getNextStatus : " << obj->getNextStatus() << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// bulkCreateRep
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::bulkCreateRep(castor::IAddress*,
                                                     std::vector<castor::IObject*> &objects,
                                                     bool endTransaction,
                                                     unsigned int type)
   {
  // check whether something needs to be done
  int nb = objects.size();
  if (0 == nb) return;
  // Casts all objects
  std::vector<castor::stager::SubRequest*> objs;
  for (int i = 0; i < nb; i++) {
    objs.push_back(dynamic_cast<castor::stager::SubRequest*>(objects[i]));
  }
  std::vector<void *> allocMem;
  try {
    // Check whether the statements are ok
    if (0 == m_bulkInsertStatement) {
      m_bulkInsertStatement = createStatement(s_bulkInsertStatementString);
      m_bulkInsertStatement->registerOutParam(23, castor::db::DBTYPE_UINT64);
    }
    // build the buffers for retryCounter
    int* retryCounterBuffer = (int*) malloc(nb * sizeof(int));
    if (retryCounterBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(retryCounterBuffer);
    unsigned short* retryCounterBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (retryCounterBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(retryCounterBufLens);
    for (int i = 0; i < nb; i++) {
      retryCounterBuffer[i] = objs[i]->retryCounter();
      retryCounterBufLens[i] = sizeof(int);
    }
    m_bulkInsertStatement->setDataBuffer
      (1, retryCounterBuffer, castor::db::DBTYPE_INT, sizeof(retryCounterBuffer[0]), retryCounterBufLens);
    // build the buffers for fileName
    unsigned int fileNameMaxLen = 0;
    for (int i = 0; i < nb; i++) {
      if (objs[i]->fileName().length()+1 > fileNameMaxLen)
        fileNameMaxLen = objs[i]->fileName().length()+1;
    }
    char* fileNameBuffer = (char*) calloc(nb, fileNameMaxLen);
    if (fileNameBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(fileNameBuffer);
    unsigned short* fileNameBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (fileNameBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(fileNameBufLens);
    for (int i = 0; i < nb; i++) {
      strncpy(fileNameBuffer+(i*fileNameMaxLen), objs[i]->fileName().c_str(), fileNameMaxLen);
      fileNameBufLens[i] = objs[i]->fileName().length()+1; // + 1 for the trailing \0
    }
    m_bulkInsertStatement->setDataBuffer
      (2, fileNameBuffer, castor::db::DBTYPE_STRING, fileNameMaxLen, fileNameBufLens);
    // build the buffers for protocol
    unsigned int protocolMaxLen = 0;
    for (int i = 0; i < nb; i++) {
      if (objs[i]->protocol().length()+1 > protocolMaxLen)
        protocolMaxLen = objs[i]->protocol().length()+1;
    }
    char* protocolBuffer = (char*) calloc(nb, protocolMaxLen);
    if (protocolBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(protocolBuffer);
    unsigned short* protocolBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (protocolBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(protocolBufLens);
    for (int i = 0; i < nb; i++) {
      strncpy(protocolBuffer+(i*protocolMaxLen), objs[i]->protocol().c_str(), protocolMaxLen);
      protocolBufLens[i] = objs[i]->protocol().length()+1; // + 1 for the trailing \0
    }
    m_bulkInsertStatement->setDataBuffer
      (3, protocolBuffer, castor::db::DBTYPE_STRING, protocolMaxLen, protocolBufLens);
    // build the buffers for xsize
    double* xsizeBuffer = (double*) malloc(nb * sizeof(double));
    if (xsizeBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(xsizeBuffer);
    unsigned short* xsizeBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (xsizeBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(xsizeBufLens);
    for (int i = 0; i < nb; i++) {
      xsizeBuffer[i] = objs[i]->xsize();
      xsizeBufLens[i] = sizeof(double);
    }
    m_bulkInsertStatement->setDataBuffer
      (4, xsizeBuffer, castor::db::DBTYPE_UINT64, sizeof(xsizeBuffer[0]), xsizeBufLens);
    // build the buffers for priority
    int* priorityBuffer = (int*) malloc(nb * sizeof(int));
    if (priorityBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(priorityBuffer);
    unsigned short* priorityBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (priorityBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(priorityBufLens);
    for (int i = 0; i < nb; i++) {
      priorityBuffer[i] = objs[i]->priority();
      priorityBufLens[i] = sizeof(int);
    }
    m_bulkInsertStatement->setDataBuffer
      (5, priorityBuffer, castor::db::DBTYPE_INT, sizeof(priorityBuffer[0]), priorityBufLens);
    // build the buffers for subreqId
    unsigned int subreqIdMaxLen = 0;
    for (int i = 0; i < nb; i++) {
      if (objs[i]->subreqId().length()+1 > subreqIdMaxLen)
        subreqIdMaxLen = objs[i]->subreqId().length()+1;
    }
    char* subreqIdBuffer = (char*) calloc(nb, subreqIdMaxLen);
    if (subreqIdBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(subreqIdBuffer);
    unsigned short* subreqIdBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (subreqIdBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(subreqIdBufLens);
    for (int i = 0; i < nb; i++) {
      strncpy(subreqIdBuffer+(i*subreqIdMaxLen), objs[i]->subreqId().c_str(), subreqIdMaxLen);
      subreqIdBufLens[i] = objs[i]->subreqId().length()+1; // + 1 for the trailing \0
    }
    m_bulkInsertStatement->setDataBuffer
      (6, subreqIdBuffer, castor::db::DBTYPE_STRING, subreqIdMaxLen, subreqIdBufLens);
    // build the buffers for flags
    int* flagsBuffer = (int*) malloc(nb * sizeof(int));
    if (flagsBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(flagsBuffer);
    unsigned short* flagsBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (flagsBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(flagsBufLens);
    for (int i = 0; i < nb; i++) {
      flagsBuffer[i] = objs[i]->flags();
      flagsBufLens[i] = sizeof(int);
    }
    m_bulkInsertStatement->setDataBuffer
      (7, flagsBuffer, castor::db::DBTYPE_INT, sizeof(flagsBuffer[0]), flagsBufLens);
    // build the buffers for modeBits
    int* modeBitsBuffer = (int*) malloc(nb * sizeof(int));
    if (modeBitsBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(modeBitsBuffer);
    unsigned short* modeBitsBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (modeBitsBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(modeBitsBufLens);
    for (int i = 0; i < nb; i++) {
      modeBitsBuffer[i] = objs[i]->modeBits();
      modeBitsBufLens[i] = sizeof(int);
    }
    m_bulkInsertStatement->setDataBuffer
      (8, modeBitsBuffer, castor::db::DBTYPE_INT, sizeof(modeBitsBuffer[0]), modeBitsBufLens);
    // build the buffers for creationTime
    double* creationTimeBuffer = (double*) malloc(nb * sizeof(double));
    if (creationTimeBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(creationTimeBuffer);
    unsigned short* creationTimeBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (creationTimeBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(creationTimeBufLens);
    for (int i = 0; i < nb; i++) {
      creationTimeBuffer[i] = time(0);
      creationTimeBufLens[i] = sizeof(double);
    }
    m_bulkInsertStatement->setDataBuffer
      (9, creationTimeBuffer, castor::db::DBTYPE_UINT64, sizeof(creationTimeBuffer[0]), creationTimeBufLens);
    // build the buffers for lastModificationTime
    double* lastModificationTimeBuffer = (double*) malloc(nb * sizeof(double));
    if (lastModificationTimeBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(lastModificationTimeBuffer);
    unsigned short* lastModificationTimeBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (lastModificationTimeBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(lastModificationTimeBufLens);
    for (int i = 0; i < nb; i++) {
      lastModificationTimeBuffer[i] = time(0);
      lastModificationTimeBufLens[i] = sizeof(double);
    }
    m_bulkInsertStatement->setDataBuffer
      (10, lastModificationTimeBuffer, castor::db::DBTYPE_UINT64, sizeof(lastModificationTimeBuffer[0]), lastModificationTimeBufLens);
    // build the buffers for answered
    int* answeredBuffer = (int*) malloc(nb * sizeof(int));
    if (answeredBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(answeredBuffer);
    unsigned short* answeredBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (answeredBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(answeredBufLens);
    for (int i = 0; i < nb; i++) {
      answeredBuffer[i] = objs[i]->answered();
      answeredBufLens[i] = sizeof(int);
    }
    m_bulkInsertStatement->setDataBuffer
      (11, answeredBuffer, castor::db::DBTYPE_INT, sizeof(answeredBuffer[0]), answeredBufLens);
    // build the buffers for errorCode
    int* errorCodeBuffer = (int*) malloc(nb * sizeof(int));
    if (errorCodeBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(errorCodeBuffer);
    unsigned short* errorCodeBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (errorCodeBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(errorCodeBufLens);
    for (int i = 0; i < nb; i++) {
      errorCodeBuffer[i] = objs[i]->errorCode();
      errorCodeBufLens[i] = sizeof(int);
    }
    m_bulkInsertStatement->setDataBuffer
      (12, errorCodeBuffer, castor::db::DBTYPE_INT, sizeof(errorCodeBuffer[0]), errorCodeBufLens);
    // build the buffers for errorMessage
    unsigned int errorMessageMaxLen = 0;
    for (int i = 0; i < nb; i++) {
      if (objs[i]->errorMessage().length()+1 > errorMessageMaxLen)
        errorMessageMaxLen = objs[i]->errorMessage().length()+1;
    }
    char* errorMessageBuffer = (char*) calloc(nb, errorMessageMaxLen);
    if (errorMessageBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(errorMessageBuffer);
    unsigned short* errorMessageBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (errorMessageBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(errorMessageBufLens);
    for (int i = 0; i < nb; i++) {
      strncpy(errorMessageBuffer+(i*errorMessageMaxLen), objs[i]->errorMessage().c_str(), errorMessageMaxLen);
      errorMessageBufLens[i] = objs[i]->errorMessage().length()+1; // + 1 for the trailing \0
    }
    m_bulkInsertStatement->setDataBuffer
      (13, errorMessageBuffer, castor::db::DBTYPE_STRING, errorMessageMaxLen, errorMessageBufLens);
    // build the buffers for requestedFileSystems
    unsigned int requestedFileSystemsMaxLen = 0;
    for (int i = 0; i < nb; i++) {
      if (objs[i]->requestedFileSystems().length()+1 > requestedFileSystemsMaxLen)
        requestedFileSystemsMaxLen = objs[i]->requestedFileSystems().length()+1;
    }
    char* requestedFileSystemsBuffer = (char*) calloc(nb, requestedFileSystemsMaxLen);
    if (requestedFileSystemsBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(requestedFileSystemsBuffer);
    unsigned short* requestedFileSystemsBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (requestedFileSystemsBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(requestedFileSystemsBufLens);
    for (int i = 0; i < nb; i++) {
      strncpy(requestedFileSystemsBuffer+(i*requestedFileSystemsMaxLen), objs[i]->requestedFileSystems().c_str(), requestedFileSystemsMaxLen);
      requestedFileSystemsBufLens[i] = objs[i]->requestedFileSystems().length()+1; // + 1 for the trailing \0
    }
    m_bulkInsertStatement->setDataBuffer
      (14, requestedFileSystemsBuffer, castor::db::DBTYPE_STRING, requestedFileSystemsMaxLen, requestedFileSystemsBufLens);
    // build the buffers for svcHandler
    unsigned int svcHandlerMaxLen = 0;
    for (int i = 0; i < nb; i++) {
      if (objs[i]->svcHandler().length()+1 > svcHandlerMaxLen)
        svcHandlerMaxLen = objs[i]->svcHandler().length()+1;
    }
    char* svcHandlerBuffer = (char*) calloc(nb, svcHandlerMaxLen);
    if (svcHandlerBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(svcHandlerBuffer);
    unsigned short* svcHandlerBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (svcHandlerBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(svcHandlerBufLens);
    for (int i = 0; i < nb; i++) {
      strncpy(svcHandlerBuffer+(i*svcHandlerMaxLen), objs[i]->svcHandler().c_str(), svcHandlerMaxLen);
      svcHandlerBufLens[i] = objs[i]->svcHandler().length()+1; // + 1 for the trailing \0
    }
    m_bulkInsertStatement->setDataBuffer
      (15, svcHandlerBuffer, castor::db::DBTYPE_STRING, svcHandlerMaxLen, svcHandlerBufLens);
    // build the buffers for reqType
    int* reqTypeBuffer = (int*) malloc(nb * sizeof(int));
    if (reqTypeBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(reqTypeBuffer);
    unsigned short* reqTypeBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (reqTypeBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(reqTypeBufLens);
    for (int i = 0; i < nb; i++) {
      reqTypeBuffer[i] = objs[i]->reqType();
      reqTypeBufLens[i] = sizeof(int);
    }
    m_bulkInsertStatement->setDataBuffer
      (16, reqTypeBuffer, castor::db::DBTYPE_INT, sizeof(reqTypeBuffer[0]), reqTypeBufLens);
    // build the buffers for diskcopy
    double* diskcopyBuffer = (double*) malloc(nb * sizeof(double));
    if (diskcopyBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(diskcopyBuffer);
    unsigned short* diskcopyBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (diskcopyBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(diskcopyBufLens);
    for (int i = 0; i < nb; i++) {
      diskcopyBuffer[i] = 0;
      diskcopyBufLens[i] = sizeof(double);
    }
    m_bulkInsertStatement->setDataBuffer
      (17, diskcopyBuffer, castor::db::DBTYPE_UINT64, sizeof(diskcopyBuffer[0]), diskcopyBufLens);
    // build the buffers for castorFile
    double* castorFileBuffer = (double*) malloc(nb * sizeof(double));
    if (castorFileBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(castorFileBuffer);
    unsigned short* castorFileBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (castorFileBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(castorFileBufLens);
    for (int i = 0; i < nb; i++) {
      castorFileBuffer[i] = (type == OBJ_CastorFile && objs[i]->castorFile() != 0) ? objs[i]->castorFile()->id() : 0;
      castorFileBufLens[i] = sizeof(double);
    }
    m_bulkInsertStatement->setDataBuffer
      (18, castorFileBuffer, castor::db::DBTYPE_UINT64, sizeof(castorFileBuffer[0]), castorFileBufLens);
    // build the buffers for status
    int* statusBuffer = (int*) malloc(nb * sizeof(int));
    if (statusBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(statusBuffer);
    unsigned short* statusBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (statusBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(statusBufLens);
    for (int i = 0; i < nb; i++) {
      statusBuffer[i] = objs[i]->status();
      statusBufLens[i] = sizeof(int);
    }
    m_bulkInsertStatement->setDataBuffer
      (19, statusBuffer, castor::db::DBTYPE_INT, sizeof(statusBuffer[0]), statusBufLens);
    // build the buffers for request
    double* requestBuffer = (double*) malloc(nb * sizeof(double));
    if (requestBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(requestBuffer);
    unsigned short* requestBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (requestBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(requestBufLens);
    for (int i = 0; i < nb; i++) {
      requestBuffer[i] = (type == OBJ_FileRequest && objs[i]->request() != 0) ? objs[i]->request()->id() : 0;
      requestBufLens[i] = sizeof(double);
    }
    m_bulkInsertStatement->setDataBuffer
      (20, requestBuffer, castor::db::DBTYPE_UINT64, sizeof(requestBuffer[0]), requestBufLens);
    // build the buffers for getNextStatus
    int* getNextStatusBuffer = (int*) malloc(nb * sizeof(int));
    if (getNextStatusBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(getNextStatusBuffer);
    unsigned short* getNextStatusBufLens = (unsigned short*) malloc(nb * sizeof(unsigned short));
    if (getNextStatusBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(getNextStatusBufLens);
    for (int i = 0; i < nb; i++) {
      getNextStatusBuffer[i] = objs[i]->getNextStatus();
      getNextStatusBufLens[i] = sizeof(int);
    }
    m_bulkInsertStatement->setDataBuffer
      (21, getNextStatusBuffer, castor::db::DBTYPE_INT, sizeof(getNextStatusBuffer[0]), getNextStatusBufLens);
    // build the buffers for returned ids
    double* idBuffer = (double*) calloc(nb, sizeof(double));
    if (idBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(idBuffer);
    unsigned short* idBufLens = (unsigned short*) calloc(nb, sizeof(unsigned short));
    if (idBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocMem.push_back(idBufLens);
    m_bulkInsertStatement->setDataBuffer
      (22, idBuffer, castor::db::DBTYPE_UINT64, sizeof(double), idBufLens);
    m_bulkInsertStatement->execute(nb);
    for (int i = 0; i < nb; i++) {
      objects[i]->setId((u_signed64)idBuffer[i]);
    }
    // release the buffers
    for (unsigned int i = 0; i < allocMem.size(); i++) {
      free(allocMem[i]);
    }
    if (endTransaction) {
      cnvSvc()->commit();
    }
  } catch (castor::exception::SQLError& e) {
    // release the buffers
    for (unsigned int i = 0; i < allocMem.size(); i++) {
      free(allocMem[i]);
    }
    // Always try to rollback
    try {
      if (endTransaction) cnvSvc()->rollback();
    } catch (castor::exception::Exception& ignored) {}
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Error in bulkInsert request :"
                    << std::endl << e.getMessage().str() << std::endl
                    << " was called in bulk with "
                    << nb << " items." << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::updateRep(castor::IAddress*,
                                                 castor::IObject* object,
                                                 bool endTransaction)
   {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Update the current object
    m_updateStatement->setInt(1, obj->retryCounter());
    m_updateStatement->setString(2, obj->fileName());
    m_updateStatement->setString(3, obj->protocol());
    m_updateStatement->setUInt64(4, obj->xsize());
    m_updateStatement->setInt(5, obj->priority());
    m_updateStatement->setString(6, obj->subreqId());
    m_updateStatement->setInt(7, obj->flags());
    m_updateStatement->setInt(8, obj->modeBits());
    m_updateStatement->setInt(9, time(0));
    m_updateStatement->setInt(10, obj->answered());
    m_updateStatement->setInt(11, obj->errorCode());
    m_updateStatement->setString(12, obj->errorMessage());
    m_updateStatement->setString(13, obj->requestedFileSystems());
    m_updateStatement->setString(14, obj->svcHandler());
    m_updateStatement->setInt(15, obj->reqType());
    m_updateStatement->setInt(16, (int)obj->status());
    m_updateStatement->setInt(17, (int)obj->getNextStatus());
    m_updateStatement->setUInt64(18, obj->id());
    m_updateStatement->execute();
    if (endTransaction) {
      cnvSvc()->commit();
    }
  } catch (castor::exception::SQLError& e) {
    // Always try to rollback
    try {
      if (endTransaction) cnvSvc()->rollback();
    } catch (castor::exception::Exception& ignored) {}
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Error in update request :"
                    << std::endl << e.getMessage().str() << std::endl
                    << "Statement was : " << std::endl
                    << s_updateStatementString << std::endl
                    << " and id was " << obj->id() << std::endl;;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// deleteRep
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::deleteRep(castor::IAddress*,
                                                 castor::IObject* object,
                                                 bool endTransaction)
   {
  castor::stager::SubRequest* obj = 
    dynamic_cast<castor::stager::SubRequest*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_deleteStatement) {
      m_deleteStatement = createStatement(s_deleteStatementString);
    }
    // Now Delete the object
    m_deleteStatement->setUInt64(1, obj->id());
    m_deleteStatement->execute();
    if (endTransaction) {
      cnvSvc()->commit();
    }
  } catch (castor::exception::SQLError& e) {
    // Always try to rollback
    try {
      if (endTransaction) cnvSvc()->rollback();
    } catch (castor::exception::Exception& ignored) {}
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Error in delete request :"
                    << std::endl << e.getMessage().str() << std::endl
                    << "Statement was : " << std::endl
                    << s_deleteStatementString << std::endl
                    << " and id was " << obj->id() << std::endl;;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createObj
//------------------------------------------------------------------------------
castor::IObject* castor::db::cnv::DbSubRequestCnv::createObj(castor::IAddress* address)
   {
  castor::BaseAddress* ad = 
    dynamic_cast<castor::BaseAddress*>(address);
  try {
    // Check whether the statement is ok
    if (0 == m_selectStatement) {
      m_selectStatement = createStatement(s_selectStatementString);
    }
    // retrieve the object from the database
    m_selectStatement->setUInt64(1, ad->target());
    castor::db::IDbResultSet *rset = m_selectStatement->executeQuery();
    if (!rset->next()) {
      castor::exception::NoEntry ex;
      ex.getMessage() << "No object found for id :" << ad->target();
      throw ex;
    }
    // create the new Object
    castor::stager::SubRequest* object = new castor::stager::SubRequest();
    // Now retrieve and set members
    object->setRetryCounter(rset->getInt(1));
    object->setFileName(rset->getString(2));
    object->setProtocol(rset->getString(3));
    object->setXsize(rset->getUInt64(4));
    object->setPriority(rset->getInt(5));
    object->setSubreqId(rset->getString(6));
    object->setFlags(rset->getInt(7));
    object->setModeBits(rset->getInt(8));
    object->setCreationTime(rset->getUInt64(9));
    object->setLastModificationTime(rset->getUInt64(10));
    object->setAnswered(rset->getInt(11));
    object->setErrorCode(rset->getInt(12));
    object->setErrorMessage(rset->getString(13));
    object->setRequestedFileSystems(rset->getString(14));
    object->setSvcHandler(rset->getString(15));
    object->setReqType(rset->getInt(16));
    object->setId(rset->getUInt64(17));
    object->setStatus((enum castor::stager::SubRequestStatusCodes)rset->getInt(20));
    object->setGetNextStatus((enum castor::stager::SubRequestGetNextStatusCodes)rset->getInt(22));
    delete rset;
    return object;
  } catch (castor::exception::SQLError& e) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Error in select request :"
                    << std::endl << e.getMessage().str() << std::endl
                    << "Statement was : " << std::endl
                    << s_selectStatementString << std::endl
                    << " and id was " << ad->target() << std::endl;;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// bulkCreateObj
//------------------------------------------------------------------------------
std::vector<castor::IObject*>
castor::db::cnv::DbSubRequestCnv::bulkCreateObj(castor::IAddress* address)
   {
  // Prepare result
  std::vector<castor::IObject*> res;
  // check whether something needs to be done
  castor::VectorAddress* ad = 
    dynamic_cast<castor::VectorAddress*>(address);
  int nb = ad->target().size();
  if (0 == nb) return res;
  try {
    // Check whether the statement is ok
    if (0 == m_bulkSelectStatement) {
      m_bulkSelectStatement = createStatement(s_bulkSelectStatementString);
      m_bulkSelectStatement->registerOutParam(2, castor::db::DBTYPE_CURSOR);
    }
    // set the buffer for input ids
    m_bulkSelectStatement->setDataBufferUInt64Array(1, ad->target());
    // Execute statement
    m_bulkSelectStatement->execute();
    // get the result, that is a cursor on the selected rows
    castor::db::IDbResultSet *rset =
      m_bulkSelectStatement->getCursor(2);
    // loop and create the new objects
    bool status = rset->next();
    while (status) {
      // create the new Object
      castor::stager::SubRequest* object = new castor::stager::SubRequest();
      // Now retrieve and set members
      object->setRetryCounter(rset->getInt(1));
      object->setFileName(rset->getString(2));
      object->setProtocol(rset->getString(3));
      object->setXsize(rset->getUInt64(4));
      object->setPriority(rset->getInt(5));
      object->setSubreqId(rset->getString(6));
      object->setFlags(rset->getInt(7));
      object->setModeBits(rset->getInt(8));
      object->setCreationTime(rset->getUInt64(9));
      object->setLastModificationTime(rset->getUInt64(10));
      object->setAnswered(rset->getInt(11));
      object->setErrorCode(rset->getInt(12));
      object->setErrorMessage(rset->getString(13));
      object->setRequestedFileSystems(rset->getString(14));
      object->setSvcHandler(rset->getString(15));
      object->setReqType(rset->getInt(16));
      object->setId(rset->getUInt64(17));
      object->setStatus((enum castor::stager::SubRequestStatusCodes)rset->getInt(20));
      object->setGetNextStatus((enum castor::stager::SubRequestGetNextStatusCodes)rset->getInt(22));
      // store object in results and loop;
      res.push_back(object);
      status = rset->next();
    }
    delete rset;
    return res;
  } catch (castor::exception::SQLError& e) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Error in bulkSelect request :"
                    << std::endl << e.getMessage().str() << std::endl
                    << " was called in bulk with "
                    << nb << " items." << std::endl;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::db::cnv::DbSubRequestCnv::updateObj(castor::IObject* obj)
   {
  try {
    // Check whether the statement is ok
    if (0 == m_selectStatement) {
      m_selectStatement = createStatement(s_selectStatementString);
    }
    // retrieve the object from the database
    m_selectStatement->setUInt64(1, obj->id());
    castor::db::IDbResultSet *rset = m_selectStatement->executeQuery();
    if (!rset->next()) {
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
    object->setXsize(rset->getUInt64(4));
    object->setPriority(rset->getInt(5));
    object->setSubreqId(rset->getString(6));
    object->setFlags(rset->getInt(7));
    object->setModeBits(rset->getInt(8));
    object->setCreationTime(rset->getUInt64(9));
    object->setLastModificationTime(rset->getUInt64(10));
    object->setAnswered(rset->getInt(11));
    object->setErrorCode(rset->getInt(12));
    object->setErrorMessage(rset->getString(13));
    object->setRequestedFileSystems(rset->getString(14));
    object->setSvcHandler(rset->getString(15));
    object->setReqType(rset->getInt(16));
    object->setId(rset->getUInt64(17));
    object->setStatus((enum castor::stager::SubRequestStatusCodes)rset->getInt(20));
    object->setGetNextStatus((enum castor::stager::SubRequestGetNextStatusCodes)rset->getInt(22));
    delete rset;
  } catch (castor::exception::SQLError& e) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Error in update request :"
                    << std::endl << e.getMessage().str() << std::endl
                    << "Statement was : " << std::endl
                    << s_updateStatementString << std::endl
                    << " and id was " << obj->id() << std::endl;;
    throw ex;
  }
}

