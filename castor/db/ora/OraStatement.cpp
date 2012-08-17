/******************************************************************************
 *                      OraStatement.cpp
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
 * @(#)$RCSfile: OraStatement.cpp,v $ $Revision: 1.20 $ $Release$ $Date: 2009/03/26 14:30:12 $ $Author: itglp $
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#include "OraStatement.hpp"
#include "OraResultSet.hpp"
#include "castor/exception/OutOfMemory.hpp"
#include "occi.h"


//------------------------------------------------------------------------------
// OraStatement
//------------------------------------------------------------------------------
castor::db::ora::OraStatement::OraStatement(oracle::occi::Statement* stmt, castor::db::ora::OraCnvSvc* cnvSvc) throw() :
  m_statement(stmt),
  m_cnvSvc(cnvSvc),
  m_clobBuf(""),
  m_clobPos(0),
  m_arrayBuf(0),
  m_arrayBufLens(0),
  m_arrayPos(0),
  m_arraySize(0)
{
  m_statement->setAutoCommit(false);
}

//------------------------------------------------------------------------------
// ~OraStatement
//------------------------------------------------------------------------------
castor::db::ora::OraStatement::~OraStatement() throw() {
  try {
    // Free memory buffers
    if (m_arrayBuf)
      free(m_arrayBuf);
    m_arrayBuf = 0;
    if (m_arrayBufLens)
      free(m_arrayBufLens);
    m_arrayBufLens = 0;
    m_arrayPos = 0;
    if (m_arraySize) 
      free(m_arraySize);
    m_arraySize = 0;
    // Close statement
    m_cnvSvc->terminateStatement(m_statement);
  }
  catch(castor::exception::Exception &ignored) {}
}

//------------------------------------------------------------------------------
// endTransaction
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::endTransaction()
{
  m_statement->setAutoCommit(true);
}

//------------------------------------------------------------------------------
// setInt
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setInt(int pos, int value)
{
  m_statement->setInt(pos, value);
}

//------------------------------------------------------------------------------
// setInt64
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setInt64(int pos, signed64 value)
{
  m_statement->setDouble(pos, (double)value);
}

//------------------------------------------------------------------------------
// setUInt64
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setUInt64(int pos, u_signed64 value)
{
  m_statement->setDouble(pos, (double)value);
}

//------------------------------------------------------------------------------
// setString
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setString(int pos, std::string value)
{
  m_statement->setString(pos, value);
}

//------------------------------------------------------------------------------
// setFloat
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setFloat(int pos, float value)
{
  m_statement->setFloat(pos, value);
}

//------------------------------------------------------------------------------
// setDouble
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setDouble(int pos, double value)
{
  m_statement->setDouble(pos, value);
}

//------------------------------------------------------------------------------
// setClob
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setClob(int pos, std::string value)
{
  // A Clob is actually stored in two steps: first as an empty one
  oracle::occi::Clob clob(m_cnvSvc->getConnection());
  clob.setEmpty();
  m_statement->setClob(pos, clob);
  // then afterwards with a SELECT FOR UPDATE query: so we keep the value
  // for later use in the execute method
  m_clobBuf = value;
  if(m_clobBuf.empty()) {
    // we explicitly avoid this because any subsequent query on a row
    // where an empty clob was stored will fail!
    m_clobBuf = " ";
  }
  m_clobPos = pos;
}

// -----------------------------------------------------------------------------
// setNull
// -----------------------------------------------------------------------------
void castor::db::ora::OraStatement::setNull(int pos)
{
  m_statement->setNull(pos, oracle::occi::OCCIINT);
}

//------------------------------------------------------------------------------
// setDataBuffer
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setDataBuffer
(int pos, void* buffer, unsigned dbType, unsigned size, void* bufLens)
  throw(castor::exception::SQLError) {
  if(dbType > DBTYPE_MAXVALUE) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Invalid dbType: " << dbType;
    throw ex;
  }
  try {
    m_statement->setDataBuffer(pos, buffer, oraBulkTypeMap[dbType], size, (ub2*)bufLens);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDataBufferArray
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setDataBufferArray
(int pos, void* buffer, unsigned dbType, unsigned size, unsigned elementSize, void* bufLens)
  throw(castor::exception::Exception) {
  try {
    if (0 == m_arraySize) {
      m_arraySize = (ub4*) malloc(sizeof(ub4));
      if (0 == m_arraySize) {
        castor::exception::OutOfMemory e;
        throw e;
      }
    }
    *m_arraySize = size;
    m_statement->setDataBufferArray(pos, buffer,
      // yes, Oracle is not that symmetric in data type handling...
      (dbType == DBTYPE_UINT64 || dbType == DBTYPE_INT64 ? oracle::occi::OCCI_SQLT_NUM : oraBulkTypeMap[dbType]),
      size, m_arraySize, elementSize, (ub2*)bufLens);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDataBufferUInt64Array
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::setDataBufferUInt64Array
(int pos, std::vector<u_signed64> data)
  throw(castor::exception::Exception) {
  m_arrayPos = pos;
  // a dedicated method to handle u_signed64 arrays, i.e. arrays of ids
  unsigned int nb1 = data.size() == 0 ? 1 : data.size();
  ub2* lens = (ub2*) malloc(nb1 * sizeof(ub2));
  if (lens == 0) {
    castor::exception::OutOfMemory e;
    throw e;
  }
  m_arrayBufLens = lens;
  unsigned char (*buf)[21] = (unsigned char(*)[21]) calloc(nb1*21, sizeof(unsigned char));
  if (buf == 0) {
    castor::exception::OutOfMemory e;
    throw e;
  }
  m_arrayBuf = buf;

  for(unsigned int i = 0; i < data.size(); i++) {
    oracle::occi::Number n = (double)data[i];
    oracle::occi::Bytes b = n.toBytes();
    lens[i] = b.length();
    b.getBytes(buf[i], lens[i]);
  }
  setDataBufferArray(pos, buf, DBTYPE_UINT64, nb1, 21, lens);
}

//------------------------------------------------------------------------------
// registerOutParam
//------------------------------------------------------------------------------
void castor::db::ora::OraStatement::registerOutParam(int pos, unsigned dbType)
  throw (castor::exception::SQLError) {
  if(dbType > DBTYPE_MAXVALUE) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Invalid dbType: " << dbType;
    throw ex;
  }
  try {
    m_statement->registerOutParam(pos, oraTypeMap[dbType]);
  } catch (oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getInt
//------------------------------------------------------------------------------
int castor::db::ora::OraStatement::getInt(int pos)
  throw (castor::exception::SQLError) {
  try {
    return m_statement->getInt(pos);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getInt64
//------------------------------------------------------------------------------
signed64 castor::db::ora::OraStatement::getInt64(int pos)
  throw (castor::exception::SQLError) {
  try {
    return (signed64)m_statement->getDouble(pos);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getUInt64
//------------------------------------------------------------------------------
u_signed64 castor::db::ora::OraStatement::getUInt64(int pos)
  throw (castor::exception::SQLError) {
  try {
    return (u_signed64)m_statement->getDouble(pos);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getString
//------------------------------------------------------------------------------
std::string castor::db::ora::OraStatement::getString(int pos)
  throw (castor::exception::SQLError) {
  try {
    return m_statement->getString(pos);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getFloat
//------------------------------------------------------------------------------
float castor::db::ora::OraStatement::getFloat(int pos)
  throw (castor::exception::SQLError) {
  try {
    return m_statement->getFloat(pos);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getDouble
//------------------------------------------------------------------------------
double castor::db::ora::OraStatement::getDouble(int pos)
  throw (castor::exception::SQLError) {
  try {
    return m_statement->getDouble(pos);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getClob
//------------------------------------------------------------------------------
std::string castor::db::ora::OraStatement::getClob(int pos)
  throw (castor::exception::SQLError) {
  try {
    oracle::occi::Clob clob = m_statement->getClob(pos);
    clob.open(oracle::occi::OCCI_LOB_READONLY);
    int len = clob.length();
    char* buf = (char*) malloc(len+1);
    clob.read(len, (unsigned char*)buf, len);
    buf[len] = 0;
    clob.close();
    std::string res(buf);
    free(buf);
    return res;
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getCursor
//------------------------------------------------------------------------------
castor::db::IDbResultSet* castor::db::ora::OraStatement::getCursor(int pos)
  throw (castor::exception::SQLError) {
  try {
    return new castor::db::ora::OraResultSet(m_statement->getCursor(pos), m_statement);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
castor::db::IDbResultSet* castor::db::ora::OraStatement::executeQuery()
  throw (castor::exception::SQLError) {
  try {
    return new castor::db::ora::OraResultSet(m_statement->executeQuery(), m_statement);
  }
  catch(oracle::occi::SQLException e) {
    m_cnvSvc->handleException(e);

    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    ex.setSQLErrorCode(e.getErrorCode());
    throw ex;
  }
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
int castor::db::ora::OraStatement::execute(int count)
  throw (castor::exception::Exception) {
  try {
    int ret = 0;
    if(count == 1) {
      ret = m_statement->executeUpdate();
    }
    else {
      if(m_clobPos > 0) {
        // bulk operations are not supported when CLOB data is involved
        castor::exception::SQLError ex;
        ex.getMessage() << "Cannot execute a bulk query with CLOB values, operation not supported";
        throw ex;
      }
      ret = m_statement->executeArrayUpdate(count);
    }
    // Free memory buffers
    if (m_arrayPos > 0) {
      if (m_arrayBuf)
        free(m_arrayBuf);
      m_arrayBuf = 0;
      if (m_arrayBufLens)
        free(m_arrayBufLens);
      m_arrayBufLens = 0;
      m_arrayPos = 0;   // reset for next usage
    }
    // special handling for clobs: they need to be populated in this weird way...
    if(m_clobPos > 0) {
      // extract needed metadata: we know we are in a 'standard' INSERT or UPDATE
      // statement, where 'standard' means autogenerated with genCastor
      std::string origQuery = m_statement->getSQL();
      std::stringstream ssQry(origQuery);
      std::string buf, tabName, fieldName;
      unsigned idPos;
      ssQry >> buf;
      if(buf == "UPDATE") {
        // case of update, typical query:
        // "UPDATE Table SET field1 = :1, field2 = :2, ... WHERE id = :N";
        ssQry >> tabName;
        ssQry >> buf;
        for(unsigned i = 0; i < 3*(m_clobPos-1); i++) {
          ssQry >> buf;
        }
        ssQry >> fieldName;
      }
      else {
        // case of insert, typical query:
        // "INSERT INTO Table (field1, field2, ...) VALUES (:1, :2, ...) RETURNING id INTO :N";
        ssQry >> buf;
        ssQry >> tabName;
        for(unsigned i = 0; i < m_clobPos-1; i++) {
          ssQry >> buf;
        }
        ssQry >> fieldName;
        // drop the trailing ','; we assume we're *not* the first field
        fieldName = fieldName.substr(0, fieldName.size()-1);
      }
      idPos = atoi(origQuery.substr(origQuery.rfind(":")+1).c_str());
      std::stringstream clobQuery;
      clobQuery << "SELECT " << fieldName << " FROM " << tabName
                << " WHERE id = " << (u_signed64)m_statement->getDouble(idPos) << " FOR UPDATE";

      // now run the SELECT FOR UPDATE query and populate the clob
      oracle::occi::Statement* stmt = m_cnvSvc->getConnection()->createStatement(clobQuery.str());
      oracle::occi::ResultSet* rset = stmt->executeQuery();
      if (rset->next() != oracle::occi::ResultSet::END_OF_FETCH) {
        oracle::occi::Clob clob = rset->getClob(1);
        clob.open(oracle::occi::OCCI_LOB_READWRITE);
        int len = m_clobBuf.size();
        clob.write(len, (unsigned char*)m_clobBuf.c_str(), len);
        clob.close();
      }
      stmt->executeUpdate();
      stmt->closeResultSet(rset);
      m_cnvSvc->getConnection()->terminateStatement(stmt);

      m_clobPos = 0;   // reset for next usage
    }
    return ret;
  }
  catch(oracle::occi::SQLException e) {
    // Free memory buffers
    if (m_arrayPos > 0) {
      if (m_arrayBuf)
        free(m_arrayBuf);
      m_arrayBuf = 0;
      if (m_arrayBufLens)
        free(m_arrayBufLens);
      m_arrayBufLens = 0;
      m_arrayPos = 0;   // reset for next usage
    }
    m_cnvSvc->handleException(e);

    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    ex.setSQLErrorCode(e.getErrorCode());
    throw ex;
  }
}
