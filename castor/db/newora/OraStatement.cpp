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
 * @(#)$RCSfile: OraStatement.cpp,v $ $Revision: 1.15 $ $Release$ $Date: 2008/06/19 15:12:42 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#include "OraStatement.hpp"
#include "OraResultSet.hpp"
#include "occi.h"


// -----------------------------------------------------------------------
// OraStatement
// -----------------------------------------------------------------------
castor::db::ora::OraStatement::OraStatement(oracle::occi::Statement* stmt, castor::db::ora::OraCnvSvc* cnvSvc) :
  m_statement(stmt), m_cnvSvc(cnvSvc), m_clobBuf(""), m_clobPos(0)
{
  m_statement->setAutoCommit(false);
}

// -----------------------------------------------------------------------
// ~OraStatement
// -----------------------------------------------------------------------
castor::db::ora::OraStatement::~OraStatement() {
  try {
    m_cnvSvc->closeStatement(this);
  }
  catch(oracle::occi::SQLException ignored) {}
}


// -----------------------------------------------------------------------
// endTransaction
// -----------------------------------------------------------------------
void castor::db::ora::OraStatement::endTransaction()
{
  m_statement->setAutoCommit(true);
}

// -----------------------------------------------------------------------
// setInt
// -----------------------------------------------------------------------
void castor::db::ora::OraStatement::setInt(int pos, int value)
{
  m_statement->setInt(pos, value);
}

// -----------------------------------------------------------------------
// setInt64
// -----------------------------------------------------------------------
void castor::db::ora::OraStatement::setInt64(int pos, signed64 value)
{
  m_statement->setDouble(pos, (double)value);
}

// -----------------------------------------------------------------------
// setUInt64
// -----------------------------------------------------------------------
void castor::db::ora::OraStatement::setUInt64(int pos, u_signed64 value)
{
  m_statement->setDouble(pos, (double)value);
}

// -----------------------------------------------------------------------
// setString
// -----------------------------------------------------------------------
void castor::db::ora::OraStatement::setString(int pos, std::string value)
{
  m_statement->setString(pos, value);
}

// -----------------------------------------------------------------------
// setClob
// -----------------------------------------------------------------------
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

// -----------------------------------------------------------------------
// setFloat
// -----------------------------------------------------------------------
void castor::db::ora::OraStatement::setFloat(int pos, float value)
{
  m_statement->setFloat(pos, value);
}

// -----------------------------------------------------------------------
// setDouble
// -----------------------------------------------------------------------
void castor::db::ora::OraStatement::setDouble(int pos, double value)
{
  m_statement->setDouble(pos, value);
}

// -----------------------------------------------------------------------
// setDataBuffer
// -----------------------------------------------------------------------
void castor::db::ora::OraStatement::setDataBuffer
  (int pos, void* buffer, unsigned dbType, unsigned size, void* bufLen)
  throw(castor::exception::SQLError) {
  if(dbType > DBTYPE_MAXVALUE) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Invalid dbType: " << dbType;
    throw ex;
  }    
  try {
    m_statement->setDataBuffer(pos, buffer, oraBulkTypeMap[dbType], size, (ub2*)bufLen);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}


// -----------------------------------------------------------------------
// registerOutParam
// -----------------------------------------------------------------------
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

// -----------------------------------------------------------------------
// getInt
// -----------------------------------------------------------------------
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

// -----------------------------------------------------------------------
// getInt64
// -----------------------------------------------------------------------
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

// -----------------------------------------------------------------------
// getUInt64
// -----------------------------------------------------------------------
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

// -----------------------------------------------------------------------
// getString
// -----------------------------------------------------------------------
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

// -----------------------------------------------------------------------
// getClob
// -----------------------------------------------------------------------
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
    return std::string(res);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// getFloat
// -----------------------------------------------------------------------
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

// -----------------------------------------------------------------------
// getDouble
// -----------------------------------------------------------------------
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


// -----------------------------------------------------------------------
// executeQuery
// -----------------------------------------------------------------------
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

// -----------------------------------------------------------------------
// execute
// -----------------------------------------------------------------------
int castor::db::ora::OraStatement::execute(int count)
  throw (castor::exception::SQLError) {
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
      if(rset->next()) {
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
    m_cnvSvc->handleException(e);

    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    ex.setSQLErrorCode(e.getErrorCode());
    throw ex;
  }
}
