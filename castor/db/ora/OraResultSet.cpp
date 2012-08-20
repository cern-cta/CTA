/******************************************************************************
 *                      OraResultSet.cpp
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
 * @(#)$RCSfile: OraResultSet.cpp,v $ $Revision: 1.7 $ $Release$ $Date: 2008/06/19 15:12:42 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#include "OraResultSet.hpp"
#include "OraStatement.hpp"
#include "castor/exception/OutOfMemory.hpp"

castor::db::ora::OraResultSet::OraResultSet(oracle::occi::ResultSet* rset, oracle::occi::Statement* statement) :
  m_rset(rset),
  m_statement(statement)
{
}

castor::db::ora::OraResultSet::~OraResultSet() {
  try {
    m_statement->closeResultSet(m_rset);
  }
  catch(oracle::occi::SQLException ignored) {}
}

bool castor::db::ora::OraResultSet::next(int count)
  throw (castor::exception::SQLError) {
  try {
    return (m_rset->next(count) != oracle::occi::ResultSet::END_OF_FETCH);
  }
  catch(oracle::occi::SQLException e) {
    //m_cnvSvc->handleException(e);
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

int castor::db::ora::OraResultSet::getInt(int i)
  throw (castor::exception::SQLError) {
  try {
    return m_rset->getInt(i);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

signed64 castor::db::ora::OraResultSet::getInt64(int i)
  throw (castor::exception::SQLError) {
  try {
    return (signed64)m_rset->getDouble(i);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

u_signed64 castor::db::ora::OraResultSet::getUInt64(int i)
  throw (castor::exception::SQLError) {
  try {
    return (u_signed64)m_rset->getDouble(i);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

float castor::db::ora::OraResultSet::getFloat(int i)
  throw (castor::exception::SQLError) {
  try {
    return m_rset->getFloat(i);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

double castor::db::ora::OraResultSet::getDouble(int i)
  throw (castor::exception::SQLError) {
  try {
    return m_rset->getDouble(i);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

std::string castor::db::ora::OraResultSet::getString(int i)
  throw (castor::exception::SQLError) {
  try {
    return m_rset->getString(i);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}

std::string castor::db::ora::OraResultSet::getClob(int i)
  throw (castor::exception::SQLError) {
  try {
    oracle::occi::Clob clob = m_rset->getClob(i);
    clob.open(oracle::occi::OCCI_LOB_READONLY);
    int len = clob.length();
    char* buf = (char*) malloc(len+1);
    if (0 == buf) {
      castor::exception::OutOfMemory ex;
      throw ex;
    }
    buf[len] = 0;
    clob.read(len, (unsigned char*)buf, len+1);
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

void castor::db::ora::OraResultSet::setDataBuffer
  (int pos, void* buffer, unsigned dbType, unsigned size, void* bufLen)
  throw(castor::exception::SQLError) {
  if(dbType > DBTYPE_MAXVALUE) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Invalid dbType: " << dbType;
    throw ex;
  }    
  try {
    m_rset->setDataBuffer(pos, buffer, castor::db::ora::oraBulkTypeMap[dbType], size, (ub2*)bufLen);
  }
  catch(oracle::occi::SQLException e) {
    castor::exception::SQLError ex;
    ex.getMessage() << "Database error, Oracle code: " << e.getErrorCode()
                    << std::endl << e.what();
    throw ex;
  }
}
