/******************************************************************************
 *                      OraBaseCnv.cpp
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
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/db/DbAddress.hpp"

// Local Files
#include "OraBaseCnv.hpp"
#include "OraCnvSvc.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::db::OraBaseCnv::OraBaseCnv() :
  BaseObject(),
  m_cnvSvc(0),
  m_connection(0) {
  m_cnvSvc = dynamic_cast<castor::db::OraCnvSvc*>
    (svcs()->cnvService("OraCnvSvc", SVC_ORACNV));
  if (m_cnvSvc) {
    try {
      m_connection = m_cnvSvc->getConnection();
    } catch (oracle::occi::SQLException e) {
      castor::Exception ex;
      ex.getMessage() << "Error trying to get a connection to ORACLE :"
                      << std::endl << e.what() << std::endl;
      throw ex;
    }
  } else {
    castor::Exception ex;
    ex.getMessage() << "No OraCnvSvc available"
                    << std::endl;
    throw ex;
  }
}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::db::OraBaseCnv::~OraBaseCnv() {
  m_cnvSvc->deleteConnection(m_connection);
  m_cnvSvc->release();
}

// -----------------------------------------------------------------------
// RepType
// -----------------------------------------------------------------------
const unsigned int castor::db::OraBaseCnv::RepType() {
  return castor::REP_ORACLE;
}

// -----------------------------------------------------------------------
// repType
// -----------------------------------------------------------------------
inline const unsigned int castor::db::OraBaseCnv::repType() const {
  return RepType();
}

// -----------------------------------------------------------------------
// createStatement
// -----------------------------------------------------------------------
oracle::occi::Statement*
castor::db::OraBaseCnv::createStatement (const std::string &stmtString)
  throw (castor::Exception) {
  if (0 == m_connection) {
    castor::Exception ex;
    ex.getMessage() << "Cannot create statement with no connection."
                    << std::endl;
    throw ex;
  };
  try {
    oracle::occi::Statement* stmt = m_connection->createStatement();
    stmt->setSQL(stmtString);
    return stmt;
  } catch (oracle::occi::SQLException e) {
    castor::Exception ex;
    ex.getMessage() << "Unable to create statement :" << std::endl
                    << stmtString << std::endl
                    << e.what() << std::endl;
    throw ex;
  }
  // This is never called
  return 0;
}

// -----------------------------------------------------------------------
// deleteStatement
// -----------------------------------------------------------------------
void castor::db::OraBaseCnv::deleteStatement(oracle::occi::Statement* stmt)
  throw (oracle::occi::SQLException) {
  m_connection->terminateStatement(stmt);
}

// -----------------------------------------------------------------------
// connection
// -----------------------------------------------------------------------
oracle::occi::Connection* castor::db::OraBaseCnv::connection() const {
  return m_connection;
}

// -----------------------------------------------------------------------
// cnvSvc
// -----------------------------------------------------------------------
castor::db::OraCnvSvc* castor::db::OraBaseCnv::cnvSvc() const {
  return m_cnvSvc;
}

// -----------------------------------------------------------------------
// getObjFromId
// -----------------------------------------------------------------------
castor::IObject* castor::db::OraBaseCnv::getObjFromId
(unsigned long id,
 castor::ObjectCatalog& newlyCreated)
  throw (castor::Exception) {
  if (newlyCreated.find(id) != newlyCreated.end()) {
    return newlyCreated[id];
  } else {
    castor::db::DbAddress clientAd(id, "OraCnvSvc", repType());
    try {
      return cnvSvc()->createObj(&clientAd, newlyCreated);
    } catch (castor::Exception e) {
      if (e.getMessage().str().compare(0, 13, "No type found") == 0) {
        return 0;
      } else {
        throw e;
      }
    }
  }
}
