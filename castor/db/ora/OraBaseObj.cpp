/******************************************************************************
 *                      OraBaseObj.cpp
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
#include "castor/Services.hpp"
#include "castor/db/DbAddress.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

// Local Files
#include "OraBaseObj.hpp"
#include "OraCnvSvc.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::db::ora::OraBaseObj::OraBaseObj(castor::ICnvSvc* cnvSvc) :
  BaseObject(), m_cnvSvc(0), m_ownsCnvSvc(false) {
  m_cnvSvc = dynamic_cast<castor::db::ora::OraCnvSvc*>(cnvSvc);
  if (0 == m_cnvSvc) {
    m_cnvSvc = dynamic_cast<castor::db::ora::OraCnvSvc*>
      (svcs()->cnvService("OraCnvSvc", SVC_ORACNV));
    if (!m_cnvSvc) {
      castor::exception::Internal ex;
      ex.getMessage() << "No OraCnvSvc available";
      throw ex;
    }
    m_ownsCnvSvc = true;
  }
}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::db::ora::OraBaseObj::~OraBaseObj() throw() {
  if (m_ownsCnvSvc) {
    m_cnvSvc->release();
  }
}

// -----------------------------------------------------------------------
// createStatement
// -----------------------------------------------------------------------
oracle::occi::Statement*
castor::db::ora::OraBaseObj::createStatement (const std::string &stmtString)
  throw (castor::exception::Exception) {
  try {
    oracle::occi::Statement* stmt =
      cnvSvc()->getConnection()->createStatement();
    stmt->setAutoCommit(false);
    stmt->setSQL(stmtString);
    return stmt;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to create statement :" << std::endl
                    << stmtString << std::endl
                    << e.what();
    throw ex;
  }
  // This is never called
  return 0;
}

// -----------------------------------------------------------------------
// deleteStatement
// -----------------------------------------------------------------------
void castor::db::ora::OraBaseObj::deleteStatement(oracle::occi::Statement* stmt)
  throw (oracle::occi::SQLException) {
  if (0 != stmt) {
    cnvSvc()->getConnection()->terminateStatement(stmt);
  }
}

// -----------------------------------------------------------------------
// cnvSvc
// -----------------------------------------------------------------------
castor::db::ora::OraCnvSvc* castor::db::ora::OraBaseObj::cnvSvc() const {
  return m_cnvSvc;
}
