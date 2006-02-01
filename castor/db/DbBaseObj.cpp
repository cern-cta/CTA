/******************************************************************************
 *                      DbBaseObj.cpp
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
 * @(#)$RCSfile: DbBaseObj.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2006/02/01 10:22:33 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

// Local Files
#include "DbBaseObj.hpp"
#include "DbCnvSvc.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::db::DbBaseObj::DbBaseObj(castor::ICnvSvc* cnvSvc) :
  BaseObject(), m_cnvSvc(0) {
  m_cnvSvc = dynamic_cast<castor::db::DbCnvSvc*>(cnvSvc);
  if (0 == m_cnvSvc) {
    m_cnvSvc = dynamic_cast<castor::db::DbCnvSvc*>
      (svcs()->cnvService("DbCnvSvc", SVC_DBCNV));
    if (!m_cnvSvc) {
      castor::exception::Internal ex;
      ex.getMessage() << "No DbCnvSvc available";
      throw ex;
    }
  }
}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::db::DbBaseObj::~DbBaseObj() throw() {}

// -----------------------------------------------------------------------
// createStatement
// -----------------------------------------------------------------------
castor::db::IDbStatement*
castor::db::DbBaseObj::createStatement(const std::string &stmtString)
  throw (castor::exception::Exception) {
  return m_cnvSvc->createStatement(stmtString);
}

// -----------------------------------------------------------------------
// cnvSvc
// -----------------------------------------------------------------------
castor::db::DbCnvSvc* castor::db::DbBaseObj::cnvSvc() const {
  return m_cnvSvc;
}


// -----------------------------------------------------------------------
// commit
// -----------------------------------------------------------------------
void
castor::db::DbBaseObj::commit() {
	try {
	  m_cnvSvc->commit();
	} catch (castor::exception::Exception) {
	  // commit failed, let's rollback for security
	  rollback();
	}
}

// -----------------------------------------------------------------------
// rollback
// -----------------------------------------------------------------------
void
castor::db::DbBaseObj::rollback() {
  try {
    m_cnvSvc->rollback();
  } catch (castor::exception::Exception) {
    // rollback failed, let's drop the connection for security
    m_cnvSvc->dropConnection();
  }
}

