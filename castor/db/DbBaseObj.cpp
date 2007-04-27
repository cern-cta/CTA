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
 * @(#)$RCSfile: DbBaseObj.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2007/04/27 10:22:24 $ $Author: itglp $
 *
 * Base class for all database oriented objects
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
    initCnvSvc("DbCnvSvc");
  }
  m_cnvSvc->registerDbObj(this);
}

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::db::DbBaseObj::DbBaseObj(std::string serviceName) :
  BaseObject(), m_cnvSvc(0) {
  initCnvSvc(serviceName);
  m_cnvSvc->registerDbObj(this);
}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::db::DbBaseObj::~DbBaseObj() throw() {
  m_cnvSvc->unregisterDbObj(this);
}

// -----------------------------------------------------------------------
// initCnvSvc
// -----------------------------------------------------------------------
void castor::db::DbBaseObj::initCnvSvc(std::string serviceName)
  throw (castor::exception::Exception) {
  m_cnvSvc = dynamic_cast<castor::db::DbCnvSvc*>
    (svcs()->cnvService(serviceName, SVC_DBCNV));
  if (!m_cnvSvc) {
    castor::exception::Internal ex;
    ex.getMessage() << "No DbCnvSvc available";
    throw ex;
  }
}


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

