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
 *
 * Base class for all database oriented objects
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/dlf/Dlf.hpp"
#include "castor/IObject.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"

// Local Files
#include "DbBaseObj.hpp"
#include "DbCnvSvc.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::db::DbBaseObj::DbBaseObj(castor::ICnvSvc* service) :
  BaseObject(), m_cnvSvc(0), m_cnvSvcName("DbCnvSvc") {
  // if dynamic_cast returns 0, we go back to the default "DbCnvSvc" service
  m_cnvSvc = dynamic_cast<castor::db::DbCnvSvc*>(service);
}

// -----------------------------------------------------------------------
// initCnvSvc
// -----------------------------------------------------------------------
void castor::db::DbBaseObj::initCnvSvc()
  throw (castor::exception::Exception) {
  m_cnvSvc = dynamic_cast<castor::db::DbCnvSvc*>
    (svcs()->cnvService(m_cnvSvcName, SVC_DBCNV));
  if (!m_cnvSvc) {
    castor::exception::Exception ex;
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
  return cnvSvc()->createStatement(stmtString);
}

// -----------------------------------------------------------------------
// cnvSvc
// -----------------------------------------------------------------------
castor::db::DbCnvSvc* castor::db::DbBaseObj::cnvSvc()
  throw (castor::exception::Exception) {
  if (0 == m_cnvSvc) initCnvSvc();
  return m_cnvSvc;
}


// -----------------------------------------------------------------------
// reset
// -----------------------------------------------------------------------
void castor::db::DbBaseObj::reset() throw() {
  m_cnvSvc = 0;
}

// -----------------------------------------------------------------------
// commit
// -----------------------------------------------------------------------
void
castor::db::DbBaseObj::commit() {
  try {
    cnvSvc()->commit();
  } catch (castor::exception::Exception &ex) {
    // log the exception
    // 36 = "Call to commit() failed"
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR, 36, params);

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
    cnvSvc()->rollback();
  } catch (castor::exception::Exception &) {
    // rollback failed, let's reset the conversion service
    // (and thus drop the connection) for security
    cnvSvc()->reset();
  }
}

