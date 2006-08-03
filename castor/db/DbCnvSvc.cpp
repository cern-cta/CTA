/******************************************************************************
 *                      DbCnvSvc.cpp
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
 * @(#)$RCSfile: DbCnvSvc.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2006/08/03 12:40:26 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/IConverter.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/IObject.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/db/DbBaseObj.hpp"
#include "castor/exception/BadVersion.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include <iomanip>

// Local Files
#include "DbCnvSvc.hpp"


//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for type retrieval
const std::string castor::db::DbCnvSvc::s_getTypeStatementString =
  "SELECT type FROM Id2Type WHERE id = :1";

// -----------------------------------------------------------------------
// DbCnvSvc
// -----------------------------------------------------------------------
castor::db::DbCnvSvc::DbCnvSvc(const std::string name) :
  BaseCnvSvc(name),
  m_getTypeStatement(0) {
  // Add alias for DiskCopyForRecall on DiskCopy
  addAlias(58, 5);
  // Add alias for TapeCopyForMigration on TapeCopy
  addAlias(59, 30);  
}

// -----------------------------------------------------------------------
// ~DbCnvSvc
// -----------------------------------------------------------------------
castor::db::DbCnvSvc::~DbCnvSvc() throw() {}

// -----------------------------------------------------------------------
// repType
// -----------------------------------------------------------------------
const unsigned int castor::db::DbCnvSvc::repType() const {
  return RepType();
}

// -----------------------------------------------------------------------
// RepType
// -----------------------------------------------------------------------
const unsigned int castor::db::DbCnvSvc::RepType() {
  return castor::REP_DATABASE;
}

// -----------------------------------------------------------------------
// dropConnection
// -----------------------------------------------------------------------
void castor::db::DbCnvSvc::dropConnection () throw() {
  // make all registered converters aware
  for (std::set<castor::db::DbBaseObj*>::const_iterator it =
         m_registeredCnvs.begin();
       it != m_registeredCnvs.end();
       it++) {
    (*it)->reset();
  }
  delete m_getTypeStatement;
  m_getTypeStatement = 0;
  // child classes have to really drop the connection to the db
}


// -----------------------------------------------------------------------
// createObj
// -----------------------------------------------------------------------
castor::IObject* castor::db::DbCnvSvc::createObj(castor::IAddress* address)
  throw (castor::exception::Exception) {
  // If the address has no type, find it out
  if (OBJ_INVALID == address->objType()) {
    castor::BaseAddress* ad =
      dynamic_cast<castor::BaseAddress*>(address);
    unsigned int type = getTypeFromId(ad->target());
    if (0 == type) return 0;
    ad->setObjType(type);
  }
  // call method of parent object
  return this->BaseCnvSvc::createObj(address);
}

// -----------------------------------------------------------------------
// getTypeFromId
// -----------------------------------------------------------------------
const unsigned int
castor::db::DbCnvSvc::getTypeFromId(const u_signed64 id)
  throw (castor::exception::Exception) {
  // a null id has a null type
  if (0 == id) return 0;
  castor::db::IDbResultSet *rset;
  try {
    // Check whether the statement is ok
    if (0 == m_getTypeStatement) {
      m_getTypeStatement = createStatement(s_getTypeStatementString);
    }
    // Execute it
    m_getTypeStatement->setInt64(1, id);
    rset = m_getTypeStatement->executeQuery();
    if (!rset->next()) {
      delete rset;
      castor::exception::NoEntry ex;
      ex.getMessage() << "No type found for id : " << id;
      throw ex;
    }
    const unsigned int res = rset->getInt(1);
    delete rset;
    return res;
  } catch (castor::exception::SQLError e) {
    delete rset;
    rollback();
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Error in getting type from id."
                    << std::endl << e.getMessage();
    throw ex;
  }
  // never reached
  return OBJ_INVALID;
}

// -----------------------------------------------------------------------
// getObjFromId
// -----------------------------------------------------------------------
castor::IObject* castor::db::DbCnvSvc::getObjFromId(u_signed64 id)
  throw (castor::exception::Exception) {
  castor::BaseAddress clientAd;
  clientAd.setTarget(id);
  clientAd.setCnvSvcName("DbCnvSvc");
  clientAd.setCnvSvcType(repType());
  return createObj(&clientAd);
}

