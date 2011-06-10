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
 * @(#)$RCSfile: DbCnvSvc.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2009/06/17 14:58:42 $ $Author: itglp $
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
#include "castor/VectorAddress.hpp"
#include "castor/exception/BadVersion.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include <iomanip>

// Local Files
#include "DbCnvSvc.hpp"


// -----------------------------------------------------------------------
// DbCnvSvc
// -----------------------------------------------------------------------
castor::db::DbCnvSvc::DbCnvSvc(const std::string name) :
  BaseCnvSvc(name) {
  // Add alias for DiskCopyForRecall on DiskCopy
  addAlias(58, 5);
  // Add alias for TapeCopyForMigration on TapeCopy
  addAlias(59, 30);
}

// -----------------------------------------------------------------------
// repType
// -----------------------------------------------------------------------
unsigned int castor::db::DbCnvSvc::repType() const {
  return RepType();
}

// -----------------------------------------------------------------------
// RepType
// -----------------------------------------------------------------------
unsigned int castor::db::DbCnvSvc::RepType() {
  return castor::REP_DATABASE;
}

// -----------------------------------------------------------------------
// reset
// -----------------------------------------------------------------------
void castor::db::DbCnvSvc::reset() throw() {
  // call parent's reset
  BaseCnvSvc::reset();
  // child classes have to really drop the connection to the db
}

// -----------------------------------------------------------------------
// getObjFromId
// -----------------------------------------------------------------------
castor::IObject* castor::db::DbCnvSvc::getObjFromId(u_signed64 id, unsigned objType)
  throw (castor::exception::Exception) {
  castor::BaseAddress clientAd;
  clientAd.setTarget(id);
  clientAd.setObjType(objType);
  clientAd.setCnvSvcName("DbCnvSvc");
  clientAd.setCnvSvcType(repType());
  return createObj(&clientAd);
}

// -----------------------------------------------------------------------
// getObjsFromIds
// -----------------------------------------------------------------------
std::vector<castor::IObject*> castor::db::DbCnvSvc::getObjsFromIds
(std::vector<u_signed64> &ids, unsigned objType)
  throw (castor::exception::Exception) {
  castor::VectorAddress clientAd;
  clientAd.setTarget(ids);
  clientAd.setObjType(objType);
  clientAd.setCnvSvcName("DbCnvSvc");
  clientAd.setCnvSvcType(repType());
  return bulkCreateObj(&clientAd);
}
