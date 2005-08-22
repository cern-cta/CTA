/******************************************************************************
 *                      DbBaseCnv.cpp
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
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/exception/Internal.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/Constants.hpp"
#include "castor/IObject.hpp"
#include "DbBaseCnv.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::db::cnv::DbBaseCnv::DbBaseCnv(castor::ICnvSvc* cs) :
  DbBaseObj(cs) {
  cnvSvc()->registerCnv(this);
}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::db::cnv::DbBaseCnv::~DbBaseCnv() throw() {
  cnvSvc()->unregisterCnv(this);
}

// -----------------------------------------------------------------------
// repType
// -----------------------------------------------------------------------
inline const unsigned int castor::db::cnv::DbBaseCnv::repType() const {
  return cnvSvc()->repType();
}

// -----------------------------------------------------------------------
// RepType
// -----------------------------------------------------------------------
const unsigned int castor::db::cnv::DbBaseCnv::RepType() {
  return castor::REP_DATABASE;
}

