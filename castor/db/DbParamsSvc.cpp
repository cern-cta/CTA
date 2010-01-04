/******************************************************************************
 *                      DbParamsSvc.cpp
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
 * @(#)$RCSfile: DbParamsSvc.cpp,v $ $Revision: 1.23 $ $Release$ $Date: 2009/05/19 08:26:50 $ $Author: gtaur $
 *  
 * A service to provide parameters to access the db layer of a Castor application
 *
 * @author castor dev team
 *****************************************************************************/

// Local Files
#include "castor/db/DbParamsSvc.hpp"
#include "castor/SvcFactory.hpp"

// Hardcoded default, valid for all Castor specific daemons. Other
// applications (e.g. Repack) must override their DbParamsSvc instance
// and provide their versioning.
namespace castor {
  namespace db {
    const std::string STAGERSCHEMAVERSION = "2_1_9_4";
  }
}

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::DbParamsSvc>* s_factoryDbParamsSvc =
  new castor::SvcFactory<castor::db::DbParamsSvc>();


// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::db::DbParamsSvc::DbParamsSvc(const std::string name) throw() :
  BaseSvc(name),
  m_schemaVersion(castor::db::STAGERSCHEMAVERSION) 
  {}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::DbParamsSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::DbParamsSvc::ID() {
  return castor::SVC_DBPARAMSSVC;
}
