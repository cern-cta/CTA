/******************************************************************************
 *                      BaseSvc.cpp
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
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Local Files
#include "Services.hpp"
#include "BaseSvc.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::BaseSvc::BaseSvc(const std::string name) throw() :
  BaseObject(),
  m_name(name) {}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::BaseSvc::~BaseSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// reset
// -----------------------------------------------------------------------
void castor::BaseSvc::reset() throw() {
  // Call reset on all dependent services
  for (std::set<castor::IService*>::const_iterator it = m_depSvcs.begin();
       it != m_depSvcs.end();
       it++) {
    (*it)->reset();
  }
}
