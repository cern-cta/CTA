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
 * @(#)$RCSfile: BaseSvc.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/12/14 16:10:38 $ $Author: sponcec3 $
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
  m_name(name),
  m_refs(1) {}

// -----------------------------------------------------------------------
// release
// -----------------------------------------------------------------------
void castor::BaseSvc::release() throw() {
  m_refs--;
  // Never release services. This can lead to memory leaks
  // in very specific contexts but is an optimisation most
  // of the time. Otherwise, services are constantly created
  // and released.
//   if (0 == m_refs) {
//     svcs()->removeService(name());
//     delete this;
//   }
}
