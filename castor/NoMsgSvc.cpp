/******************************************************************************
 *                      NoMsgSvc.cpp
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
 * @(#)$RCSfile: NoMsgSvc.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/11/18 16:54:04 $ $Author: sponcec3 $
 *
 * A message service writing nowhere
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/SvcFactory.hpp"

// Local Includes
#include "NoMsgSvc.hpp"

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::NoMsgSvc>* s_factoryNoMsgSvc =
  new castor::SvcFactory<castor::NoMsgSvc>();

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::NoMsgSvc::NoMsgSvc(const std::string name) throw() :
  MsgSvc(name) {}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::NoMsgSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::NoMsgSvc::ID() {
  return SVC_NOMSG;
}
