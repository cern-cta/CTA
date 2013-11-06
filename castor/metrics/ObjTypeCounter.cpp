/******************************************************************************
 *                      ObjTypeCounter.cpp
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
 * @(#)BaseClient.cpp,v 1.37 $Release$ 2006/02/16 15:56:58 sponcec3
 *
 *
 *
 * @author castor-dev team
 *****************************************************************************/

// Local includes
#include "castor/metrics/ObjTypeCounter.hpp"
#include "castor/Constants.hpp"

//------------------------------------------------------------------------------
// instantiate
//------------------------------------------------------------------------------
castor::metrics::Counter* castor::metrics::ObjTypeCounter::instantiate(castor::IObject* obj)
{
  return new ObjTypeCounter(obj);
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::metrics::ObjTypeCounter::ObjTypeCounter(castor::IObject* obj) :
  castor::metrics::Counter(ObjectsIdStrings[obj->type()]),
  m_type(obj->type())
{
  m_value = 1;    // set the initial value  
}

//------------------------------------------------------------------------------
// match
//------------------------------------------------------------------------------
inline int castor::metrics::ObjTypeCounter::match(castor::IObject* obj)
{
  return (obj->type() == m_type);
}
