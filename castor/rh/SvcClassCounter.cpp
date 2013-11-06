/******************************************************************************
 *                      SvcClassCounter.cpp
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
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Local includes
#include <errno.h>
#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/stager/Request.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/rh/SvcClassCounter.hpp"


//------------------------------------------------------------------------------
// instantiate
//------------------------------------------------------------------------------
castor::metrics::Counter* castor::rh::SvcClassCounter::instantiate(castor::IObject* obj)
{
  return new SvcClassCounter(obj);
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::SvcClassCounter::SvcClassCounter(castor::IObject* obj) :
  castor::metrics::Counter("")
{
  castor::stager::Request* r;
  r = dynamic_cast<castor::stager::Request*>(obj);
  if(r == 0) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "This counter must be initialized with a valid Request object";
    throw ex;
  }
  
  m_name = r->svcClassName();
  m_value = match(r);    // set the initial value 
}

//------------------------------------------------------------------------------
// match
//------------------------------------------------------------------------------
int castor::rh::SvcClassCounter::match(castor::IObject* obj)
{
  castor::stager::Request* r =
    dynamic_cast<castor::stager::Request*>(obj);
  if(r != 0 && r->svcClassName() == m_name) {
    castor::stager::FileRequest* fr = 
      dynamic_cast<castor::stager::FileRequest*>(r);
    if(fr != 0)
      return fr->subRequests().size();
    else
      return 1;
  }
  return 0;
}

