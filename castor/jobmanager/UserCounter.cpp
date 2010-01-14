/******************************************************************************
 *                      UserCounter.cpp
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
#include "castor/jobmanager/JobSubmissionRequest.hpp"
#include "castor/jobmanager/UserCounter.hpp"


//------------------------------------------------------------------------------
// instantiate
//------------------------------------------------------------------------------
castor::metrics::Counter* castor::jobmanager::UserCounter::instantiate(castor::IObject* obj)
{
  return new UserCounter(obj);
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::jobmanager::UserCounter::UserCounter(castor::IObject* obj) :
  castor::metrics::Counter("")
{
  castor::jobmanager::JobSubmissionRequest* r;
  r = dynamic_cast<castor::jobmanager::JobSubmissionRequest*>(obj);
  if (r == 0) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "This counter must be initialized with a valid JobSubmissionRequest object";
    throw ex;
  }

  m_name = r->username();
  m_value = 1;    // set the initial value
}

//------------------------------------------------------------------------------
// match
//------------------------------------------------------------------------------
int castor::jobmanager::UserCounter::match(castor::IObject* obj)
{
  castor::jobmanager::JobSubmissionRequest* r =
    dynamic_cast<castor::jobmanager::JobSubmissionRequest*>(obj);
  if(r != 0 && r->username() == m_name) {
      return 1;
  }
  return 0;
}

