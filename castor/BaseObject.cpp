/******************************************************************************
 *                      BaseObject.cpp
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
 * @(#)$RCSfile: BaseObject.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/05/19 16:37:14 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/MsgSvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "Cglobals.h"

// -----------------------------------------------------------------------
// msgSvc
// -----------------------------------------------------------------------
castor::MsgSvc* castor::BaseObject::msgSvc()
  throw (castor::exception::Exception) {
  IService* svc = svcs()->service("MsgSvc",
                                  castor::MsgSvc::ID());
  if (0 == svc) return 0;
  castor::MsgSvc* msgSvc = dynamic_cast<castor::MsgSvc*> (svc);
  if (0 == msgSvc) {
    svc->release();
    return 0;
  }
  return msgSvc;
}

//------------------------------------------------------------------------------
// svcs
//------------------------------------------------------------------------------
castor::Services* castor::BaseObject::svcs()
  throw (castor::exception::Exception) {
  void **tls;
  getTLS((void **)&tls);
  if (0 == *tls) {
    *tls = (void *)(new castor::Services());
  }
  return (castor::Services*) *tls;
};

//------------------------------------------------------------------------------
// getTLS
//------------------------------------------------------------------------------
void castor::BaseObject::getTLS(void **thip)
 throw (castor::exception::Exception) {
  static int Cns_api_key = -1;
  int rc = Cglobals_get (&Cns_api_key, (void **) thip, sizeof(void *));
  if (*thip == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() << "Could not get thread local storage";
    throw ex;
  }
  if (rc == 1) {
    *(void **)(*thip) = 0;
  }
}

//------------------------------------------------------------------------------
// clog
//------------------------------------------------------------------------------
castor::logstream& castor::BaseObject::clog()
 throw(castor::exception::Exception) {
  return msgSvc()->stream();
}
