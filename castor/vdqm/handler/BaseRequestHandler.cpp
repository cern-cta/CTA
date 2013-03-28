/******************************************************************************
 *                      BaseRequestHandler.cpp
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
 * @author Matthias Braeger
 *****************************************************************************/

#include <string>
#include <vector>
//#include "occi.h"

#include "castor/IObject.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/IService.hpp"

#include "castor/exception/Internal.hpp"

#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeDriveCompatibility.hpp"

#include "castor/vdqm/VdqmTape.hpp"
 
//Local Includes
#include "BaseRequestHandler.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::BaseRequestHandler::BaseRequestHandler() 
throw(castor::exception::Exception)
{
  castor::IService* svc = NULL;

  /**
   * The IVdqmService Objects has some important fuctions
   * to handle db queries.
   */
  
  try
  {
    /**
     * Getting DbVdqmSvc: It can be the OraVdqmSvc or the MyVdqmSvc
     */
    svc = services()->service("DbVdqmSvc", castor::SVC_DBVDQMSVC);

    if (0 == svc)
    {
      castor::exception::Internal ex;
      ex.getMessage() << "Could not get DbVdqmSvc" << std::endl;

      throw ex;
    }
  }
  catch(castor::exception::Exception &ex)
  {
    ex.getMessage() << "Could not get DbVdqmSvc" << std::endl;

    throw ex;
  }
  catch (...)
  {
    castor::exception::Internal ex;

    ex.getMessage() << "Could not get DbVdqmSvc"      << std::endl;
    ex.getMessage() << "Caught and unknown exception" << std::endl;

    throw ex;
  } 

  ptr_IVdqmService = dynamic_cast<IVdqmSvc*>(svc);

  if(0 == ptr_IVdqmService)
  {
    castor::exception::Internal ex;

    ex.getMessage() << "Got a bad DbVdqmSvc: "
      << "ID=" << svc->id()
      << ", Name=" << svc->name()
      << std::endl;

    throw ex;
  }
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::handler::BaseRequestHandler::~BaseRequestHandler() 
	throw() {
	//Reset the pointer
	ptr_IVdqmService = 0;
}
