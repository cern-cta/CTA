/******************************************************************************
 *                      castor/stager/Request.hpp
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
 * @(#)$RCSfile: RequestHelper.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/01/20 13:53:03 $ $Author: bcouturi $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_REQUEST_HELPER_HPP
#define CASTOR_STAGER_REQUEST_HELPER_HPP

// Include Files
#include "castor/stager/Request.hpp"
#include "stager_client_api.h"

namespace castor {

  namespace stager {

    /**
     * Name of the Service class environment variables
     */
    extern const char* SVCCLASS_ENV;
    extern const char* SVCCLASS_ENV_ALT;


    /**
     * class Requesthelper
     * Utility class to ease the manipulations of Requests
     */
    class RequestHelper  {

    public:

      
      /**
       * Empty Constructor
       */
      RequestHelper(castor::stager::Request* r) throw();


      /**
       * Check and set the options
       */
      void setOptions(struct stage_options* opts);

      /**
       * Empty Destructor
       */
      virtual ~RequestHelper() throw();

    private:
      
      /**
       * The referenced request
       */
      castor::stager::Request* m_request;

    };

  }
}

#endif // CASTOR_STAGER_REQUEST_HELPER_HPP
