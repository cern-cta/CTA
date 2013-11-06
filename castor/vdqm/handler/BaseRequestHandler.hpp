/******************************************************************************
 *                      BaseRequestHandler.hpp
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
 * @author castor dev team
 *****************************************************************************/

#ifndef _BASEREQUESTHANDLER_HPP_
#define _BASEREQUESTHANDLER_HPP_

#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"

#include "errno.h"
#include "serrno.h"

namespace castor {
  //Forward declaration
  class IObject;
  class Services;
  
  namespace vdqm {
    
    namespace handler {
      /**
       * The BaseRequestHandler provides a set of useful functions for
       * concrete request instances.
       */
      class BaseRequestHandler : public castor::BaseObject {
  
      public:
        
        /**
         * Constructor
         */
        BaseRequestHandler() throw(castor::exception::Exception);
        
        /**
         * Destructor
         */
        virtual ~BaseRequestHandler() throw();
            
            
      protected:
        castor::vdqm::IVdqmSvc* ptr_IVdqmService;
        
      }; // class BaseRequestHandler
    
    } // namespace handler
    
  } // namespace vdqm

} // namespace castor

#endif //_BASEREQUESTHANDLER_HPP_
