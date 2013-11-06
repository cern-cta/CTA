/******************************************************************************
*                castor/stager/daemon/StageRequestSvcThread.hpp
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
* Service thread for handling stager specific requests
*
* @author castor dev team
*****************************************************************************/

#ifndef STAGER_DAEMON_STAGEREQUESTSVCTHREAD_HPP
#define STAGER_DAEMON_STAGEREQUESTSVCTHREAD_HPP 1


#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"
#include "castor/stager/daemon/BaseRequestSvcThread.hpp"


namespace castor {
  
  namespace stager {
    
    namespace daemon {
      
      
      class StageRequestSvcThread : public virtual BaseRequestSvcThread {
        
      public:
      
        StageRequestSvcThread() throw();
        ~StageRequestSvcThread() throw() {};
        
        virtual void process(castor::IObject* subRequestToProcess) throw();
       
      };
      
    } // end namespace daemon
    
  } // end namespace stager
  
} // end namespace castor

#endif
