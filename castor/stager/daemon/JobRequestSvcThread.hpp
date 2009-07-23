/******************************************************************************
*                castor/stager/daemon/JobRequestSvcThread.hpp
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
* @(#)$RCSfile: JobRequestSvcThread.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2009/07/23 12:22:00 $ $Author: waldron $
*
* Service thread for handling Job oriented requests
*
* @author castor dev team
*****************************************************************************/

#ifndef STAGER_DAEMON_JOBREQUESTSVCTHREAD_HPP
#define STAGER_DAEMON_JOBREQUESTSVCTHREAD_HPP 1

#include "castor/stager/daemon/BaseRequestSvcThread.hpp"
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"


namespace castor {

  namespace stager {

    namespace daemon {
      
      class JobRequestSvcThread : public BaseRequestSvcThread {
        
      public: 
        JobRequestSvcThread() throw (castor::exception::Exception);
        ~JobRequestSvcThread() throw() {};
        
        virtual void process(castor::IObject* subRequestToProcess) throw();
        
        /// jobmanager daemon host and port
        std::string m_jobManagerHost;
        unsigned m_jobManagerPort;
      };
      
    }// end namespace daemon
    
  } // end namespace stager
  
} //end namespace castor

#endif
