/******************************************************************************
 *                castor/stager/daemon/BaseRequestSvcThread.hpp
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
 * @(#)$RCSfile: BaseRequestSvcThread.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/12/14 16:45:45 $ $Author: itglp $
 *
 * Base service thread for handling stager requests
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef STAGER_DAEMON_BASEREQUESTSVCTHREAD_HPP
#define STAGER_DAEMON_BASEREQUESTSVCTHREAD_HPP 1

#include "serrno.h"
#include <errno.h>
#include <string>
#include <iostream>

#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/server/SelectProcessThread.hpp"
#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"


namespace castor {
  namespace stager{
    namespace dbService {
      
      class BaseRequestSvcThread : public castor::server::SelectProcessThread {
        
      public: 

        BaseRequestSvcThread(std::string name, std::string dbServiceName, int dbServiceType) throw() :
          SelectProcessThread(), m_name(name), 
          m_dbServiceName(dbServiceName), m_dbServiceType(dbServiceType) {};
        virtual ~BaseRequestSvcThread() throw() {};
        
        /***************************************************************************/
        /* abstract functions inherited from the SelectProcessThread to implement */
        /*************************************************************************/
        virtual castor::IObject* select() throw();
        
        void handleException(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, int errorCode, std::string errorMessage) throw();
        
      protected:
        std::string m_name;
        std::string m_dbServiceName;
        int m_dbServiceType;
        
        int typeRequest;

      };// end class BaseRequestSvcThread
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
