/******************************************************************************
 *                      RHThread.hpp
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
 * @(#)$RCSfile: RHThread.hpp,v $ $Revision: 1.11 $ $Release$ $Date: 2008/11/03 09:29:40 $ $Author: sponcec3 $
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RH_RHTHREAD_HPP
#define RH_RHTHREAD_HPP 1

#include "castor/server/IThread.hpp"
#include "castor/stager/Request.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/BaseObject.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/io/AuthServerSocket.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/rh/IRHSvc.hpp"

namespace castor {

  namespace rh {

    /**
     * The Request Handler main thread.
     */
    class RHThread : public castor::server::IThread, public castor::BaseObject {

    public:

      /**
       * Constructor
       * @param useAccessLists whether to use access lists
       */
      RHThread(bool useAccessLists) throw (castor::exception::Exception);

      /**
       * Default destructor
       */
      ~RHThread() throw() {};

      /**
       * Method called once per request, where all the code resides
       * @param param the socket obtained from the calling thread pool
       */
      virtual void run(void *param);
      
      /// Not implemented
      virtual void init() {};

      /// Not implemented
      virtual void stop() {};
      
    private:
      
      /**
       * Handles an incoming request
       * @param fr the request
       * @param ad database conversion service
       * @param cuuid the uuid of the request (for logging purposes)
       * @throw Exception in case of error
       * @return the number of subrequests involved
       */
      unsigned int handleRequest(castor::stager::Request* fr, 
				 castor::BaseAddress ad,
				 Cuuid_t cuuid)
        throw (castor::exception::Exception);
      
      /**
       * Send a notification to stager to wake up one of its thread pools
       * to process the request
       * @param fr the request
       * @param cuuid the uuid of the request (for logging purposes)
       * @param nbThreads the number of threads to wake up on the stager
       */
      void sendNotification(castor::stager::Request* fr,
			    Cuuid_t cuuid,
			    unsigned int nbThreads);
      
    private:
      
      /// Whether to use access lists
      bool m_useAccessLists;
      
      /// Stager host
      std::string m_stagerHost;
      
      /// Stager notify port
      unsigned m_stagerPort;
      
      /// hash table for mapping requests to svc handlers
      std::map<int, std::string> m_svcHandler;
      
    }; // class RHThread
    
  } // end of namespace rh
  
} // end of namespace castor

#endif // RH_RHTHREAD_HPP
