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
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RH_RHTHREAD_HPP
#define RH_RHTHREAD_HPP 1

#include "castor/stager/Request.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/io/AuthServerSocket.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/rh/IRHSvc.hpp"
#include "castor/rh/RateLimiter.hpp"
#include "castor/BaseObject.hpp"
#include "castor/server/IThread.hpp"

namespace castor {

  namespace rh {

    /**
     * The Request Handler main thread.
     */
    class RHThread : public virtual castor::server::IThread,
                     public castor::BaseObject {

    public:

      /**
       * Constructor
       */
      RHThread() throw (castor::exception::Exception);

      /**
       * Destructor. See the cpp file for details.
       */
      virtual ~RHThread() throw();

      /**
       * Init method
       */
      virtual void init();

      /**
       * Stop method
       */
      virtual void stop();

      /**
       * Method called once per request, where all the code resides
       * @param param the socket obtained from the listener thread
       */
      virtual void run(void *param);

    private:
      
      /**
       * small function that creates a thread-specific storage key
       * for the rate limiter
       */
      static void makeRateLimiterKey() throw (castor::exception::Exception);

      /**
       * Return a RateLimiter object from thread local storage
       * @throw Exception in case of error
       * @return a RateLimiter object
       */
      castor::rh::RateLimiter *getRateLimiterFromTLS()
        throw (castor::exception::Exception);

      /**
       * Handles an incoming request
       * @param fr the request
       * @throw Exception in case of error
       * @return the number of subrequests involved
       */
      unsigned int handleRequest(castor::stager::Request* fr)
        throw (castor::exception::Exception);

      /// Stager host
      std::string m_stagerHost;

      /// Stager notify port
      unsigned m_stagerPort;

      /// Hash table for mapping requests to svc handlers
      std::map<int, std::string> m_svcHandler;

      /// List of trusted SRM hosts
      std::vector<std::string> m_srmHostList;

      /**
       * The key to thread-specific storage for the RateLimiter
       */
      static pthread_key_t s_rateLimiterKey;
      
      /**
       * The key for creating only once the thread-specific storage
       * key for the RateLimiter
       */
      static pthread_once_t s_rateLimiterOnce;

    }; // class RHThread

  } // end of namespace rh

} // end of namespace castor

#endif // RH_RHTHREAD_HPP
