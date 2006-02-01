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
 * @(#)$RCSfile: RHThread.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/02/01 17:12:33 $ $Author: itglp $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RH_RHTHREAD_HPP
#define RH_RHTHREAD_HPP 1

#include "castor/server/IThread.hpp"
#include "castor/BaseObject.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace rh {

    /**
     * The Request Handler main thread.
     */
    class RHThread : public castor::server::IThread, public castor::BaseObject {

    public:

      /**
       * Method called once per request, where all the code resides
       * @param param the socket obtained from the calling thread pool
       */
      virtual void run(void *param) throw();
      
      /// not implemented
      virtual void stop() {};
      
    private:
      
      /**
       * handles an incoming request
       * @param fr the request
       * @param cuuid its uuid (for logging purposes only)
       */
      void handleRequest(castor::IObject* fr, Cuuid_t cuuid)
        throw (castor::exception::Exception);

    }; // class RHThread

  } // end of namespace rh

} // end of namespace castor

#endif // RH_RHTHREAD_HPP
