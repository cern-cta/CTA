/******************************************************************************
 *                      VdqmServer.hpp
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
 * @(#)RCSfile: VdqmServer.hpp  Revision: 1.0  Release Date: Apr 8, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef RH_VDQMSERVER_HPP
#define RH_VDQMSERVER_HPP 1

#include "castor/BaseServer.hpp"
#include "castor/exception/Exception.hpp"

#define CSP_MSG_MAGIC 0xCA001
#define CSP_RHSERVER_PORT 9002
#define CSP_NOTIFICATION_PORT 9001

namespace castor {
	//Forward Declarations
	class IObject;

  namespace vdqm {

    /**
     * The Request Handler server. This is were client requests
     * arrive. The main task of this component is to store them
     * for future use
     */
    class VdqmServer : public castor::BaseServer {

    public:

      /**
       * Constructor
       */
      VdqmServer();

      /**
       * Method called once per request, where all the code resides
       */
      void *processRequest(void *param) throw();

      /**
       * Main Server loop, listening for the clients
       */
      int main();

      /**
       * Overriding start method for logging purposes
       */
      //virtual int start();

    private:

      /**
       * handles an incoming request
       * @param fr the request
       * @param cuuid its uuid (for logging purposes only)
       */
      void handleRequest(castor::IObject* fr, Cuuid_t cuuid)
        throw (castor::exception::Exception);
        
      /**
       * Internal function to handle the old Vdqm request. Puts the values into
       * the old structs and reads out the request typr number, to delegates
       * them to the right function.
       */
      void handleOldVdqmRequest();

    }; // class VdqmServer

  } // end of namespace vdqm

} // end of namespace castor

#endif // RH_VDQMSERVER_HPP
