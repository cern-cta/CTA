/******************************************************************************
 *                      Server.hpp
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
 * @(#)$RCSfile: Server.hpp,v $ $Revision: 1.1.1.1 $ $Release$ $Date: 2004/05/12 12:13:35 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RH_SERVER_HPP
#define RH_SERVER_HPP 1

#include "castor/BaseServer.hpp"

#define CSP_MSG_MAGIC 0xCA001
#define CSP_RHSERVER_PORT 9002
#define CSP_NOTIFICATION_PORT 9001

namespace castor {

  namespace rh {

    /**
     * The Request Handler server. This is were client requests
     * arrive. The main task of this component is to store them
     * for future use
     */
    class Server : public castor::BaseServer {

    public:

      /**
       * Constructor
       */
      Server();

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
       */
      void handleRequest(castor::IObject* fr) throw(Exception);

    }; // class Server

  } // end of namespace rh

} // end of namespace castor

#endif // RH_SERVER_HPP
