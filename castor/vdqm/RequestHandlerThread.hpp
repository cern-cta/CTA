/******************************************************************************
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

#pragma once

#include "castor/server/IThread.hpp"


namespace castor {

  namespace vdqm {

    /**
     * Handles the requests of the VDQM server's client.
     */
    class RequestHandlerThread : public castor::server::IThread {

    public:

      /**
       * Constructor
       */
      RequestHandlerThread() throw();

      /**
       * Destructor
       */
      ~RequestHandlerThread() throw();

      /**
       * Initialization of the thread.
       */
      virtual void init() throw();

      /**
       * Main work for this thread.
       */
      virtual void run(void *param) throw();

      /**
       * Convenience method to stop the thread.
       */
      virtual void stop() throw();

    private:

      /**
       * Handles the request with the specified cuuid on the specified socket.
       *
       * @param cuuid The cuuid of the request
       * @param sock The socket
       */
      void handleRequest(Cuuid_t &cuuid, castor::io::ServerSocket &sock)
        ;

    }; // class RequestHandlerThread

  } // end namespace vdqm

} //end namespace castor

