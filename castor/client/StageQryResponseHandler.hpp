/******************************************************************************
 *                      StageQryResponseHandler.hpp
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
 * @(#)$RCSfile: StageQryResponseHandler.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/11/05 17:47:20 $ $Author: sponcec3 $
 *
 * A response handler dedicated to stage queries
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CLIENT_STAGEQRYRESPONSEHANDLER_HPP 
#define CLIENT_STAGEQRYRESPONSEHANDLER_HPP 1

// Include Files
#include "castor/exception/Exception.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/BaseObject.hpp"

namespace castor {

  namespace rh {
    // Forward declaration
    class Response;
  }

  namespace client {

    /**
     * A Response Handler for StageQry clients
     */
    class StageQryResponseHandler : public IResponseHandler, BaseObject {

    public:

      /**
       * handles a response when one arrives
       * @param r the response to handle
       */
      virtual void handleResponse(castor::rh::Response& r)
        throw (castor::exception::Exception);

      /**
       * terminates the response handler. This is called
       * when all responses were received.
       */
      virtual void terminate()
        throw (castor::exception::Exception) {};

    };

  } // end of namespace client

} // end of namespace castor

#endif // CLIENT_STAGEQRYRESPONSEHANDLER_HPP
