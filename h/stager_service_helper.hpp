/******************************************************************************
 *                      stager_service_helper.hpp
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
 * @(#)$RCSfile: stager_service_helper.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/08/18 16:00:13 $ $Author: sponcec3 $
 *
 * Just a little helper function replying to a client
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef H_STAGER_SERVICE_HELPER_HPP 
#define H_STAGER_SERVICE_HELPER_HPP 1

#include <string>

namespace castor {

  // Forward Declarations
  class IClient;

  namespace rh {

    // Forward Declarations
    class Response;

  }
  
  namespace stager {

    /**
     * Sends a Response to a client
     * In case of error, on writes a message to the log
     * @param client the client where to send the response
     * @param res the response to send
     * @param reqId the uuid of the correponding request
     */
    void replyToClient(castor::IClient* client,
                       castor::rh::Response* res,
                       std::string reqId);
      
  } // End of namespace stager

} // End of namespace castor

#endif // H_STAGER_SERVICE_HELPER_HPP
