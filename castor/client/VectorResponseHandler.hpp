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
 * A simple Response Handler for command line clients
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#pragma once

// Include Files
#include "castor/exception/Exception.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/BaseObject.hpp"
#include <vector>

namespace castor {

  namespace rh {
    // Forward declaration
    class Response;
  }

  namespace client {
    

      /**
       * Multiple files response handler
       */
      class VectorResponseHandler : public castor::client::IResponseHandler {
        
      public:

        VectorResponseHandler(std::vector<castor::rh::Response *> *vector)
          ;

        virtual void handleResponse(castor::rh::Response& r)
          ;
        
        virtual void terminate()
           {
        };


      private:
        std::vector<castor::rh::Response *> *m_responses;
      };


    } // end of namespace client

} // end of namespace castor

