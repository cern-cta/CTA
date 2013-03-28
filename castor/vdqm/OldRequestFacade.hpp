/******************************************************************************
 *                      OldRequestFacade.hpp
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
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _OLDREQUESTFACADE_HPP_
#define _OLDREQUESTFACADE_HPP_

//Include files
#include "castor/BaseObject.hpp"
#include "castor/exception/PermissionDenied.hpp"
#include "h/vdqm_messages.h"

#include <string>

namespace castor {

  namespace io {
    // Forward declaration
    class ServerSocket;
  }

  namespace vdqm {
    
    // Forward declaration
    class OldProtocolInterpreter;

    /**
     * This class provides functions to handle the old VDQM protocol.
     * It is used from the VdqmServer class.
     */
    class OldRequestFacade : public BaseObject {

      public:
       
        /**
         * Constructor
         *
         * @param volumeRequest The un-marshalled messsage body if the request
         * to be processed is a volume request.
         * @param driveRequest The un-marshalled messsage body if the request
         * to be processed is a drive request.
         * @param header The un-marshalled message header.
         */
        OldRequestFacade(vdqmVolReq_t *const volumeRequest,
          vdqmDrvReq_t *const driveRequest, vdqmHdr_t *const header,
          castor::io::ServerSocket &socket);
      
        /**
         * Calls the right function for the request.
         *
         * @param cuuid The unique id of the request. Needed for dlf.
         * @return true, if there were no complications
         */
        bool handleRequestType(OldProtocolInterpreter* oldProtInterpreter,
          const Cuuid_t cuuid) throw (castor::exception::Exception);
       
        /**
         * Throws an exception if the request type is invalid.
         *
         * @param cuuid The unique id of the request. Needed for dlf.
         */
        void checkRequestType(const Cuuid_t cuuid) 
          throw (castor::exception::Exception);

        /**
         * Destructor.
         */
        ~OldRequestFacade() throw ();
      
      
      private:
      
        vdqmVolReq_t             *const ptr_volumeRequest;
        vdqmDrvReq_t             *const ptr_driveRequest;
        vdqmHdr_t                *const ptr_header;
        castor::io::ServerSocket        &m_socket;
        const int                       m_reqtype;

        /**
         * Logs the reception of a drive request message.
         */
        void logDriveRequest(const vdqmHdr_t *const header,
          const vdqmDrvReq_t *const request, const Cuuid_t cuuid, int severity);

        /**
         * Logs the reception of a volume request message.
         */
        void logVolumeRequest(const vdqmHdr_t *const header,
          const vdqmVolReq_t *const request, const Cuuid_t cuuid, int severity);

    }; // class VdqmServer

  } // end of namespace vdqm

} // end of namespace castor

#endif //_OLDREQUESTFACADE_HPP_
