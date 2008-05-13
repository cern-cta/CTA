/******************************************************************************
 *                      VdqmMagic2ProtocolInterpreter.hpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_VDQM_VDQMMAGIC2PROTOCOLINTERPRETER_HPP
#define CASTOR_VDQM_VDQMMAGIC2PROTOCOLINTERPRETER_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/io/ServerSocket.hpp"
#include "h/vdqm_messages.h"


namespace castor {

  namespace vdqm {
    
    /**
     * This class provides the functions to send and receive messages through a 
     * socket using VDQM messages with the magic number VDQM_MAGIC2.
     */
    class VdqmMagic2ProtocolInterpreter {

    public:

      /**
       * Constructor.
       *
       * @param sock The Object, which includes the actual socket connection to
       * the client
       * @param cuuid the cuuid of the incoming request
       * @exception In case that one of the parameters is NULL
       */
      VdqmMagic2ProtocolInterpreter(castor::io::ServerSocket *const sock,
        const Cuuid_t *const cuuid)throw (castor::exception::Exception);

      /**
       * Reads the message header of VDQM message with a magic number of
       * VDQM_MAGIC2 from the socket of this protocol interpreter.  Please note
       * that this method assumes the magic number has already been read from
       * the socket.
       *
       * @param magic The already read out magic number which is to be copied
       * into the header ro make it complete
       * @param header Pointer to the memory which the message header should be
       * read out into
       */
      void castor::vdqm::VdqmMagic2ProtocolInterpreter::readHeader(
        const unsigned int magic, vdqmHdr_t *const header)
        throw(castor::exception::Exception);

      /**
       * Reads the message body of vdqmVolPriority message from the socket of
       * this protocol interpreter.  Please note that this method assumes the
       * message header has already been read from the socket.
       *
       * @param len The length of the message body
       * @param vdqmVolPriority Pointer to the memory which the message body
       * should be read out into
       */
      void castor::vdqm::VdqmMagic2ProtocolInterpreter::readVolPriority(
        const int len, vdqmVolPriority_t *const vdqmVolPriority)
        throw(castor::exception::Exception);


    private:

      /**
       * The object which includes the socket connection to the client
       */
      castor::io::ServerSocket *const m_sock;

      /**
       * The cuuid of the request
       */
      const Cuuid_t *m_cuuid;

    }; // class VdqmMagic2ProtocolInterpreter

  } // namespace vdqm

} // namespace castor      

#endif // CASTOR_VDQM_VDQMMAGIC2PROTOCOLINTERPRETER_HPP
