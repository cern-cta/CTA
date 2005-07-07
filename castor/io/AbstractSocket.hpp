/******************************************************************************
 *                      Socket.hpp
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
 * @(#)$RCSfile: AbstractSocket.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/07/07 16:30:54 $ $Author: mbraeger $
 *
 * defines a dedicated socket that handles most of the network
 * calls
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ABSTRACT_SOCKET_HPP
#define CASTOR_ABSTRACT_SOCKET_HPP 1

// Include Files
#include <net.h>
#include <string>
#include <netinet/in.h>
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward declaration
  class IObject;

  namespace io {

    /**
     * A dedicated socket class, able to deal with socket manipulation
     * and to handle sending and receiving of IObjects
     */
    class AbstractSocket : public BaseObject {

    public:

      /**
       * Sends an object on the socket
       * @param obj the IObject to send
       */
      void sendObject(castor::IObject& obj)
        throw(castor::exception::Exception);

      /**
       * Reads an object from the socket.
       * Note that the deallocation of it is the responsability of the caller
       * @return the IObject read
       */
      virtual castor::IObject* readObject()
        throw(castor::exception::Exception);

      /**
       * Retrieve socket name
       */
      void getPortIp(unsigned short& port,
                     unsigned long& ip) const
        throw (castor::exception::Exception);

      /**
       * Retrieve peer name
       */
      void getPeerIp(unsigned short& port,
                     unsigned long& ip) const
        throw (castor::exception::Exception);


      /**
       * To be deleted when Socket class is complete XXX
       */
      int socket() {
        return m_socket;
      }


    protected:

      /**
       * internal method to create the inner socket
       */
      void createSocket() throw (castor::exception::Exception);

      /**
       * builds an address for the local machine on the given port
       */
      struct sockaddr_in buildAddress(const unsigned short port)
        throw (castor::exception::Exception);

      /**
       * builds an address for a remote machine on the given port
       */
      struct sockaddr_in buildAddress(const unsigned short port,
                                      const std::string host)
        throw (castor::exception::Exception);

      /**
       * builds an address for a remote machine on the given port
       */
      struct sockaddr_in buildAddress(const unsigned short port,
                                      const unsigned long ip)
        throw (castor::exception::Exception);

      /**
       * Internal method to send the content of a buffer
       * over the socket.
       * @param magic the magic number to be used as first
       * 4 bytes of the data sent
       * @param buf the data to send
       * @param n the length of buf
       */
      void sendBuffer(const unsigned int magic,
                      const char* buf,
                      const int n)
        throw (castor::exception::Exception);

      /**
       * Internal method to read from a socket into a buffer.
       * @param magic the magic number expected as the first 4 bytes read.
       * An exception is sent if the correct magic number is not found
       * @param buf allocated by the method, it contains the data read.
       * Note that the deallocation of this buffer is the responsability
       * of the caller
       * @param n the length of the allocated buffer
       */
      void readBuffer(const unsigned int magic,
                      char** buf,
                      int& n)
        throw (castor::exception::Exception);

    protected:

      /// the underlying socket
      int m_socket;
      struct sockaddr_in m_saddr;
    };

  } // end of namespace io

} // end of namespace castor

#endif // CASTOR_ABSTRACT_SOCKET_HPP
