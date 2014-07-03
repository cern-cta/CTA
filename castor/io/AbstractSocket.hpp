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
 * Defines a dedicated socket that handles most of the network calls
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#pragma once

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
       * Constructor building a Socket objet around a regular socket
       * @param socket the regular socket used
       */
      AbstractSocket(int socket) throw();

      /**
       * Constructor building a socket with no port. As a consequence,
       * the used port will be 0 and the socket will not be bound.
       * The bind method should be call independently
       * @param reusable whether the socket should be reusable
       * @exception Exception in case of error
       */
      AbstractSocket(const bool reusable) 
        ;

      /**
       * Constructor building a socket on a given local port
       * @param port the local port for this socket. Use 0 if
       * you want the system to allocate a port
       * @param reusable whether the socket should be reusable
       * @exception Exception in case of error
       */
      AbstractSocket(const unsigned short port,
                     const bool reusable)
        ;

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given by its name
       * @param reusable whether the socket should be reusable
       * @exception Exception in case of error
       */
      AbstractSocket(const unsigned short port,
                     const std::string host,
                     const bool reusable)
        ;

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param ip the host to connect to, given as an ip address
       * @param reusable whether the socket should be reusable
       * @exception Exception in case of error
       */
      AbstractSocket(const unsigned short port,
                     const unsigned long ip,
                     const bool reusable)
        ;

      /**
       * Destructor
       */
      virtual ~AbstractSocket() throw();

      /**
       * Sends an object on the socket
       * @param obj the IObject to send
       * @exception Exception in case of error
       */
      void sendObject(castor::IObject& obj)
        ;

      /**
       * Reads an object from the socket.
       * Note that the deallocation of it is the responsability of the caller.
       * This is a blocking call.
       * @return the IObject read
       * @exception Exception in case of error
       */
      virtual castor::IObject* readObject()
        ;

      /**
       * Check whether there is something to read on the socket
       * This is a non blocking call
       * @return whether data is available on the socket
       */
      bool isDataAvailable() throw();

      /**
       * Retrieve socket name
       */
      void getPortIp(unsigned short& port,
                     unsigned long& ip) const
        ;

      /**
       * Retrieve peer name
       */
      void getPeerIp(unsigned short& port,
                     unsigned long& ip) const
        ;

      /**
       * Sets the SO_REUSEADDR option on the socket
       */
      void setReusable() ;
      
      /**
       * Set the timeout value in seconds for reading and writing data
       * @param timeout the new timeout value
       */
      void setTimeout(const int timeout) {
        m_timeout = timeout;
      }

      /**
       * Returns the timeout value in seconds for reading and writing data
       */
      int timeout() const throw() {
        return m_timeout;
      }

      /**
       * Sets the timeout value in seconds for establishing new connections
       * @param timeout the new timeout value
       */
      void setConnTimeout(const int timeout) {
        m_connTimeout = timeout;
      }

      /**
       * Returns the timeout value in seconds for establishing new connections
       */
      int connTimeout() const throw() {
        return m_connTimeout;
      }

      /**
       * Closes the socket
       */
      void close() throw();

      /**
       * To be deleted when Socket class is complete XXX
       */
      int socket() {
        return m_socket;
      }

      /**
       * Resets the internal socket. Useful if this socket descriptor
       * has been cloned elsewhere and this instance needs to be deleted
       */
      void resetSocket() throw() {
        m_socket = -1;
      }

    protected:

      /**
       * Internal method to create the inner socket
       */
      virtual void createSocket()  = 0;

      /**
       * Builds an address for the local machine on the given port
       */
      struct sockaddr_in buildAddress(const unsigned short port)
        ;

      /**
       * Builds an address for a remote machine on the given port
       */
      struct sockaddr_in buildAddress(const unsigned short port,
                                      const std::string host)
        ;

      /**
       * Builds an address for a remote machine on the given port
       */
      struct sockaddr_in buildAddress(const unsigned short port,
                                      const unsigned long ip)
        ;

      /**
       * Internal method to send the content of a buffer
       * over the socket.
       * @param magic the magic number to be used as first
       * 4 bytes of the data sent
       * @param buf the data to send
       * @param n the length of buf
       * @exception Exception in case of error
       */
      virtual void sendBuffer(const unsigned int magic,
                              const char* buf,
                              const int n)
         = 0;

      /**
       * Internal method to read from a socket into a buffer.
       * @param magic the magic number expected as the first 4 bytes read.
       * An exception is sent if the correct magic number is not found
       * @param buf allocated by the method, it contains the data read.
       * Note that the deallocation of this buffer is the responsability
       * of the caller
       * @param n the length of the allocated buffer
       * @exception Exception in case of error
       */
      virtual void readBuffer(const unsigned int magic,
                              char** buf,
                              int& n)
         = 0;

    protected:

      /// The underlying socket
      int m_socket;

      /// The socket address
      struct sockaddr_in m_saddr;

      /// Flag to indicate whether the socket's address is reusable
      bool m_reusable;

      /// The timeout value in seconds for reading and writing data
      int m_timeout;

      /// The timeout value in seconds for establishing connections
      int m_connTimeout;

    };

  } // end of namespace io

} // end of namespace castor

