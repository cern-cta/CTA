/******************************************************************************
 *                      DevTools.hpp
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
 * @author Castor dev team
 *****************************************************************************/

#ifndef CASTOR_VDQM_DEVTOOLS_HPP
#define CASTOR_VDQM_DEVTOOLS_HPP 1

#include <stdint.h>
#include <iostream>

#include "castor/exception/Exception.hpp"


namespace castor {

  namespace vdqm {
  	
    /**
     * Collection of static methods to faciliate the development of the VDQM.
     */
    class DevTools {
    public:

      /**
       * Prints the string form of specified IP using the specified output
       * stream.
       *
       * @param os the output stream.
       * @param ip the IP address in host byte order.
       */
      static void printIp(std::ostream &os, const unsigned long ip) throw();

      /**
       * Prints the magic number, request type, and IP information of the
       * specified message using the specified output stream.
       *
       * @param os the output stream.
       * @param messageWasSent should be set to true if the message was sent,
       * or to false the message was received.
       * @param messageInNetworkByteOrder should be set to true if the contents
       * of  the message is in network byte order, or to false if the contents
       * is in host byte order.
       * @param socket the socket used to send or receive the message.
       * @param hdrbuf the header buffer.
       */
      static void printMessage(std::ostream &os, const bool messageWasSent,
        const bool messageInNetworkByteOrder, const int socket, void* hdrbuf)
        throw (castor::exception::Exception);
    
    private:

      /**
       * Private constructor to prevent the instantiation of DevTool objects.
       */
      DevTools() throw();

    }; // class DevTools

  } // namespace vdqm

} // namespace castor


#endif // CASTOR_VDQM_DEVTOOLS_HPP
