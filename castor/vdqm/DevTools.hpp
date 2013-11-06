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

#include "castor/exception/Exception.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"

#include <stdint.h>
#include <iostream>


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
       * Prints a textual description of the specified socket to the specified
       * output stream.
       *
       * @param os the output stream to which the string is to be printed.
       * @param the socket whose textual description is to be printed to the
       * stream.
       */
      static void printSocketDescription(std::ostream &os,
        castor::io::ServerSocket &socket);

      /**
       * Prints the magic number, request type, and IP information of the
       * specified message using the specified output stream.
       *
       * WARNING
       * This method relies on the buffer pointed to by hdrbuf containing at
       * lease the magic number, request type and len of the message.  This
       * message also relies on the buffer containing a complete message
       * (header + body) in the case of a VDQM_CLIENTINFO message being sent to
       * rtcpd.
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

      /**
       * Prints the string form of the specified tape drive status bitset using
       * the specified output stream.
       *
       * @param os the output stream to which the string form should be written
       * to.
       * @param bitset the status of a tape drive specified as a bitset.  This
       * is the bitset used by the VDQM messages and is not an internal tape
       * drive status value of the VDQM2
       */
      static void printTapeDriveStatusBitset(std::ostream &os, int bitset);

      /**
       * Returns the string form of the specified tape drive status.
       *
       * @param status the tape drive status
       */
      static const char *tapeDriveStatus2Str(
        const TapeDriveStatusCodes status);

      /**
       * Returns the string form of the specified CASTOR-message magic-number.
       *
       * @param magic the CASTOR-message magic-number.
       */
      static const char *castorMagicNb2Str(const uint32_t magic);

      /**
       * Returns the string form of the specified RTCP-message request-type.
       *
       * @param type the RTCP-message request-type.
       */
      static const char *rtcpReqTypeToStr(const uint32_t type);

      /**
       * Returns the string form of the specified VDQM-message request-type.
       *
       * @param type the VDQM-message request-type.
       */
      static const char *vdqmReqTypeToStr(const uint32_t type);

      /**
       * Returns the string form of the specified CASTOR-message request-type.
       *
       * @param type the CASTOR-message request-type.
       */
      static const char *castorReqTypeToStr(const uint32_t magic,
        const uint32_t type);
    
    private:

      /**
       * Private constructor to prevent the instantiation of DevTool objects.
       */
      DevTools() throw();

      /**
       * The element type of the map used to print out the textual
       * representation of a drive unit status bitset.
       */
      struct UnitMaskAndStr {
        unsigned int mask;
        const char   *str;
      };

      /**
       * Map used to print out the textual representation of a drive unit
       * status bitset.
       */
      static UnitMaskAndStr unitStatusTypes_[];

    }; // class DevTools

  } // namespace vdqm

} // namespace castor


#endif // CASTOR_VDQM_DEVTOOLS_HPP
