/******************************************************************************
 *                      RTCopyDConnection.hpp
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
 * @(#)RCSfile: RTCopyDConnection.hpp  Revision: 1.0  Release Date: Jul 29, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/
#ifndef _RTCOPYDCONNECTION_HPP_
#define _RTCOPYDCONNECTION_HPP_

// Include Files
#include <string>

#include "castor/io/AbstractSocket.hpp"


namespace castor {

  namespace vdqm {
  	
  	//Forward declaration
  	class TapeDrive;

    /**
     * A dedicated socket class, able to deal with socket manipulation
     * and to handle sending of messages to the RTCopy daemon.
     */
    class RTCopyDConnection : public castor::io::AbstractSocket {

    public:

      /**
       * Constructor building a Socket objet around a regular socket
       * @param socket the regular socket used
       */
      RTCopyDConnection(int socket) throw ();

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given by its name
       */
      RTCopyDConnection(const unsigned short port,
             const std::string host)
        throw (castor::exception::Exception);

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given as an ip address
       */
      RTCopyDConnection(const unsigned short port,
             const unsigned long ip)
        throw (castor::exception::Exception);


      /**
       * destructor
       */
      ~RTCopyDConnection() throw();
      
      
      /**
       * connects the socket to the given address
       */
       virtual void connect()
				 throw (castor::exception::Exception);      
      
      
      /**
       * This function is used to inform the RT Copy daemon about an actual
       * free tape, which can be dedidacet to a waiting TapeRequest. Please
       * make sure, that you have already realized the connection to RTCP!
       * 
       * @param tapeDrive The TapeDrive which can handle the TapeRequest. Please
       * make sure, that the TapeRequest is already included in the TaepDrive!
       * @exception In case of errors 
       */
      bool sendJobToRTCPD(TapeDrive *tapeDrive) 
        throw (castor::exception::Exception);    
        
      
    private:
      
      /**
       * This is a helper function for sendJobToRTCP(), to read
       * the answer of RTCP
       * 
       * @return false, in case of problems on RTCP side
       * @exception In case of errors 
       */
      bool readRTCPAnswer(TapeDrive *tapeDrive)
	     throw (castor::exception::Exception);    
    };

  } // end of namespace vdqm

} // end of namespace castor      

#endif //_RTCOPYDCONNECTION_HPP_
