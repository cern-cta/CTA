/******************************************************************************
 *                      VdqmServerSocket.cpp
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
 * @(#)RCSfile: VdqmServerSocket.cpp  Revision: 1.0  Release Date: Apr 12, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include <net.h>
#include <netdb.h>
#include <errno.h>
#include <serrno.h>
#include <unistd.h> // for close()
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>

#include "castor/exception/Internal.hpp"
//#include "castor/io/biniostream.h"

#include <vdqm_constants.h>
#include "osdep.h" //for LONGSIZE
#include "Cnetdb.h"
#include <common.h>

// Local Includes
#include "VdqmServerSocket.hpp"
#include "vdqmMacros.h"  // Needed for marshalling

// definition of some constants
#define STG_CALLBACK_BACKLOG 2
#define VDQMSERV 1

 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmServerSocket::VdqmServerSocket(int socket) throw () :
  m_listening(false) {
  	
  m_socket = socket;
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmServerSocket::VdqmServerSocket(const unsigned short port,
                                       const bool reusable)
  throw (castor::exception::Exception) :
  m_listening(false) {
  	
  m_socket = 0;
  createSocket();
  if (reusable) this->reusable();
  m_saddr = buildAddress(port);
  bind(m_saddr);
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmServerSocket::~VdqmServerSocket() throw () {
	close(m_socket);
}



//------------------------------------------------------------------------------
// Sets the socket to Reusable address
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServerSocket::reusable()
  throw (castor::exception::Exception) {

  int on = 1;
	if (setsockopt (m_socket, SOL_SOCKET, SO_REUSEADDR, 
                  (char *)&on, sizeof(on)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to set socket to reusable";
    throw ex;    
  }
}


//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServerSocket::bind(sockaddr_in saddr)
  throw (castor::exception::Exception) {
  // Binds the socket
  if (::bind(m_socket, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to bind socket";
    close(m_socket);
    throw ex;
  }
}


//------------------------------------------------------------------------------
// readMagicNumber
//------------------------------------------------------------------------------
unsigned int castor::vdqm::VdqmServerSocket::readMagicNumber()
	throw (castor::exception::Exception) {
  
	char buffer[sizeof(unsigned int)];
	char *p;
//	std::string message;
  int ret;
  unsigned int magic;
  
  // Read the magic number from the socket
  ret = netread(m_socket, buffer, sizeof(unsigned int));
                    
  if (ret != sizeof(unsigned int)) {
    if (0 == ret) {
      castor::exception::Internal ex;
      ex.getMessage() << "VdqmServerSocket::readMagicNumber(): "
      								<< "Unable to receive Magic Number" << std::endl
                      << "The connection was closed by remote end";
      throw ex;
    } else if (-1 == ret) {
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "VdqmServerSocket::readMagicNumber(): "
      								<< "Unable to receive Magic Number: " 
      								<< errno << " - " << strerror(errno) << std::endl;
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage() << "VdqmServerSocket::readMagicNumber(): "
      								<< "Received Magic Number is too short : only "
                      << ret << " bytes";
      throw ex;
    }
  }
  
  
  /**
   * This is code for the C++ world, which is not compatible with the
   * C Marshalling of the magic number.
   */
//  message = std::string(buffer, sizeof(unsigned int));
//  castor::io::biniostream rcvStream(message);
//	castor::io::biniostream& addrPointer = rcvStream;
//	
//	/**
//	 * Do the unmarshalling of the message
//	 */
//  addrPointer >> magic;

	p = buffer;
	DO_MARSHALL(LONG, p, magic, ReceiveFrom);
  
  return magic;
}

//------------------------------------------------------------------------------
// listen
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServerSocket::listen()
  throw(castor::exception::Exception) {
  if (::listen(m_socket, STG_CALLBACK_BACKLOG) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to listen on socket";
    (void) close(m_socket);
    m_socket = 0;
    throw ex;
  }
  m_listening = true;
}


//------------------------------------------------------------------------------
// accept
//------------------------------------------------------------------------------
castor::vdqm::VdqmServerSocket* castor::vdqm::VdqmServerSocket::accept()
  throw(castor::exception::Exception) {
  // Check if listen was called, if not, call it
  if (!m_listening) {
    listen();
  }
  // loop until we really get something
  for (;;) {
    struct sockaddr_in saddr;
    memset(&saddr, 0, sizeof(saddr));
    int fromlen = sizeof(saddr);
    int fdc = ::accept(m_socket,
                       (struct sockaddr *) &saddr,
                       (socklen_t *)(&fromlen));
    if (fdc == -1) {
      if (errno == EINTR) {
        continue;
      } else {
        castor::exception::Exception ex(errno);
        ex.getMessage() << "Error in accepting on socket";
        throw ex;
      }
    }
    return new VdqmServerSocket(fdc);
  }
}


//------------------------------------------------------------------------------
// vdqmNetwrite
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServerSocket::vdqmNetwrite(void* hdrbuf) 
	throw (castor::exception::Exception) {
  int rc;
  
  rc = netwrite_timeout(m_socket, hdrbuf, VDQM_HDRBUFSIZ, VDQM_TIMEOUT);
  switch (rc) {
		case -1: 
				{
					serrno = SECOMERR;
	      	castor::exception::Exception ex(serrno);
					ex.getMessage() << "VdqmServerSocket: "
												<< "netwrite(HDR): " 
												<< neterror() << std::endl;
					throw ex;	
				}
				break;
	  case 0:
	  		{
	  			serrno = SECONNDROP;
	      	castor::exception::Exception ex(serrno);
					ex.getMessage() << "VdqmServerSocket: "
												<< "netwrite(HDR): connection dropped" 
												<< std::endl;
					throw ex;	
	  		}
	}	
}


//------------------------------------------------------------------------------
// vdqmNetread
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServerSocket::vdqmNetread(void* hdrbuf) 
	throw (castor::exception::Exception) {
  int rc;
  
  rc = netread_timeout(m_socket, hdrbuf, VDQM_HDRBUFSIZ, VDQM_TIMEOUT);
  switch (rc) {
  	case -1: 
	  		{
	  			serrno = SECOMERR;
	      	castor::exception::Exception ex(serrno);
					ex.getMessage() << "VdqmServerSocket: "
													<< "netread(HDR): " 
													<< neterror() << std::endl;
					throw ex;	
	  		}
     case 0:
     		{
	  			serrno = SECONNDROP;
	      	castor::exception::Exception ex(serrno);
					ex.getMessage() << "VdqmServerSocket: "
													<< "netread(HDR): connection dropped" 
													<< std::endl;
					throw ex;	
	  		}
  }		
}
