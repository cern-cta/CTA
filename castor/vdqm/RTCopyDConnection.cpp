/******************************************************************************
 *                      RTCopyDConnection.cpp
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
 * @(#)RCSfile: RTCopyDConnection.cpp  Revision: 1.0  Release Date: Jul 29, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include <net.h>
#include <unistd.h>
#include <rtcp_constants.h> /* Definition of RTCOPY_MAGIC   */
#include <vdqm_constants.h>

#include "castor/exception/InvalidArgument.hpp"
#include "castor/stager/ClientIdentification.hpp"
#include "castor/io/biniostream.h"

// Local includes
#include "RTCopyDConnection.hpp"
#include "TapeDrive.hpp"
#include "TapeRequest.hpp"
#include "DeviceGroupName.hpp"

#include "osdep.h" //for LONGSIZE
    

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::RTCopyDConnection::RTCopyDConnection(int socket) throw () {
  m_socket = socket;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::RTCopyDConnection::RTCopyDConnection(const unsigned short port,
                           const std::string host)
  throw (castor::exception::Exception) {
  m_socket = 0;
  createSocket();
  m_saddr = buildAddress(port, host);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::RTCopyDConnection::RTCopyDConnection(const unsigned short port,
                           const unsigned long ip)
  throw (castor::exception::Exception) {
  m_socket = 0; 
  createSocket();
  m_saddr = buildAddress(port, ip);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::vdqm::RTCopyDConnection::~RTCopyDConnection() throw () {
  shutdown(m_socket, SD_BOTH);
  closesocket(m_socket);
}


//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void castor::vdqm::RTCopyDConnection::connect()
  throw (castor::exception::Exception) {
  // Connects the socket
  if (::connect(m_socket, (struct sockaddr *)&m_saddr, sizeof(m_saddr)) < 0) {
    int tmpserrno = errno;
    int tmperrno = errno;
    if (errno != ECONNREFUSED) {
      tmpserrno = SECOMERR;
    }
    castor::exception::Exception ex(tmpserrno);
    ex.getMessage() << "Unable to connect socket";
    if (m_saddr.sin_family == AF_INET) {
      unsigned long ip = m_saddr.sin_addr.s_addr;
      ex.getMessage() << " to "
                      << (ip%256) << "."
                      << ((ip >> 8)%256) << "."
                      << ((ip >> 16)%256) << "."
                      << (ip >> 24) << ":"
                      << ntohs(m_saddr.sin_port);
    }
    close(m_socket);
    errno = tmperrno;
    
    throw ex;
  }
}


//------------------------------------------------------------------------------
// sendJobToRTCP
//------------------------------------------------------------------------------
bool castor::vdqm::RTCopyDConnection::sendJobToRTCP(TapeDrive *tapeDrive) 
	throw (castor::exception::Exception) {
	
	bool acknSucc = true; // The return value

	TapeRequest* tapeRequest;
	castor::stager::ClientIdentification* clientData;
	
	castor::io::biniostream sendStream;
	castor::io::biniostream& addrPointer = sendStream;

  int len, rc, magic, reqtype;


	if (tapeDrive == NULL || tapeDrive->runningTapeReq() == NULL) {
		castor::exception::InvalidArgument ex;
		
		ex.getMessage() << "RTCopyDConnection::sendJobToRTCP(): "
										<< "One of the arguments is NULL" << std::endl;
									
		throw ex;
	}
	
  tapeRequest = tapeDrive->runningTapeReq();
  clientData = tapeRequest->client();
  
  magic = RTCOPY_MAGIC_OLD0;
  reqtype = VDQM_CLIENTINFO;
  
  /**
   * Let's count the length of the message for the header.
   * Please notice: Normally the ID is a 64Bit number!!
   * But for historical reasons, we will do a downcast of the
   * id, which is then still unique, because a tapeRequest has
   * a very short lifetime, compared to the number of new IDs
   */
  len = 4*LONGSIZE + clientData->userName().length() + 
			clientData->machine().length()  + 
      tapeDrive->deviceGroupName()->dgName().length()  + 
      tapeDrive->driveName().length()  + 4;
  
  
  /**
   * Now we can do the marshalling
   */
  addrPointer << magic << reqtype << len;
  addrPointer << (int)tapeRequest->id(); //Down cast, because
  addrPointer << clientData->euid();
	addrPointer << clientData->egid();
	addrPointer << clientData->machine();
	addrPointer << tapeDrive->deviceGroupName()->dgName();
	addrPointer << tapeDrive->driveName();
	addrPointer << clientData->userName();
  
  /**
   * After marshalling we can send the informations to RTCP
   */
  rc = netwrite_timeout(m_socket, (char*)sendStream.str().c_str(), 
  											sendStream.str().length(), VDQM_TIMEOUT);

	if (rc == -1) {
		serrno = SECOMERR;
    castor::exception::Exception ex(serrno);
		ex.getMessage() << "RTCopyDConnection::sendJobToRTCP(): "
										<< "netwrite(REQ): " 
										<< neterror() << std::endl;
		throw ex;	
	}
	else if (rc == 0) {
  	serrno = SECONNDROP;
    castor::exception::Exception ex(serrno);
		ex.getMessage() << "RTCopyDConnection::sendJobToRTCP(): "
										<< "netwrite(REQ): connection dropped" << std::endl;
		throw ex;	
	}
 
 
	acknSucc = readRTCPAnswer();
	
	return acknSucc;
}


//------------------------------------------------------------------------------
// readRTCPAnswer
//------------------------------------------------------------------------------
bool castor::vdqm::RTCopyDConnection::readRTCPAnswer() 
	throw (castor::exception::Exception) {
		
  int rc, magic, reqtype, len, errmsglen, msglen, status;
  char errmsg[1024];
  char hdrbuf[VDQM_MSGBUFSIZ];
  std::string message;
  
  rc = netread_timeout(m_socket, hdrbuf, LONGSIZE*3, VDQM_TIMEOUT);
  
  switch (rc) {
  	case -1: 
	  		{
	  			serrno = SECOMERR;
	      	castor::exception::Exception ex(serrno);
					ex.getMessage() << "RTCopyDConnection::sendJobToRTCP(): "
													<< "netread(HDR): " 
													<< neterror() << std::endl;
					throw ex;	
	  		}
     case 0:
     		{
	  			serrno = SECONNDROP;
	      	castor::exception::Exception ex(serrno);
					ex.getMessage() << "RTCopyDConnection::sendJobToRTCP(): "
													<< "netread(HDR): connection dropped" 
													<< std::endl;
					throw ex;	
	  		}
  }			    
  
  
  message = hdrbuf;
 	castor::io::biniostream rcvStream(message);
	castor::io::biniostream& addrPointer = rcvStream;
	
	/**
	 * Do the unmarshalling of the message
	 */
  addrPointer >> magic;
  addrPointer >> reqtype;
  addrPointer >> len;
  
  rc = 0;
  if ( len > 0 ) {
    if ( len > VDQM_MSGBUFSIZ - 3*LONGSIZE ) {
			// "RTCopyDConnection: Too large errmsg buffer requested" message
			castor::dlf::Param params[] =
			  {castor::dlf::Param("valid length", (VDQM_MSGBUFSIZ-3*LONGSIZE)),
			   castor::dlf::Param("requested length", len)};
			castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 48, 2, params);

      len = VDQM_MSGBUFSIZ - 3*LONGSIZE;
    }
    
    rc = netread_timeout(m_socket, hdrbuf, len, VDQM_TIMEOUT);
	  switch (rc) {
	  	case -1: 
		  		{
		  			serrno = SECOMERR;
		      	castor::exception::Exception ex(serrno);
						ex.getMessage() << "RTCopyDConnection::sendJobToRTCP(): "
														<< "netread(HDR): " 
														<< neterror() << std::endl;
						throw ex;	
		  		}
	     case 0:
	     		{
		  			serrno = SECONNDROP;
		      	castor::exception::Exception ex(serrno);
						ex.getMessage() << "RTCopyDConnection::sendJobToRTCP(): "
														<< "netread(HDR): connection dropped" 
														<< std::endl;
						throw ex;	
		  		}
	  }
	  
	  
	  message = hdrbuf;
	 	castor::io::biniostream rcvStream2(message);
	 	castor::io::biniostream& addrPointer2 = rcvStream2;
	  
    /*
     * Acknowledge message
     */
    errmsglen = 1024;
    *errmsg = '\0';
    status = 0;
    
    addrPointer2 >> magic;
    addrPointer2 >> reqtype;
    addrPointer2 >> len;
    
		if ( (magic != RTCOPY_MAGIC && magic != RTCOPY_MAGIC_OLD0) || 
					reqtype != VDQM_CLIENTINFO ) return false;    
    
    addrPointer2 >> status;


    msglen = len - LONGSIZE -1;
    msglen = msglen < errmsglen-1 ? msglen : errmsglen-1;
    strncpy(errmsg, hdrbuf, msglen);
    errmsg[msglen] = '\0';
    errmsglen = msglen;

    len += 3*LONGSIZE;
    
    if ( errmsglen > 0 ) {
			// "RTCopyDConnection: rtcopy daemon returned an error" message
			castor::dlf::Param params[] =
			  {castor::dlf::Param("status", status),
			   castor::dlf::Param("error msg", errmsg)};
			castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 49, 2, params);
			
			return false;
    }
  }  
  
  return true;
}
