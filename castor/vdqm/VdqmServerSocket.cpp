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
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/biniostream.h"
#include "castor/io/StreamAddress.hpp"

#include <vdqm_constants.h>
#include "osdep.h" //for LONGSIZE
#include "marshall.h"
#include "h/Cnetdb.h"
#include <common.h>

// Local Includes
#include "VdqmServerSocket.hpp"
#include "newVdqm.h"

// definition of some constants
#define STG_CALLBACK_BACKLOG 2
#define VDQMSERV 1

#define ADMINREQ(X) ( X == VDQM_HOLD || X == VDQM_RELEASE || \
    X == VDQM_SHUTDOWN )

#define DO_MARSHALL(X,Y,Z,W) { \
    if ( W == SendTo ) {marshall_##X(Y,Z);} \
    else {unmarshall_##X(Y,Z);} }
    
#define DO_MARSHALL_STRING(Y,Z,W,N) { \
    if ( W == SendTo ) {marshall_STRING(Y,Z);} \
    else { if(unmarshall_STRINGN(Y,Z,N)) { serrno=EINVAL; return -1; } } }

    
#define REQTYPE(Y,X) ( X == VDQM_##Y##_REQ || \
    X == VDQM_DEL_##Y##REQ || \
    X == VDQM_GET_##Y##QUEUE || (!strcmp(#Y,"VOL") && X == VDQM_PING) || \
    (!strcmp(#Y,"DRV") && X == VDQM_DEDICATE_DRV) )
 
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
// readObject
//------------------------------------------------------------------------------
castor::IObject* castor::vdqm::VdqmServerSocket::readObject()
  throw(castor::exception::Exception) {

  char* buffer;
  int length;

  // reads from the socket
  readRestOfBuffer(&buffer, length);
  // package the buffer
  std::string sbuffer(buffer, length);
  castor::io::biniostream input(sbuffer);
  // unmarshalls the object
  castor::io::StreamAddress ad(input, "StreamCnvSvc", castor::SVC_STREAMCNV);
  
  castor::IObject* obj = svcs()->createObj(&ad);
  free(buffer);
  
  // return
  return obj;
}


//------------------------------------------------------------------------------
// readMagicNumber
//------------------------------------------------------------------------------
unsigned int castor::vdqm::VdqmServerSocket::readMagicNumber()
	throw (castor::exception::Exception) {
  
	char buffer[sizeof(unsigned int)];
	char* dummy;
  int magic;
  
  // Read the magic number from the socket
  int ret = netread(m_socket,
                    buffer,
                    sizeof(unsigned int));
                    
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
  
  dummy = buffer;
  DO_MARSHALL(LONG, dummy, magic, ReceiveFrom);
  
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
// readRestOfBuffer
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServerSocket::readRestOfBuffer(char** buf, int& n)
  throw (castor::exception::Exception) {
  	
  // First read the header
  unsigned int length;
  int ret = netread(m_socket,
                    (char*)&length,
                    sizeof(unsigned int));
  if (ret != sizeof(unsigned int)) {
    if (0 == ret) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to receive length" << std::endl
                      << "The connection was closed by remote end";
      throw ex;
    } else if (-1 == ret) {
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "Unable to receive length";
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage() << "Received length is too short : only "
                      << ret << " bytes";
      throw ex;
    }
  }

  // Now read the data
  n = length;
  *buf = (char*) malloc(n);
  if (netread(m_socket, *buf, n) != n) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Unable to receive data";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readOldProtocol
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServerSocket::readOldProtocol(newVdqmHdr_t *header, 
      																							newVdqmVolReq_t *volumeRequest, 
													      										newVdqmDrvReq_t *driveRequest,
													      										Cuuid_t cuuid) 
	throw (castor::exception::Exception) {

  // header buffer is shorter, 
  //because the magic number should already be read out
  int headerBufSize = VDQM_HDRBUFSIZ - LONGSIZE;
  char hdrbuf[headerBufSize];
  
  char buf[VDQM_MSGBUFSIZ];
  char *p,*domain;
  struct sockaddr_in from;
  struct hostent *hp;
  int fromlen;
  int magic,reqtype,len,local_access; 
  int rc;
  
      
  reqtype = -1;
  local_access = 0;
  magic = len = 0;

  
  //read rest of header. The magic number is already read out
  rc = netread_timeout(m_socket, hdrbuf, headerBufSize, VDQM_TIMEOUT);
	
	if (rc == -1) {
		serrno = SECOMERR;
		castor::exception::Exception ex(serrno);
		ex.getMessage() << "VdqmServerSocket::readOldProtocol() "
										<< "netread(header): "
                		<< neterror() << std::endl;
		throw ex;
	}
  else if (rc == 0) {
		serrno = SECONNDROP;
		castor::exception::Exception ex(serrno);
		ex.getMessage() << "VdqmServerSocket::readOldProtocol() "
										<< "netread(header): "
										<< "connection dropped" << std::endl;
		throw ex;
	}
    
  p = hdrbuf;

//---------------- Maybe for Unmarschalling? ------------------------
//	// package the buffer
//  std::string sbuffer(p, LONGSIZE);
//  castor::io::biniostream input(sbuffer);
//  // unmarshalls the object
//  castor::io::StreamAddress ad(input, "StreamCnvSvc", castor::SVC_STREAMCNV);

  DO_MARSHALL(LONG, p, reqtype, ReceiveFrom);
  DO_MARSHALL(LONG, p, len, ReceiveFrom);
	
	if ( header != NULL ) {
	    header->reqtype = reqtype;
	    header->len = len;
	} 
	else {
		castor::exception::Internal ex;
    ex.getMessage() << "VdqmServerSocket::readOldProtocol(): "
      							<< "header struct == NULL" << std::endl;
    throw ex;
	}

	if ( VALID_VDQM_MSGLEN(len) ) {
		rc = netread_timeout(m_socket,buf,len,VDQM_TIMEOUT);
		
		if (rc == -1) {

						serrno = SECOMERR;
						castor::exception::Exception ex(serrno);
						ex.getMessage() << "VdqmServerSocket::readOldProtocol() "
														<< "netread(REQ): "
														<< neterror() << std::endl;
						throw ex;
		}
		else if (rc == 0) {
						serrno = SECONNDROP;
						castor::exception::Exception ex(serrno);						
						ex.getMessage() << "VdqmServerSocket::readOldProtocol() "
      											<< "netread(REQ): "
      											<< "connection dropped" << std::endl;
      			throw ex;
  	}
	} 
	else if ( len > 0 ) {
		serrno = SEUMSG2LONG;
		castor::exception::Exception ex(serrno);						
		ex.getMessage() << "VdqmServerSocket::readOldProtocol() netread(REQ): "
										<< "invalid message length "
										<< len << std::endl;
		throw ex;
	}
        
	fromlen = sizeof(from);
	rc = getpeername(m_socket, (struct sockaddr *)&from, (socklen_t *)&fromlen);
	if ( rc == SOCKET_ERROR ) {
		castor::exception::Internal ex;
		ex.getMessage() << "VdqmServerSocket::readOldProtocol() getpeername(): "
										<< neterror() << std::endl;			
		throw ex;
	} 
  
	if ( (hp = Cgethostbyaddr((void *)&(from.sin_addr), 
														sizeof(struct in_addr),
														from.sin_family)) == NULL ) {
		castor::exception::Internal ex;
		ex.getMessage() << "VdqmServerSocket::readOldProtocol() Cgethostbyaddr(): " 
										<< "h_errno = " << h_errno << neterror() << std::endl;
		throw ex;
	}
  
	if (	(REQTYPE(VOL,reqtype) && volumeRequest == NULL) ||
				(REQTYPE(DRV,reqtype) && driveRequest == NULL) ) {
		serrno = EINVAL;
		castor::exception::Exception ex(serrno);
		ex.getMessage() << "VdqmServerSocket::readOldProtocol(): "
										<< "no buffer for reqtype = 0x" 
										<< std::hex << reqtype << std::endl;
  	throw ex;   
	} 
	else if ( REQTYPE(DRV, reqtype) ) {
	  /* 
	   * We need to authorize request host if not same as server name.
	   */
	  strcpy(driveRequest->reqhost,hp->h_name);
	  if ( isremote(from.sin_addr, driveRequest->reqhost) == 1 &&
    			getconfent("VDQM", "REMOTE_ACCESS", 1) == NULL ) {
			castor::exception::Internal ex;
			ex.getMessage() << "VdqmServerSocket::readOldProtocol(): " 
											<< "remote access attempted, host = " 
											<< driveRequest->reqhost << std::endl;
			throw ex;
		} 
		else {
			local_access = 1;
			if ( (domain = strstr(driveRequest->reqhost,".")) != NULL ) 
				*domain = '\0';
		}
	}
  
	if ( ADMINREQ(reqtype) ) {
		// ADMIN request
		castor::dlf::Param params[] =
    	{castor::dlf::Param("reqtype", reqtype),
     	 castor::dlf::Param("h_name", hp->h_name)};
  	castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 15, 2, params);

		if ( (isadminhost(m_socket,hp->h_name) != 0) ) {
    	serrno = EPERM;
    	castor::exception::Exception ex(serrno);
			ex.getMessage() << "VdqmServerSocket::readOldProtocol(): "
											<< "unauthorised ADMIN request (0x" << std::hex << reqtype 
											<< ") from " << hp->h_name << std::endl;
			throw ex;

  	}   
	}
  
  p = buf;
  if ( REQTYPE(VOL,reqtype) && volumeRequest != NULL ) {
    DO_MARSHALL(LONG,p,volumeRequest->VolReqID,ReceiveFrom);
    DO_MARSHALL(LONG,p,volumeRequest->DrvReqID,ReceiveFrom);
    DO_MARSHALL(LONG,p,volumeRequest->priority,ReceiveFrom);
    DO_MARSHALL(LONG,p,volumeRequest->client_port,ReceiveFrom);
    DO_MARSHALL(LONG,p,volumeRequest->clientUID,ReceiveFrom);
    DO_MARSHALL(LONG,p,volumeRequest->clientGID,ReceiveFrom);
    DO_MARSHALL(LONG,p,volumeRequest->mode,ReceiveFrom);
    DO_MARSHALL(LONG,p,volumeRequest->recvtime,ReceiveFrom);
    DO_MARSHALL_STRING(p,volumeRequest->client_host,ReceiveFrom, sizeof(volumeRequest->client_host));
    DO_MARSHALL_STRING(p,volumeRequest->volid,ReceiveFrom, sizeof(volumeRequest->volid));
    DO_MARSHALL_STRING(p,volumeRequest->server,ReceiveFrom, sizeof(volumeRequest->server));
    DO_MARSHALL_STRING(p,volumeRequest->drive,ReceiveFrom, sizeof(volumeRequest->drive));
    DO_MARSHALL_STRING(p,volumeRequest->dgn,ReceiveFrom, sizeof(volumeRequest->dgn));
    DO_MARSHALL_STRING(p,volumeRequest->client_name,ReceiveFrom, sizeof(volumeRequest->client_name));
  }
  
  if ( REQTYPE(DRV,reqtype) && driveRequest != NULL ) {
    DO_MARSHALL(LONG,p,driveRequest->status,ReceiveFrom);
    DO_MARSHALL(LONG,p,driveRequest->DrvReqID,ReceiveFrom);
    DO_MARSHALL(LONG,p,driveRequest->VolReqID,ReceiveFrom);
    DO_MARSHALL(LONG,p,driveRequest->jobID,ReceiveFrom);
    DO_MARSHALL(LONG,p,driveRequest->recvtime,ReceiveFrom);
    DO_MARSHALL(LONG,p,driveRequest->resettime,ReceiveFrom);
    DO_MARSHALL(LONG,p,driveRequest->usecount,ReceiveFrom);
    DO_MARSHALL(LONG,p,driveRequest->errcount,ReceiveFrom);
    DO_MARSHALL(LONG,p,driveRequest->MBtransf,ReceiveFrom);
    DO_MARSHALL(LONG,p,driveRequest->mode,ReceiveFrom);
    DO_MARSHALL(HYPER,p,driveRequest->TotalMB,ReceiveFrom);
    DO_MARSHALL_STRING(p,driveRequest->volid,ReceiveFrom, sizeof(driveRequest->volid));
    DO_MARSHALL_STRING(p,driveRequest->server,ReceiveFrom, sizeof(driveRequest->server));
    DO_MARSHALL_STRING(p,driveRequest->drive,ReceiveFrom, sizeof(driveRequest->drive));
    DO_MARSHALL_STRING(p,driveRequest->dgn,ReceiveFrom, sizeof(driveRequest->dgn));

    /**
     * Normally we received the dedicate String, but we ignore it and use 
     * it for the errorHistory
     */
    DO_MARSHALL_STRING(p,driveRequest->errorHistory,ReceiveFrom, sizeof(driveRequest->errorHistory));
    if ( (local_access == 1) &&
         (domain = strstr(driveRequest->server,".")) != NULL ) *domain = '\0';
  }
 
  
	if ( REQTYPE(DRV,reqtype) && (reqtype != VDQM_GET_DRVQUEUE) ) {
		if (	(strcmp(driveRequest->reqhost,driveRequest->server) != 0) &&
    			(isadminhost(m_socket,driveRequest->reqhost) != 0) ) {
			serrno = EPERM;
      castor::exception::Exception ex(serrno);
			ex.getMessage() << "VdqmServerSocket::readOldProtocol(): "
											<< "unauthorised drive request (0x" << std::hex << reqtype 
											<< ") for " << driveRequest->drive 
											<< "@" << driveRequest->server
											<< " from " << driveRequest->reqhost << std::endl;
			throw ex;	
		}
	}
	
  return(reqtype);
}


//------------------------------------------------------------------------------
// sendToOldClient
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServerSocket::sendToOldClient(newVdqmHdr_t *header, 
													      										newVdqmVolReq_t *volumeRequest, 
      																							newVdqmDrvReq_t *driveRequest,
      																							Cuuid_t cuuid) 
	throw (castor::exception::Exception) {

  char hdrbuf[VDQM_HDRBUFSIZ];
  char buf[VDQM_MSGBUFSIZ];
  char servername[CA_MAXHOSTNAMELEN+1];
  char *p;
  int magic,reqtype,len; 
  int rc;
  
      
  reqtype = -1;
  *servername = '\0';
  magic = len = 0;
 
	rc = gethostname(servername, CA_MAXHOSTNAMELEN);

    
  if ( header != NULL && VDQM_VALID_REQTYPE(header->reqtype) ) 
  	reqtype = header->reqtype;
  else if ( volumeRequest != NULL ) reqtype = VDQM_VOL_REQ;
  else if ( driveRequest != NULL ) reqtype = VDQM_DRV_REQ;
  else {
      log(LOG_ERR,"vdqm_Transfer(): cannot determine request type to send\n");
      return(-1);
  }
  
  if ( *servername != '\0' ) {
      if ( (reqtype == VDQM_VOL_REQ) && (*volumeRequest->client_host) == '\0' ) 
          strcpy(volumeRequest->client_host,servername);
      else if ( reqtype == VDQM_DRV_REQ && *driveRequest->server == '\0' )
          strcpy(driveRequest->server,servername);
  }

    
  p = buf;
  if ( REQTYPE(VOL,reqtype) && volumeRequest != NULL ) {
      DO_MARSHALL(LONG,p,volumeRequest->VolReqID,SendTo);
      DO_MARSHALL(LONG,p,volumeRequest->DrvReqID,SendTo);
      DO_MARSHALL(LONG,p,volumeRequest->priority,SendTo);
      DO_MARSHALL(LONG,p,volumeRequest->client_port,SendTo);
      DO_MARSHALL(LONG,p,volumeRequest->clientUID,SendTo);
      DO_MARSHALL(LONG,p,volumeRequest->clientGID,SendTo);
      DO_MARSHALL(LONG,p,volumeRequest->mode,SendTo);
      DO_MARSHALL(LONG,p,volumeRequest->recvtime,SendTo);
      DO_MARSHALL_STRING(p,volumeRequest->client_host,SendTo, sizeof(volumeRequest->client_host));
      DO_MARSHALL_STRING(p,volumeRequest->volid,SendTo, sizeof(volumeRequest->volid));
      DO_MARSHALL_STRING(p,volumeRequest->server,SendTo, sizeof(volumeRequest->server));
      DO_MARSHALL_STRING(p,volumeRequest->drive,SendTo, sizeof(volumeRequest->drive));
      DO_MARSHALL_STRING(p,volumeRequest->dgn,SendTo, sizeof(volumeRequest->dgn));
      DO_MARSHALL_STRING(p,volumeRequest->client_name,SendTo, sizeof(volumeRequest->client_name));
  }
  if ( REQTYPE(DRV,reqtype) && driveRequest != NULL ) {
      DO_MARSHALL(LONG,p,driveRequest->status,SendTo);
      DO_MARSHALL(LONG,p,driveRequest->DrvReqID,SendTo);
      DO_MARSHALL(LONG,p,driveRequest->VolReqID,SendTo);
      DO_MARSHALL(LONG,p,driveRequest->jobID,SendTo);
      DO_MARSHALL(LONG,p,driveRequest->recvtime,SendTo);
      DO_MARSHALL(LONG,p,driveRequest->resettime,SendTo);
      DO_MARSHALL(LONG,p,driveRequest->usecount,SendTo);
      DO_MARSHALL(LONG,p,driveRequest->errcount,SendTo);
      DO_MARSHALL(LONG,p,driveRequest->MBtransf,SendTo);
      DO_MARSHALL(LONG,p,driveRequest->mode,SendTo);
      DO_MARSHALL(HYPER,p,driveRequest->TotalMB,SendTo);
      DO_MARSHALL_STRING(p,driveRequest->volid,SendTo, sizeof(driveRequest->volid));
      DO_MARSHALL_STRING(p,driveRequest->server,SendTo, sizeof(driveRequest->server));
      DO_MARSHALL_STRING(p,driveRequest->drive,SendTo, sizeof(driveRequest->drive));
      DO_MARSHALL_STRING(p,driveRequest->dgn,SendTo, sizeof(driveRequest->dgn));
      
      /**
	     * Normally we sent the dedicate String now, but we ignore it and use 
	     * it for the errorHistory. This will be interpreted by newer tapeDaemon
	     */
      DO_MARSHALL_STRING(p,driveRequest->errorHistory,SendTo, sizeof(driveRequest->errorHistory));
  }
 
  
  /**
   * reqtype has already been determined above
   */
  if ( header != NULL && header->magic != 0 ) magic = header->magic;
  else magic = VDQM_MAGIC;
  
  len = 0;
  if ( REQTYPE(VOL,reqtype)) {
  	len = NEWVDQM_VOLREQLEN(volumeRequest);
  }
  else if ( REQTYPE(DRV,reqtype) ) {
    len = NEWVDQM_DRVREQLEN(driveRequest);
  }
  else if ( ADMINREQ(reqtype) ) {
  	len = 0;
  }
  else if ( header != NULL ) {
  	len = header->len;
  }
        
  p = hdrbuf;
  DO_MARSHALL(LONG,p,magic,SendTo);
  DO_MARSHALL(LONG,p,reqtype,SendTo);
  DO_MARSHALL(LONG,p,len,SendTo);

  //send buffer to the client
  vdqmNetwrite(hdrbuf);
   
  if ( len > 0 ) {
		rc = netwrite_timeout(m_socket, buf, len, VDQM_TIMEOUT);
		if (rc == -1) {
  		serrno = SECOMERR;
      castor::exception::Exception ex(serrno);
			ex.getMessage() << "VdqmServerSocket::sendToOldClient(): "
											<< "netwrite(REQ): " 
											<< neterror() << std::endl;
			throw ex;	
		}
		else if (rc == 0) {
    	serrno = SECONNDROP;
      castor::exception::Exception ex(serrno);
			ex.getMessage() << "VdqmServerSocket::sendToOldClient(): "
											<< "netwrite(REQ): connection dropped" << std::endl;
			throw ex;	
		}
  }
  
    
  return(reqtype); 
}



//------------------------------------------------------------------------------
// acknCommitOldProtocol
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServerSocket::acknCommitOldProtocol() 
	throw (castor::exception::Exception) {
		
	char hdrbuf[VDQM_HDRBUFSIZ];
	int recvreqtype, len, rc;
	unsigned int magic;
	char *p;

	    
  magic = VDQM_MAGIC;
  len = 0;
  recvreqtype = VDQM_COMMIT;

  p = hdrbuf;
  DO_MARSHALL(LONG,p,magic,SendTo);
  DO_MARSHALL(LONG,p,recvreqtype,SendTo);
  DO_MARSHALL(LONG,p,len,SendTo);
	  	 
  magic = VDQM_MAGIC;
  len = 0;
  p = hdrbuf;
  
  //send buffer to the client
  vdqmNetwrite(hdrbuf);
}


//------------------------------------------------------------------------------
// recvAcknFromOldProtocol
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServerSocket::recvAcknFromOldProtocol() 
	throw (castor::exception::Exception) {
	
    char hdrbuf[VDQM_HDRBUFSIZ];
    int magic, recvreqtype, len, rc;
    char *p;
    
    magic = VDQM_MAGIC;
    len = 0;
    recvreqtype = 0;
    
    //Reads the header from the socket
    vdqmNetread(hdrbuf);
    
    p = hdrbuf;
    DO_MARSHALL(LONG, p, magic, ReceiveFrom);
    DO_MARSHALL(LONG, p, recvreqtype, ReceiveFrom);
    DO_MARSHALL(LONG, p, len, ReceiveFrom);
    
    return recvreqtype;
}


//------------------------------------------------------------------------------
// sendAcknPing
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServerSocket::sendAcknPing(int queuePosition)
	throw (castor::exception::Exception) {
	
	int reqtype;
  char hdrbuf[VDQM_HDRBUFSIZ];
  int magic, len, rc;
  char *p;
    
  magic = VDQM_MAGIC;
  len = queuePosition;
  reqtype = VDQM_PING;

  p = hdrbuf;
  DO_MARSHALL(LONG,p,magic,SendTo);
  DO_MARSHALL(LONG,p,reqtype,SendTo);
  DO_MARSHALL(LONG,p,len,SendTo);
    
  magic = VDQM_MAGIC;
  len = 0;
  p = hdrbuf;
  
  //Send buffer to client
  vdqmNetwrite(hdrbuf);
}


//------------------------------------------------------------------------------
// sendAcknRollbackOldProtocol
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServerSocket::sendAcknRollbackOldProtocol(int errorCode) 
	throw (castor::exception::Exception) {
    
    char hdrbuf[VDQM_HDRBUFSIZ];
    int magic, recvreqtype, len;
    char *p;
   
    
    magic = VDQM_MAGIC;
    len = 0;
    recvreqtype = errorCode;
    
    
    p = hdrbuf;
    DO_MARSHALL(LONG,p,magic, SendTo);
    DO_MARSHALL(LONG,p,recvreqtype, SendTo);
    DO_MARSHALL(LONG,p,len, SendTo);
    

    magic = VDQM_MAGIC;
    len = 0;
    p = hdrbuf;
    
    //Send buffer to client
    vdqmNetwrite(hdrbuf);
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
