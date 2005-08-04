/******************************************************************************
 *                      OldProtocolInterpreter.cpp
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
 * @(#)RCSfile: OldProtocolInterpreter.cpp  Revision: 1.0  Release Date: Aug 3, 2005  Author: mbraeger 
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
#include <sstream>

#include "osdep.h" //for LONGSIZE
#include "h/Cnetdb.h"
#include <common.h>

#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Internal.hpp"
 
// Local includes
#include "OldProtocolInterpreter.hpp"
#include "VdqmServerSocket.hpp"
#include "newVdqm.h"
#include "vdqmMacros.h"  // Needed for marshalling


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::OldProtocolInterpreter::OldProtocolInterpreter(
	VdqmServerSocket* serverSocket,
	const Cuuid_t* cuuid) throw(castor::exception::Exception) {
	
	if ( 0 == serverSocket || 0 == cuuid) {
		castor::exception::InvalidArgument ex;
  	ex.getMessage() << "One of the arguments is NULL";
  	throw ex;
	}
	else {
		ptr_serverSocket = serverSocket;
		m_cuuid = cuuid;
	}
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::vdqm::OldProtocolInterpreter::~OldProtocolInterpreter() 
	throw() {
}


//------------------------------------------------------------------------------
// readProtocol
//------------------------------------------------------------------------------
int castor::vdqm::OldProtocolInterpreter::readProtocol(newVdqmHdr_t *header, 
      																					newVdqmVolReq_t *volumeRequest, 
													      								newVdqmDrvReq_t *driveRequest) 
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
  rc = netread_timeout(ptr_serverSocket->socket(), hdrbuf, headerBufSize, VDQM_TIMEOUT);
	
	if (rc == -1) {
		serrno = SECOMERR;
		castor::exception::Exception ex(serrno);
		ex.getMessage() << "OldProtocolInterpreter::readOldProtocol() "
										<< "netread(header): "
                		<< neterror() << std::endl;
		throw ex;
	}
  else if (rc == 0) {
		serrno = SECONNDROP;
		castor::exception::Exception ex(serrno);
		ex.getMessage() << "OldProtocolInterpreter::readOldProtocol() "
										<< "netread(header): "
										<< "connection dropped" << std::endl;
		throw ex;
	}
    
  p = hdrbuf;

  DO_MARSHALL(LONG, p, reqtype, ReceiveFrom);
  DO_MARSHALL(LONG, p, len, ReceiveFrom);
	
	if ( header != NULL ) {
	    header->reqtype = reqtype;
	    header->len = len;
	} 
	else {
		castor::exception::Internal ex;
    ex.getMessage() << "OldProtocolInterpreter::readOldProtocol(): "
      							<< "header struct == NULL" << std::endl;
    throw ex;
	}

	if ( VALID_VDQM_MSGLEN(len) ) {
		rc = netread_timeout(ptr_serverSocket->socket(),buf,len,VDQM_TIMEOUT);
		
		if (rc == -1) {

						serrno = SECOMERR;
						castor::exception::Exception ex(serrno);
						ex.getMessage() << "OldProtocolInterpreter::readOldProtocol() "
														<< "netread(REQ): "
														<< neterror() << std::endl;
						throw ex;
		}
		else if (rc == 0) {
						serrno = SECONNDROP;
						castor::exception::Exception ex(serrno);						
						ex.getMessage() << "OldProtocolInterpreter::readOldProtocol() "
      											<< "netread(REQ): "
      											<< "connection dropped" << std::endl;
      			throw ex;
  	}
	} 
	else if ( len > 0 ) {
		serrno = SEUMSG2LONG;
		castor::exception::Exception ex(serrno);						
		ex.getMessage() << "OldProtocolInterpreter::readOldProtocol() netread(REQ): "
										<< "invalid message length "
										<< len << std::endl;
		throw ex;
	}
        
	fromlen = sizeof(from);
	rc = getpeername(ptr_serverSocket->socket(), (struct sockaddr *)&from, (socklen_t *)&fromlen);
	if ( rc == SOCKET_ERROR ) {
		castor::exception::Internal ex;
		ex.getMessage() << "OldProtocolInterpreter::readOldProtocol() getpeername(): "
										<< neterror() << std::endl;			
		throw ex;
	} 
  
	if ( (hp = Cgethostbyaddr((void *)&(from.sin_addr), 
														sizeof(struct in_addr),
														from.sin_family)) == NULL ) {
		castor::exception::Internal ex;
		ex.getMessage() << "OldProtocolInterpreter::readOldProtocol() Cgethostbyaddr(): " 
										<< "h_errno = " << h_errno << neterror() << std::endl;
		throw ex;
	}
  
	if (	(REQTYPE(VOL,reqtype) && volumeRequest == NULL) ||
				(REQTYPE(DRV,reqtype) && driveRequest == NULL) ) {
		serrno = EINVAL;
		castor::exception::Exception ex(serrno);
		ex.getMessage() << "OldProtocolInterpreter::readOldProtocol(): "
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
			ex.getMessage() << "OldProtocolInterpreter::readOldProtocol(): " 
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
  	castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_USAGE, 15, 2, params);

		if ( (isadminhost(ptr_serverSocket->socket(),hp->h_name) != 0) ) {
    	serrno = EPERM;
    	castor::exception::Exception ex(serrno);
			ex.getMessage() << "OldProtocolInterpreter::readOldProtocol(): "
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
    			(isadminhost(ptr_serverSocket->socket(),driveRequest->reqhost) != 0) ) {
			serrno = EPERM;
      castor::exception::Exception ex(serrno);
			ex.getMessage() << "OldProtocolInterpreter::readOldProtocol(): "
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
int castor::vdqm::OldProtocolInterpreter::sendToOldClient(newVdqmHdr_t *header, 
											      										newVdqmVolReq_t *volumeRequest, 
   																							newVdqmDrvReq_t *driveRequest) 
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
		serrno = SECOMERR;
	  castor::exception::Internal ex;
		ex.getMessage() << "OldProtocolInterpreter::sendToOldClient(): "
										<< "cannot determine request type to send" 
										<< std::endl;
		throw ex;	
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
  ptr_serverSocket->vdqmNetwrite(hdrbuf);
   
  if ( len > 0 ) {
		rc = netwrite_timeout(ptr_serverSocket->socket(), buf, len, VDQM_TIMEOUT);
		if (rc == -1) {
  		serrno = SECOMERR;
      castor::exception::Exception ex(serrno);
			ex.getMessage() << "OldProtocolInterpreter::sendToOldClient(): "
											<< "netwrite(REQ): " 
											<< neterror() << std::endl;
			throw ex;	
		}
		else if (rc == 0) {
    	serrno = SECONNDROP;
      castor::exception::Exception ex(serrno);
			ex.getMessage() << "OldProtocolInterpreter::sendToOldClient(): "
											<< "netwrite(REQ): connection dropped" << std::endl;
			throw ex;	
		}
  }
  
    
  return(reqtype); 
}



//------------------------------------------------------------------------------
// sendAcknCommit
//------------------------------------------------------------------------------
void castor::vdqm::OldProtocolInterpreter::sendAcknCommit() 
	throw (castor::exception::Exception) {
		
	char hdrbuf[VDQM_HDRBUFSIZ];
	int recvreqtype, len;
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
  ptr_serverSocket->vdqmNetwrite(hdrbuf);
}


//------------------------------------------------------------------------------
// recvAcknFromOldClient
//------------------------------------------------------------------------------
int castor::vdqm::OldProtocolInterpreter::recvAcknFromOldClient() 
	throw (castor::exception::Exception) {
	
    char hdrbuf[VDQM_HDRBUFSIZ];
    int magic, recvreqtype, len;
    char *p;
    
    magic = VDQM_MAGIC;
    len = 0;
    recvreqtype = 0;
    
    //Reads the header from the socket
    ptr_serverSocket->vdqmNetread(hdrbuf);
    
    p = hdrbuf;
    DO_MARSHALL(LONG, p, magic, ReceiveFrom);
    DO_MARSHALL(LONG, p, recvreqtype, ReceiveFrom);
    DO_MARSHALL(LONG, p, len, ReceiveFrom);
    
    return recvreqtype;
}


//------------------------------------------------------------------------------
// sendAcknPing
//------------------------------------------------------------------------------
void castor::vdqm::OldProtocolInterpreter::sendAcknPing(int queuePosition)
	throw (castor::exception::Exception) {
	
	int reqtype;
  char hdrbuf[VDQM_HDRBUFSIZ];
  int magic, len;
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
  ptr_serverSocket->vdqmNetwrite(hdrbuf);
}


//------------------------------------------------------------------------------
// sendAcknRollback
//------------------------------------------------------------------------------
void castor::vdqm::OldProtocolInterpreter::sendAcknRollback(
	int errorCode) 
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
  ptr_serverSocket->vdqmNetwrite(hdrbuf);
}
