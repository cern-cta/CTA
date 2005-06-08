/******************************************************************************
 *                      VdqmServer.cpp
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
 * @(#)RCSfile: VdqmServer.cpp  Revision: 1.0  Release Date: Apr 11, 2005  Author: mbraeger 
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/
 
 
 // Include Files
#include <signal.h>
#include <iostream>
 
#include "errno.h"
#include "Cuuid.h"

#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseAddress.hpp"

#include "castor/stager/Request.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/QryRequest.hpp"
#include "castor/stager/FilesDeleted.hpp"
#include "castor/rh/Client.hpp"

#include "castor/io/biniostream.h"
#include "castor/MessageAck.hpp"

#include "h/stager_service_api.h"


#define VDQMSERV

#include <net.h>
#include <vdqm.h>	//Needed for the client_connection
#include <vdqm_constants.h>	//e.g. Magic Number of old vdqm protocol

// Local Includes
#include "VdqmServer.hpp"
#include "VdqmServerSocket.hpp"
#include "OldVdqmProtocol.hpp"

//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {

  // First ignoring SIGPIPE and SIGXFSZ 
  // to avoid crashing when a file is too big or
  // when the connection is lost with a client
#if ! defined(_WIN32)
	signal (SIGPIPE,SIG_IGN);
	signal (SIGXFSZ,SIG_IGN);
#endif

	
  try {
    castor::vdqm::VdqmServer server;
    server.parseCommandLine(argc, argv);
    std::cout << "Starting VDQM Daemon" << std::endl;
    server.start();
    return 0;
  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  } catch (...) {
    std::cerr << "Caught exception!" << std::endl;
  }
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmServer::VdqmServer() :
  castor::BaseServer("VdqmServer", 20) {
  initLog("VdqmLog", SVC_DLFMSG);
  // Initializes the DLF logging. This includes
  // defining the predefined messages
  castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New Request Arrival"},
     { 2, "Could not get Conversion Service for Oracle"},
     { 3, "Could not get Conversion Service for Streaming"},
     { 4, "Exception caught : server is stopping"},
     { 5, "Exception caught : ignored"},
     { 6, "Request has MagicNumber from old VDQM Protocol"},
     { 7, "Unable to read Request from socket"},
     { 8, "Processing Request"},//not used
     { 9, "Exception caught"},
     {10, "Sending reply to client"},
     {11, "Unable to send Ack to client"},
     {12, "Request stored in DB"},
     {13, "Wrong Magic number"},
     {14, "Handle old vdqm request type"},
     {15, "ADMIN request"},
     {16, "New VDQM request"},
     {17, "Handle Request type error"},
     {18, "shutdown server requested"},//not used
     {19, "Handle VDQM_VOL_REQ"},
     {20, "Handle VDQM_DRV_REQ"},
     {21, "Handle VDQM_DEL_VOLREQ"},
     {22, "Handle VDQM_DEL_DRVREQ"},
     {23, "The parameters of the old vdqm VolReq Request"},
     {24, "Request priority changed"},
     {25, "Handle VDQM_PING"},
     {-1, ""}};
  castor::dlf::dlf_init("VdqmLog", messages);
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::main () {
  int rc;
  
  try {
    // create oracle and streaming conversion service
    // so that it is not deleted and recreated all the time
    castor::ICnvSvc *svc =
      svcs()->cnvService("DbCnvSvc", castor::SVC_DBCNV);
    if (0 == svc) {
      // "Could not get Conversion Service for Oracle" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2);
      return -1;
    }
    castor::ICnvSvc *svc2 =
      svcs()->cnvService("StreamCnvSvc", castor::SVC_STREAMCNV);
    if (0 == svc2) {
      // "Could not get Conversion Service for Streaming" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3);
      return -1;
    }
    
    /* Create a socket for the server, bind, and listen */
    castor::vdqm::VdqmServerSocket sock(VDQM_PORT, true); 
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 0);
     
    for (;;) {
      /* Accept connexions */
      castor::vdqm::VdqmServerSocket* s = sock.accept();
      /* handle the command. */
      threadAssign(s);
    }
    
    // The code after this pint will never be used.
    // However it can be useful to cleanup evrything correctly
    // if one removes the loop for debug purposes

    // release the Oracle Conversion Service
    svc->release();
    svc2->release();
    
  } catch(castor::exception::Exception e) {
    // "Exception caught : server is stopping" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 2, params);
  }
}

//------------------------------------------------------------------------------
// processRequest: Method called once per request, where all the code resides
//------------------------------------------------------------------------------
void *castor::vdqm::VdqmServer::processRequest(void *param) throw() {
  
  // placeholder for the request uuid if any
  Cuuid_t cuuid = nullCuuid;
  
  // Retrieve info on the client
  unsigned short port;
  unsigned long ip;
  
  //The magic Number of the message on the socket
  unsigned int magicNumber;
  

  // We know it's a VdqmServerSocket
  castor::vdqm::VdqmServerSocket* sock =
    (castor::vdqm::VdqmServerSocket*) param;
	
	// gives a Cuuid to the request
  Cuuid_create(&cuuid); 
	
  try {
    sock->getPeerIp(port, ip);
  } catch(castor::exception::Exception e) {
    // "Exception caught : ignored" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 5, 2, params);
  }
  // "New Request Arrival" message
  castor::dlf::Param params[] =
    {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
     castor::dlf::Param("Port", port)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 1, 2, params);

  // get the incoming request
  try {
  	//First check of the Protocol
  	magicNumber = sock->readMagicNumber();
  } catch (castor::exception::Exception e) {  
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 7, 2, params);
  }

	if (magicNumber == VDQM_MAGIC) {
		//Request has MagicNumber from old VDQM Protocol
		castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 6 );
		handleOldVdqmRequest(sock, magicNumber, cuuid);
	}
  else {
		//Wrong Magic number
		castor::dlf::Param params[] =
      {castor::dlf::Param("Magic Number", magicNumber),
       castor::dlf::Param("VDQM_MAGIC", VDQM_MAGIC)};
		castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 13, 2, params);
 	}

  delete sock;
  return 0;
}



//------------------------------------------------------------------------------
// handleOldVdqmRequest
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::handleOldVdqmRequest(
																					castor::vdqm::VdqmServerSocket* sock, 
																					unsigned int magicNumber,
																					Cuuid_t cuuid) {
 	//Message of the old Protocol
	vdqmVolReq_t volumeRequest;
	vdqmDrvReq_t driveRequest;
	vdqmHdr_t header;
	int reqtype; // Request type of the message
	
	try {
		int i;
		int req_values[] = VDQM_REQ_VALUES;
		
		
		//Allcocate memory for structs
		memset(&volumeRequest,'\0',sizeof(volumeRequest));
    memset(&driveRequest,'\0',sizeof(driveRequest));
    memset(&header,'\0',sizeof(header));
		
		// Put the magic number into the header struct
		header.magic = magicNumber;
		
		// read the rest of the vdqm message
		reqtype = sock->readOldProtocol(&header, 
																		&volumeRequest, 
																		&driveRequest,
																		cuuid);														
  } catch (castor::exception::Exception e) {  
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 7, 1, params);
  }
  
	
	try {
		// Handle old vdqm request type																			
		castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 14);
		
		OldVdqmProtocol oldProtocol(&volumeRequest,
												&driveRequest,
										  	&header,
												reqtype);
		
		oldProtocol.checkRequestType(cuuid);
		oldProtocol.handleRequestType(cuuid);
		
		//Sending reply to client
		castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 10);
		sock->acknCommitOldProtocol();
		sock->sendToOldClient(&header, &volumeRequest, &driveRequest, cuuid);
		sock->recvAcknFromOldProtocol();
	} catch (castor::exception::Exception e) {  
    // "Exception caught" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 1, params);
  }
}
