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
#include <string>

#include "castor/Services.hpp"
#include "Cgetopt.h"
#include "Cinit.h"
#include "Cuuid.h"
#include "Cpool_api.h"
#include "errno.h"

#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"

#define VDQMSERV

#include <net.h>
#include <vdqm.h>	//Needed for the client_connection
#include <vdqm_constants.h>	//e.g. Magic Number of old vdqm protocol

#include "castor/vdqm/handler/TapeRequestDedicationHandler.hpp"

// Local Includes
#include "VdqmServer.hpp"
#include "VdqmServerSocket.hpp"
#include "OldVdqmProtocol.hpp"

//To make the code more readable
using namespace castor::vdqm::handler;

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
castor::vdqm::VdqmServer::VdqmServer(): 
	m_foreground(false), 
	m_threadPoolId(-1),
  m_threadNumber(DEFAULT_THREAD_NUMBER), 
  m_serverName("VdqmServer") {
  	
  initLog("Vdqm", SVC_DLFMSG);
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
     { 8, "Unable to initialize TapeRequestDedicationHandler thread"},
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
     {26, "Queue position of TapeRequest"},
     {27, "Send VDQM_PING back to client"},
     {28, "TapeRequest and its ClientIdentification removed"},
     {29, "Request deleted from DB"},
     {30, "Client error on commit"},
     {31, "Verify that the request doesn't exist, by calling IVdqmSvc->checkTapeRequest"},
     {32, "Try to store Request into the data base"},
     {33, "The parameters of the old vdqm DrvReq Request"},
     {34, "Create new TapeDrive in DB"},
     {35, "The desired \"old Protocol\" status of the client"},     
     {36, "The actual \"new Protocol\" status of the client"},     
     {37, "Remove old TapeRequest from db"},     
     {38, "WAIT DOWN request from tpdaemon client"}, 
     {39, "Assign of tapeRequest to jobID"},     
     {40, "Local assign to jobID"},     
     {41, "Inconsistent release on tape drive"},     
     {42, "client has requested a forced unmount."},
     {43, "tape drive in STATUS_UNKNOWN status. Force unmount!"},
     {44, "No tape request left for mounted tape"},
     {45, "Update of representation in DB"}, 
     {46, "No free TapeDrive, or no TapeRequest in the db"}, 
     {47, "Try to get information about the tape from the VMGR daemon"},
     {-1, ""}};
  castor::dlf::dlf_init("Vdqm", messages);
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmServer::~VdqmServer() throw() {
  // hack to release thread specific allocated memory
  castor::Services* svcs = services();
  if (0 != svcs) {
    delete svcs;
  }
  
  TapeRequestDedicationHandler* tapeRequestDedicationHandler =
  	TapeRequestDedicationHandler::Instance();
  	
  /**
   * Stopping the thread, which is handling the dedication of the tape
   * to a tape drive
   */
  tapeRequestDedicationHandler->stop();	
  	
  sleep(1);
}


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::main () {
  /**
   * The Singleton with the main loop to dedicate a
   * tape to a tape drive.
   */
  TapeRequestDedicationHandler* tapeRequestDedicationHandler;
  
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
    
    
    try {
	    /**
	     * Create thread, which dedicates the tapes to the tape drives
	     */
	     tapeRequestDedicationHandler = TapeRequestDedicationHandler::Instance();
    }
    catch (castor::exception::Exception ex) {
			// "Unable to initialize TapeRequestDedicationHandler thread" message
			castor::dlf::Param params[] =
			  {castor::dlf::Param("Message", ex.getMessage().str())};
			castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 8, 1, params);    	
    }

		/** Start the loop in a thread **/
		threadAssign(tapeRequestDedicationHandler);    
    
    /* Create a socket for the server, bind, and listen */
    VdqmServerSocket sock(VDQM_PORT, true); 
     
    for (;;) {
      /* Accept connections */
      VdqmServerSocket* s = sock.accept();
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
// start
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::start() {
  int rc;
  if (!m_foreground) {
    if ((rc = Cinitdaemon ((char *)m_serverName.c_str(), 0)) < 0) {
      return -1;
    }
  }
  return serverMain();
}

//------------------------------------------------------------------------------
// serverMain
//------------------------------------------------------------------------------
int  castor::vdqm::VdqmServer::serverMain () {
  // create threads if in multithreaded mode
  int nbThreads, actualNbThreads;
  nbThreads = m_threadNumber;
  
  m_threadPoolId = Cpool_create(nbThreads, &actualNbThreads);
  if (m_threadPoolId < 0) {
    clog() << ALERT << "Thread pool creation error : "
           << m_threadPoolId
           << std::endl;
    return -1;
  } else {
    clog() << DEBUG << "Thread pool created : "
           << m_threadPoolId << ", "
           << actualNbThreads << std::endl;
    m_threadNumber = actualNbThreads;
  }
  
  // Ignore SIGPIPE AND SIGXFSZ
#if ! defined(_WIN32)
	signal (SIGPIPE,SIG_IGN);
	signal (SIGXFSZ,SIG_IGN);
#endif

  return main();
}

//------------------------------------------------------------------------------
// threadAssign
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::threadAssign(void *param) {

  // Initializing the arguments to pass to the static request processor
  struct processRequestArgs *args = new processRequestArgs();
  args->handler = this;
  args->param = param;

  
  int assign_rc = Cpool_assign(m_threadPoolId,
                               &castor::vdqm::staticProcessRequest,
                               args,
                               -1);
  if (assign_rc < 0) {
    clog() << "Error while assigning request to pool" << std::endl;
    return -1;
  }
  
  return 0;
}


//------------------------------------------------------------------------------
// staticProcessRequest
//------------------------------------------------------------------------------
void *castor::vdqm::staticProcessRequest(void *param) {
  castor::vdqm::VdqmServer *server = 0;
  struct processRequestArgs *args;

  if (param == NULL) {
    return 0;
  }

  // Recovering the processRequestArgs
  args = (struct processRequestArgs *)param;

  if (args->handler == NULL) {
    delete args;
    return 0;
  }

  server = dynamic_cast<castor::vdqm::VdqmServer *>(args->handler);
  if (server == 0) {
    delete args;
    return 0;
  }

  // Executing the method
  void *res = server->processRequest(args->param);
  delete args;
  return res;
}

//------------------------------------------------------------------------------
// setForeground
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::setForeground(bool value) {
  m_foreground = value;
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::parseCommandLine(int argc, char *argv[]) {

  static struct Coptions longopts[] =
    {
      {"foreground",         NO_ARGUMENT,        NULL,      'f'},
      {"nbThreads",         REQUIRED_ARGUMENT,        NULL,      'n'},
      {NULL,                 0,                  NULL,        0}
    };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long (argc, argv, "fs", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'n':
      m_threadNumber = atoi(Coptarg);
      break;
    }
	}
}


//------------------------------------------------------------------------------
// processRequest: Method called once per request, where all the code resides
//------------------------------------------------------------------------------
void *castor::vdqm::VdqmServer::processRequest(void *param) throw() {
  
  castor::BaseObject* baseObject = 
  	(castor::BaseObject*) param;
  
  castor::vdqm::VdqmServerSocket* sock = 
  	dynamic_cast<castor::vdqm::VdqmServerSocket*>(baseObject);
  	
  baseObject = 0;
  	
  if (0 != sock) {
	  // placeholder for the request uuid if any
	  Cuuid_t cuuid = nullCuuid;
	  
	  // Retrieve info on the client
	  unsigned short port;
	  unsigned long ip;
	  
	  //The magic Number of the message on the socket
	  unsigned int magicNumber;
		
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
  }
  else { // If it's not a socket, then it's our dedication loop!
  	TapeRequestDedicationHandler* tapeRequestDedicationHandler =
  		(TapeRequestDedicationHandler*) param;
  		
  	/**
  	 * This function ends only, if the stop() function is beeing called.
  	 */
  	tapeRequestDedicationHandler->run();
  	
  	delete tapeRequestDedicationHandler;
  	
  	std::cout << "loop finished" << std::endl;
  }
  
  return 0;
}



//------------------------------------------------------------------------------
// handleOldVdqmRequest
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::handleOldVdqmRequest(
																					VdqmServerSocket* sock, 
																					unsigned int magicNumber,
																					Cuuid_t cuuid) {
 	//Message of the old Protocol
	vdqmVolReq_t volumeRequest;
	vdqmDrvReq_t driveRequest;
	vdqmHdr_t header;
	int reqtype; // Request type of the message
	int rc = 0; // For checking purpose
	
	// The socket read out phase
	try {
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
  
  /**
   * Initialization of the OldVdqmProtocol, which 
   * provides the essential functions
   */
	OldVdqmProtocol oldProtocol(&volumeRequest,
															&driveRequest,
													  	&header);
	
	// The request handling phase
	try {
		// Handle old vdqm request type																			
		castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 14);
		
		oldProtocol.checkRequestType(cuuid);
		oldProtocol.handleRequestType(sock, cuuid);

	} catch (castor::exception::Exception e) {  
    // "Exception caught" message
//    std::ostringstream hexNumber(std::ostringstream::out);
//    hexNumber << "0x" << std::hex << e.code();
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("errorCode", e.code())};      
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 2, params);

    /**
     * Tell the client about the error
     */
    try {
	    if (reqtype != VDQM_PING) {
	    	//This is Protocol specific, because normally it receives the positive
	    	// queue position number back.
	    	sock->sendAcknPing(-e.code());
	    }
	    else {
	    	sock->sendAcknRollbackOldProtocol(e.code());
	    }
    } catch (castor::exception::Exception e) {  
	    // "Exception caught" message
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("Message", e.getMessage().str())};
	    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 1, params);
	  }
	  
	  // Now, we don't need to make a handshake
	  return;
	}
	
	// The Handshake phase
	try {
		/**
		 * Ping requests don't need a handshake! It is already handled in 
		 * OldVdqmProtocol.
		 */
		if (reqtype != VDQM_PING) {
			//Sending reply to client
			castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 10);
			
			sock->acknCommitOldProtocol();
			sock->sendToOldClient(&header, &volumeRequest, &driveRequest, cuuid);
			rc = sock->recvAcknFromOldProtocol();
		}
	} catch (castor::exception::Exception e) {  
    // "Exception caught" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 1, params);
    
    rc = -1;
	}
	
	
  if (rc == -1) {
  	// "Client error on commit" message
  	castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 30);
		
		/**
  	 * Well, something went wrong on server side and we must 
  	 * delete the previously stored request.
  	 */
	  try {
			switch ( reqtype ) {
				case VDQM_VOL_REQ:
							  	header.reqtype = VDQM_DEL_VOLREQ;
									oldProtocol.handleRequestType(sock, cuuid);
									break;

				case VDQM_DRV_REQ:
									break;

				default:
                	break;
			}
		} catch (castor::exception::Exception e) {  
	    // "Exception caught" message
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("Message", e.getMessage().str())};
	    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 1, params);
	  }
  } // end if
  
}
