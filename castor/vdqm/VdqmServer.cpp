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

#include "castor/BaseAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"

#define VDQMSERV

#include <net.h>
#include <vdqm_constants.h>	//e.g. Magic Number of old vdqm protocol

#include "castor/vdqm/handler/TapeRequestDedicationHandler.hpp"

// Local Includes
#include "newVdqm.h" //Needed for the client_connection
#include "VdqmServer.hpp"
#include "VdqmServerSocket.hpp"
#include "OldRequestFacade.hpp"
#include "OldProtocolInterpreter.hpp"


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
  m_dedicationThreadNumber(DEFAULT_THREAD_NUMBER), 
  m_serverName("VdqmServer") {
  	
  initLog("Vdqm", SVC_DLFMSG);
  // Initializes the DLF logging. This includes
  // defining the predefined messages
  castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New Request Arrival"},
     { 2, "Could not get Conversion Service for Database"},
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
     {30, "Client didn't send a VDQM_COMMIT => Rollback of request in db"},
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
     {48, "RTCopyDConnection: Too large errmsg buffer requested"},
     {49, "RTCopyDConnection: rtcopy daemon returned an error"}, 
     {50, "Exception caught in TapeRequestDedicationHandler::run()"}, 
     {51, "VdqmServer::handleOldVdqmRequest(): Rollback of the whole request"},
     {52, "TapeDriveStatusHandler::handleVolMountStatus(): Tape mounted in tapeDrive"},
     {53, "Handle VDQM_DEDICATE_DRV"},
     {54, "Handle VDQM_GET_VOLQUEUE"},
     {55, "Handle VDQM_GET_DRVQUEUE"},
     {56, "Exception caught in TapeRequestDedicationHandler::handleDedication()"},
     {57, "Send information for showqueues command"},
     {58, "Thread pool created"},
     {59, "Thread pool creation error"},
     {60, "Error while assigning request to pool"},
     {61, "Start tape to tape drive dedication thread"},
     {62, "Dedication thread pool created"},
     {63, "Exception caught in TapeRequestDedicationHandler::dedicationRequest()"},
     {64, "No TapeDrive object to commit to RTCPD"},
     {65, "Found a queued tape request for mounted tape"},
     {66, "VdqmServer::handleOldVdqmRequest(): waiting for client acknowledge"},
     {-1, ""}};
  castor::dlf::dlf_init("Vdqm", messages);
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmServer::~VdqmServer() throw() {
   castor::vdqm::handler::TapeRequestDedicationHandler* 
  	tapeRequestDedicationHandler = 
  		castor::vdqm::handler::TapeRequestDedicationHandler::Instance(0);
  	
  /**
   * Stopping the thread, which is handling the dedication of the tape
   * to a tape drive
   */
  tapeRequestDedicationHandler->stop();	
  
  sleep(1);
  
  delete tapeRequestDedicationHandler;
  
  // hack to release thread specific allocated memory
  castor::Services* svcs = services();
//  if (0 != svcs) {
//    delete svcs;
//  }
}


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::main () {
  
  try {    

		/** Starts the TapeDriveDedication loop in a thread **/
		threadAssign(NULL);    
    
    /* Create a socket for the server, bind, and listen */
    VdqmServerSocket sock(VDQM_PORT, true); 
     
    for (;;) {
      /* Accept connections */
      VdqmServerSocket* s = sock.accept();
      /* handle the command. */
      threadAssign(s);
    }
    
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
  	// "Thread pool creation error" message
  	castor::dlf::Param params[] =
			{castor::dlf::Param("threadPoolId", m_threadPoolId)};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 59, 1, params);

    return -1;
  } else {
  	// "Thread pool created" message
		castor::dlf::Param params[] =
			{castor::dlf::Param("threadPoolId", m_threadPoolId),
			castor::dlf::Param("actualNbThreads", actualNbThreads)};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 58, 2, params);

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
  	// "Error while assigning request to pool" message
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 60);

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
      {"foreground", NO_ARGUMENT, NULL, 'f'},
      {"nbThreads", REQUIRED_ARGUMENT, NULL, 'n'},
      {"dedicationThreads", REQUIRED_ARGUMENT, NULL, 'd'},
      {NULL, 0, NULL, 0}
    };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long (argc, argv, "fnd", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'n':
      m_threadNumber = atoi(Coptarg);
      break;
    case 'd':
      m_dedicationThreadNumber = atoi(Coptarg);
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
	    
   	  // "New Request Arrival" message
			castor::dlf::Param params[] =
				{castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
				castor::dlf::Param("Port", port)};
			castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 1, 2, params);
	  } catch(castor::exception::Exception e) {
	    // "Exception caught : ignored" message
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
	       castor::dlf::Param("Precise Message", e.getMessage().str())};
	    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 5, 2, params);
	  }
	
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
			castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 6 );
			
			try {
				handleOldVdqmRequest(sock, magicNumber, cuuid);
		  } catch (castor::exception::Exception e) {
		  	// most of the exceptions are handled inside the function
		  	
		    // "Exception caught" message
		    castor::dlf::Param params[] =
		      {castor::dlf::Param("Message", e.getMessage().str().c_str()),
		       castor::dlf::Param("errorCode", e.code())};      
		    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 2, params);		  	
		  }
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
  	// "Start tape to tape drive dedication thread" message
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 61);

	  /**
	   * The Singleton with the main loop to dedicate a
	   * tape to a tape drive.
	   */
	  castor::vdqm::handler::TapeRequestDedicationHandler* 
	  	tapeRequestDedicationHandler;  		
	  	
    try {
	    /**
	     * Create thread, which dedicates the tapes to the tape drives
	     */
	    tapeRequestDedicationHandler = 
	  		castor::vdqm::handler::TapeRequestDedicationHandler::Instance(
	  		m_dedicationThreadNumber);
    }
    catch (castor::exception::Exception ex) {
			// "Unable to initialize TapeRequestDedicationHandler thread" message
			castor::dlf::Param params[] =
			  {castor::dlf::Param("Message", ex.getMessage().str())};
			castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 8, 1, params);
			
			throw ex;  
			
			exit(-1);
    }	  		
  		
  	/**
  	 * This function ends only, if the stop() function is beeing called.
  	 */
  	tapeRequestDedicationHandler->run();
  	
  	delete tapeRequestDedicationHandler;
  	
  	std::cout << "tapeRequestDedicationHandler finished" << std::endl;
  }
  
  return 0;
}



//------------------------------------------------------------------------------
// handleOldVdqmRequest
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::handleOldVdqmRequest(
																					VdqmServerSocket* sock, 
																					unsigned int magicNumber,
																					Cuuid_t cuuid) 
	throw (castor::exception::Exception) {
 	
 	//Message of the old Protocol
	newVdqmVolReq_t volumeRequest;
	newVdqmDrvReq_t driveRequest;
	newVdqmHdr_t header;
	int reqtype; // Request type of the message
	int rc = 0; // For checking purpose
	bool reqHandled = false;
	
	// handles the connection to the client
	OldProtocolInterpreter* oldProtInterpreter;

	// Needed for commit and rollback actions on the database
	castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
	ad.setCnvSvcType(castor::SVC_DBCNV); 	
	
	
	oldProtInterpreter = new OldProtocolInterpreter(sock, &cuuid);
	
	// The socket read out phase
	try {
		//Allcocate memory for structs
		memset(&volumeRequest,'\0',sizeof(volumeRequest));
    memset(&driveRequest,'\0',sizeof(driveRequest));
    memset(&header,'\0',sizeof(header));
		
		// Put the magic number into the header struct
		header.magic = magicNumber;
		
		// read the rest of the vdqm message		
		reqtype = oldProtInterpreter->readProtocol(&header, 
																		&volumeRequest, 
																		&driveRequest);														
  } catch (castor::exception::Exception e) {  
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 7, 1, params);
    
    delete oldProtInterpreter;
    
    return;
  }
  
  /**
   * Initialization of the OldRequestFacade, which 
   * provides the essential functions
   */
	OldRequestFacade oldRequestFacade(&volumeRequest,
															&driveRequest,
													  	&header);
	
	// The request handling phase
	try {
		// Handle old vdqm request type																			
		castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 14);
		
		oldRequestFacade.checkRequestType(cuuid);
		reqHandled = oldRequestFacade.handleRequestType(oldProtInterpreter, cuuid);

	} catch (castor::exception::Exception e) { 
    castor::dlf::Param params2[] =
      {castor::dlf::Param("Message", e.getMessage().str().c_str()),
       castor::dlf::Param("errorCode", e.code())};   

    // "Exception caught" message   
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 2, params2);
    
    
    // "VdqmServer::handleOldVdqmRequest(): Rollback of the whole request" message   
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 51);
		svcs()->rollback(&ad); 
		
    /**
     * Tell the client about the error
     */
    try {
	    if (reqtype != VDQM_PING) {
	    	//This is Protocol specific, because normally it receives the positive
	    	// queue position number back.
	    	oldProtInterpreter->sendAcknPing(-e.code());
	    }
	    else {
	    	oldProtInterpreter->sendAcknRollback(e.code());
	    }
    } catch (castor::exception::Exception e) {  
	    // "Exception caught" message
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("Message", e.getMessage().str())};
	    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 1, params);
	  }
	  
	  delete oldProtInterpreter;
	  
	  // Now, we don't need to make a handshake
	  return;
	}
	
	
	/**
	 *  The Handshake phase !!!
	 */
	try {
		/**
		 * Ping requests don't need a handshake! It is already handled in 
		 * OldRequestFacade.
		 */
		if (reqHandled && reqtype != VDQM_PING) {
			//Sending reply to client
			castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 10);
			
			oldProtInterpreter->sendAcknCommit();
			oldProtInterpreter->sendToOldClient(&header, &volumeRequest, &driveRequest);
			
			//"VdqmServer::handleOldVdqmRequest(): waiting for client acknowledge" message
			castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 66);
			rc = oldProtInterpreter->recvAcknFromOldClient();

			/**
			 * Now we can commit everything for the database... or not
			 */
			if ( rc  == VDQM_COMMIT) {
				svcs()->commit(&ad);
				
		    // "Request stored in DB" message
		    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 12);				
			}
			else {
				// "Client didn't send a VDQM_COMMIT => Rollback of request in db"
				svcs()->rollback(&ad);
					
		    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 30);				
			}
		}
	} catch (castor::exception::Exception e) { 
    // "Exception caught" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 9, 1, params);
    
    // "VdqmServer::handleOldVdqmRequest(): Rollback of the whole request" message   
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 51);        
 		svcs()->rollback(&ad);
	}
  
  delete oldProtInterpreter;
}
