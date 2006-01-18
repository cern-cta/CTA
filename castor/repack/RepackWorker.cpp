/******************************************************************************
 *                      RepackWorker.cpp
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
 * @(#)$RCSfile: RepackWorker.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/01/18 14:17:33 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/


#include "castor/repack/RepackCommonHeader.hpp" /* the Common Header */
#include <sys/types.h>
#include <time.h>
#include <common.h>     /* for conversion of numbers to char */
#include <vector>
#include <map>


/* ============= */
/* Local headers */
/* ============= */
#include "stager_api.h"
#include "stager_uuid.h"                /* Thread-specific uuids */
#include "vmgr_api.h"

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IService.hpp"
#include "castor/MessageAck.hpp"
#include "castor/BaseObject.hpp"
#include "castor/io/ServerSocket.hpp"

#include "castor/repack/RepackCommonHeader.hpp"
#include "castor/repack/RepackWorker.hpp"
#include "castor/repack/FileListHelper.hpp"
#include "castor/repack/DatabaseHelper.hpp"


namespace castor {


 namespace repack {
  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RepackWorker::RepackWorker()
{
  /* Setting the address to access db */
  /* -------------------------------- */
  stage_trace(3, "Thread No. %d created!", counter);

  m_param = NULL; // this is the ServerSocket, if the Thread is runed this 
                // must be initialisied before by calling the init(void*param) function

  m_nameserver = "castorns.cern.ch";  // The default Nameserver
  m_filelisthelper = new castor::repack::FileListHelper(m_nameserver);
  m_databasehelper = new castor::repack::DatabaseHelper();

  // Initializes the DLF logging. This includes
  // defining the predefined messages
  castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New Request Arrival"},
     { 2, "Could not get Conversion Service for Database"},
     { 3, "Could not get Conversion Service for Streaming"},
     { 4, "Exception caught : server is stopping"},
     { 5, "Exception caught : ignored"},
     { 6, "Invalid Request"},
     { 7, "Unable to read Request from socket"},
     { 8, "Processing Request"},
     { 9, "Exception caught"},
     {10, "Sending reply to client"},
     {11, "Unable to send Ack to client"},
     {12, "Request stored in DB"},
     {13, "Unable to store Request in DB"},
     {14, "Waked up all services at once"},
     {99, "TODO::MESSAGE"},
     {-1, ""}};
  castor::dlf::dlf_init("RepackServerLog", messages);

}


//------------------------------------------------------------------------------
// initialises the thread
//------------------------------------------------------------------------------
void RepackWorker::init(void* param)
{
  stage_trace (3, "Thread No. %d init", counter);
  this->m_param = param;
}



//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RepackWorker::~RepackWorker() throw()
{
  stage_trace(3, "Thread No. %d killed!", counter); 
  delete m_filelisthelper;
  delete m_databasehelper;
}



//------------------------------------------------------------------------------
// runs the htread starts by threadassign()
//------------------------------------------------------------------------------
void RepackWorker::run() throw()
{
  stage_trace(3, "Thread No. %d started and Request is now handled!", counter);
  MessageAck ack; // Response for client
  ack.setStatus(true);
  

  // We know it's a ServerSocket
  castor::io::ServerSocket* sock = (castor::io::ServerSocket*) m_param;
  castor::repack::RepackRequest* rreq = 0;


  // Retrieve info on the client
  unsigned short port;
  unsigned long ip;
  try {
    sock->getPeerIp(port, ip);
  } catch(castor::exception::Exception e) {
    // "Exception caught : ignored" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 2, params);
  }
  
  
  // "New Request Arrival" message
  castor::dlf::Param params[] =
    {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
     castor::dlf::Param("Port", port)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 1, 2, params);
  
  stage_trace(2, "Request from %3d.%3d.%3d.%3d:%d", 
  				 (ip%265),((ip >> 8)%256),((ip >> 16)%256),(ip >> 24), port);


  // get the incoming request
  try {
    castor::IObject* obj = sock->readObject();
    rreq = dynamic_cast<castor::repack::RepackRequest*>(obj);
    if (0 == rreq) {
      delete obj;
      // "Invalid Request" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 6);
      ack.setStatus(false);
      ack.setErrorCode(EINVAL);
      ack.setErrorMessage("Invalid Request object sent to server.");
    }
  } catch (castor::exception::Exception e) {
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 1, params);
    ack.setStatus(false);
    ack.setErrorCode(EINVAL);
    std::ostringstream stst;
    stst << "Unable to read Request object from socket."
         << std::endl << e.getMessage().str();
    ack.setErrorMessage(stst.str());
    stage_trace(3, "Unable to read Request object from socket.");
  }
  
  // Store the Request and the filelist in the Database
  try{
  	/**
  	 * retrieves the filelist from Helper 
  	 */
    
    m_filelisthelper->getFileList(rreq);
    m_databasehelper->store_Request(rreq);

    delete rreq;

    	
  }catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 2, params);
  }
  
  //send_Ack(ack, sock);
  getTapeInfo(rreq->vid());

  
  delete sock;  // originally created from RepackServer
  delete rreq;
}



//------------------------------------------------------------------------------
// Sends Response to Client
//------------------------------------------------------------------------------
void RepackWorker::send_Ack(MessageAck ack, castor::io::ServerSocket* sock)
{
  // Send Response to Client
  stage_trace(3,"Sending Response to Client" );
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 10);
  try {
    sock->sendObject(ack);
  } catch (castor::exception::Exception e) {
    // "Unable to send Ack to client" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 11, 2, params);
    std::cout << e.getMessage();
  }
}




//------------------------------------------------------------------------------
// Stops the Thread
//------------------------------------------------------------------------------
void RepackWorker::stop() throw()
{
  stage_trace(3,"Thread No. %d stopped!", counter );
}


//------------------------------------------------------------------------------
// Gets the Tape information
//------------------------------------------------------------------------------
int RepackWorker::getTapeInfo(const std::string vid){
	
	std::string p_stat = "?";
	struct vmgr_tape_info tape_info;
	typedef std::map<int,std::string> TapeDriveStatus;
	TapeDriveStatus statusname;

	/* just to have a nice access for output */
	statusname[DISABLED] = "Disabled";
	statusname[EXPORTED] = "Exported";
	statusname[TAPE_BUSY] = "Busy";
	statusname[TAPE_FULL] = "Full";
	statusname[TAPE_RDONLY] = "TAPE_RDONLY";
	statusname[ARCHIVED] = "Archived";
	
	/* check if the volume exists and is marked FULL */
	if (vmgr_querytape ((char*)vid.c_str(), 0, &tape_info, NULL) < 0) {
		stage_trace (3, "No such volume %s",vid.c_str() );
		return (-1);
	}
	if ((tape_info.status & TAPE_FULL) == 0) {
		switch ( tape_info.status )
		{
			case 0			:	p_stat = "FREE";break;
			case TAPE_BUSY  :	p_stat = "BUSY";break;
			case TAPE_RDONLY:	p_stat = "RDONLY";break;
			case EXPORTED	:	p_stat = "EXPORTED";break;
			case DISABLED	:	p_stat = "DISABLED";break;
		}
		stage_trace(3, "Volume %s has status %s\n", vid.c_str(), p_stat.c_str());
		return (-1);
	}

	stage_trace(3, "Volume %s in Pool %s : %s !\n", 
				vid.c_str(), tape_info.poolname, 
				statusname[tape_info.status].c_str());
}




   }  // END Namespace Repack
}  // END Namespace Castor

