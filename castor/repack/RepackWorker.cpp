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
 * @(#)$RCSfile: RepackWorker.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2006/01/27 13:08:32 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/



#include "RepackWorker.hpp"




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
  
  castor::BaseObject::initLog("Repack", castor::SVC_STDMSG);
  castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New Request Arrival"},
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
     {14, "Fetching filelist from Nameserver"},
     {15, "Cannot get Filepathname"},
     {16, "No such Tape!"},
     {99, "TODO::MESSAGE"},
     {-1, ""}};
  castor::dlf::dlf_init("Repack", messages);

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
  castor::MessageAck ack; // Response for client
  ack.setStatus(true);
  
  castor::io::ServerSocket* sock = (castor::io::ServerSocket*) m_param;

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
    return;
  }
  
  
  // "New Request Arrival" message
  castor::dlf::Param params[] =
    {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
     castor::dlf::Param("Port", port)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 1, 2, params);
  
  stage_trace(2, "Request from %3d.%3d.%3d.%3d:%d", 
  				 (ip%265),((ip >> 8)%256),((ip >> 16)%256),(ip >> 24), port);

  

  castor::repack::RepackRequest* rreq = 0;

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
      ack.setErrorMessage("Invalid Request!");
    }
  } catch (castor::exception::Exception e) {
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 1, params);
    ack.setStatus(false);
    ack.setErrorCode(EINVAL);
    ack.setErrorMessage("Unable to read Request object from socket.");
    stage_trace(3, "Unable to read Request object from socket.");
    return;
  }
  
  
/****************************************************************************/
  /* Store the Request and the filelist in the Database */
  try{
  	/**
  	 * check if the tape/pool exist and get the filenames
  	 */
    getPoolInfo(rreq);
    
    for ( int i = 0; i < rreq->subRequest().size() ; i++ ) 
    {
    	RepackSubRequest* subRequest = rreq->subRequest().at(i);
    	if ( getTapeInfo(subRequest) > 0 )
    	{
	    	m_filelisthelper->getFileListSegs( subRequest );
			subRequest->setStatus(SUBREQUEST_READYFORSTAGING);
			m_databasehelper->store_Request(rreq);
    	}
  	}
  }catch (castor::exception::Exception e) {
  	ack.setStatus(false);
    ack.setErrorCode(e.code());
    ack.setErrorMessage(e.getMessage().str());
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 9, 2, params);
    stage_trace(2,"Exception : %s\n%s",sstrerror(e.code()), e.getMessage().str().c_str() );
  }
/****************************************************************************/  
  
  

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
  
  
  delete sock;  // originally created from RepackServer
  delete rreq;
  

}



//------------------------------------------------------------------------------
// Stops the Thread
//------------------------------------------------------------------------------
void RepackWorker::stop() throw()
{
  stage_trace(3,"Thread No. %d stopped!", counter );
}



//------------------------------------------------------------------------------
// Retrieves Information about the Pool of the Request
//------------------------------------------------------------------------------
int RepackWorker::getPoolInfo(castor::repack::RepackRequest* rreq) throw()
{
	char *pool_name;
	int flags;
	vmgr_list list;
	struct vmgr_tape_info *lp;
	int nbvol = 0;

	if ( rreq != NULL )	{
		if ( rreq->pool().length() > 0 ) {

			/* check if pool_name exists */
			pool_name = (char*)rreq->pool().c_str();
			if (vmgr_querypool (pool_name, NULL, NULL, NULL, NULL) < 0) {
				castor::exception::Internal ex;
				ex.getMessage() << "RepackWorker::getPoolInfo(..): "
								<< "No such Pool %s" << pool_name << std::endl;
				throw ex;
			}
			flags = VMGR_LIST_BEGIN;
			while ((lp = vmgr_listtape (NULL, pool_name, flags, &list)) != NULL) {
				flags = VMGR_LIST_CONTINUE;
				RepackSubRequest* tmp = new RepackSubRequest();
				tmp->setVid(lp->vid);
				rreq->addSubRequest(tmp);
					/*TODO: Add SubRequests to RepackRequest*/
			}
			vmgr_listtape (NULL, pool_name, VMGR_LIST_END, &list);
			return nbvol;
		}
		return 0;
	}
	return -1;
}



//------------------------------------------------------------------------------
// Gets the Tape information
//------------------------------------------------------------------------------
int RepackWorker::getTapeInfo(castor::repack::RepackSubRequest* subreq){
	
	std::string p_stat = "?";
	struct vmgr_tape_info tape_info;
	typedef std::map<int,std::string> TapeDriveStatus;
	TapeDriveStatus statusname;
	char *vid = (char*)(subreq->vid().c_str());

	/* just to have a nice access for output */
	statusname[DISABLED] = "Disabled";
	statusname[EXPORTED] = "Exported";
	statusname[TAPE_BUSY] = "Busy";
	statusname[TAPE_FULL] = "Full";
	statusname[TAPE_RDONLY] = "TAPE_RDONLY";
	statusname[ARCHIVED] = "Archived";
	
	/* check if the volume exists and is marked FULL */
	if (vmgr_querytape (vid, 0, &tape_info, NULL) < 0) {
		castor::dlf::Param params[] =
  		{castor::dlf::Param("VID", vid)};
  		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 16, 1, params);
		return -1;
	}
	/* this is just done for nice output */
	if ((tape_info.status & TAPE_FULL) == 0) {
		switch ( tape_info.status )
		{
			case 0			:	p_stat = "FREE";break;
			case TAPE_BUSY  :	p_stat = "BUSY";break;
			case TAPE_RDONLY:	p_stat = "RDONLY";break;
			case EXPORTED	:	p_stat = "EXPORTED";break;
			case DISABLED	:	p_stat = "DISABLED";break;
		}
		stage_trace(3, "Volume %s has status %s\n", vid, p_stat.c_str());
		return tape_info.status;
	}

	/* Tape has status FULL !*/
	stage_trace(3, "Volume %s in Pool :%s ==> %s <==!\n", 
				vid, tape_info.poolname, 
				statusname[tape_info.status].c_str());
	return tape_info.status;
}




   }  // END Namespace Repack
}  // END Namespace Castor

