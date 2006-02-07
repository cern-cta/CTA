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
 * @(#)$RCSfile: RepackWorker.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2006/02/07 19:58:10 $ $Author: felixehm $
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
  m_nameserver = "castorns.cern.ch";  // The default Nameserver
  m_filelisthelper = new castor::repack::FileListHelper(m_nameserver);
  m_databasehelper = new castor::repack::DatabaseHelper();
}



//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RepackWorker::~RepackWorker() throw()
{
  delete m_filelisthelper;
  delete m_databasehelper;
}



//------------------------------------------------------------------------------
// runs the htread starts by threadassign()
//------------------------------------------------------------------------------
void RepackWorker::run(void* param) throw()
{
  stage_trace(3, "RepackWorker started and Request is now handled!");
  castor::MessageAck ack; // Response for client
  ack.setStatus(true);
  
  castor::io::ServerSocket* sock = (castor::io::ServerSocket*) param;

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
	u_signed64 size = 0;
    for ( int i = 0; i < rreq->subRequest().size() ; i++ ) 
    {
    	RepackSubRequest* subRequest = rreq->subRequest().at(i);
    	getTapeInfo(subRequest);
    	// set the status
		subRequest->setStatus(SUBREQUEST_READYFORSTAGING);
  	}

	/* Go to DB !*/
  	m_databasehelper->storeRequest(rreq);

  }catch (castor::exception::Internal e) {
  	ack.setStatus(false);
    ack.setErrorCode(e.code());
    ack.setErrorMessage(e.getMessage().str());
    
    /*
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 9, 2, params);
    */
    stage_trace(2,"%s\n%s",sstrerror(e.code()), e.getMessage().str().c_str() );
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
				castor::dlf::Param params[] =
				{castor::dlf::Param("POOL", pool_name),
		         castor::dlf::Param("ERRORTEXT", sstrerror(serrno))};
		  		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 19, 2, params);
				throw ex;
			}
			flags = VMGR_LIST_BEGIN;
			castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 20, 0, NULL);
			while ((lp = vmgr_listtape (NULL, pool_name, flags, &list)) != NULL) {
				flags = VMGR_LIST_CONTINUE;
				RepackSubRequest* tmp = new RepackSubRequest();
				tmp->setVid(lp->vid);
				rreq->addSubRequest(tmp);
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
	statusname[DISABLED] 	= "Disabled";
	statusname[EXPORTED] 	= "Exported";
	statusname[TAPE_BUSY] 	= "Busy";
	statusname[TAPE_FULL] 	= "Full";
	statusname[TAPE_RDONLY] = "TAPE_RDONLY";
	statusname[ARCHIVED] 	= "Archived";
	statusname[0]			= "Free";
	
	/* check if the volume exists and get its status */
	if (vmgr_querytape (vid, 0, &tape_info, NULL) < 0) {
		castor::exception::Internal ex;
		ex.getMessage() << sstrerror(serrno);
		castor::dlf::Param params[] =
        {castor::dlf::Param("VID", vid),
         castor::dlf::Param("ErrorText", sstrerror(serrno))};
  		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 16, 1, params);
		//throw ex;
	}

	/* Tapestatus is unkown - nothing to be done !*/
	if ( statusname[tape_info.status] == "" ){
		castor::exception::Internal ex;
		ex.getMessage() << 	"Tape "<< vid << " has unkown status, repack abort for this tape!";
		castor::dlf::Param params[] =
        {castor::dlf::Param("VID", vid)};
  		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17, 1, params);
  		//throw ex;
	}
	/* Tape is Free - nothing to be done !*/
	else if ( tape_info.status == 0) {
		castor::exception::Internal ex;
		ex.getMessage() << 	"Tape "<< vid << " is marked as FREE, no repack to be done!";
		castor::dlf::Param params[] =
        {castor::dlf::Param("VID", vid)};
  		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 18, 1, params);
  		//throw ex;
	}
	/* Ok,we got a valid status. fine, we can proceed*/
	else
		p_stat = statusname[tape_info.status];
	
	stage_trace(3, "Volume %s in Pool :%s ==> %s (%d)<==!\n", 
				vid, tape_info.poolname, p_stat.c_str(), tape_info.status);

	return tape_info.status;
}




   }  // END Namespace Repack
}  // END Namespace Castor

