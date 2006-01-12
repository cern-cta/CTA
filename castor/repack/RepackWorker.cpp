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
 * @(#)$RCSfile: RepackWorker.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/01/12 14:05:31 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/


#include <iostream>
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
#include "serrno.h"
#include "Cns_api.h"
#include "vmgr_api.h"

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IService.hpp"
#include "castor/MessageAck.hpp"
#include "castor/BaseObject.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/repack/RepackWorker.hpp"




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

  param = NULL; // this is the ServerSocket, if the Thread is runed this 
                // must be initialisied before by calling the init(void*param) function

  nameserver = "castorns.cern.ch";  // The default Nameserver

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
// destructor
//------------------------------------------------------------------------------
RepackWorker::~RepackWorker() throw()
{
  stage_trace(3, "Thread No. %d killed!", counter); 
}



//------------------------------------------------------------------------------
// runs the htread starts by threadassign()
//------------------------------------------------------------------------------
void RepackWorker::run()
{
  stage_trace(3, "Thread No. %d started and Request is now handled!", counter);

  MessageAck ack; // Response for client
  ack.setStatus(true);

  // We know it's a ServerSocket
  castor::io::ServerSocket* sock = (castor::io::ServerSocket*) param;
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
  
  try{
    store_RequestDB(rreq);
  }catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 2, params);
  }
  
  //send_Ack(ack, sock);
  print_filelist(rreq->vid());
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
// Stores the Request in the DB
//------------------------------------------------------------------------------
void RepackWorker::store_RequestDB(RepackRequest* rreq) throw ()
{
  stage_trace(3,"Storing Request in DB" );
/*
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  try {
    svcs()->createRep(&ad, rreq, false);
    // Store files for file requests

    if (0 != rreq) {
      svcs()->fillRep(&ad, rreq, OBJ_SubRequest, false);
    }

    svcs()->commit(&ad);
    // "Request stored in DB" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("ID", rreq->id())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 12, 1, params);

  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    throw e;
  }
  */
}


void RepackWorker::print_filelist(const std::string buf)
{
  Cns_direntape *dtp;

  
  std::vector<Cns_direntape*> seg_list;

  Cns_list list;
  int nbsegs = 0;
  int nbsegs_notused = 0;
  double size_requested = 0;
  double wasted_size = 0;
  int flags = CNS_LIST_BEGIN;
  char* vid = (char*)buf.c_str();	// we copy the buf, so its faster
  

  stage_trace(3,"Getting filelist for %s from %s", vid, nameserver);
  {
  // "Getting filelist" message
  castor::dlf::Param params[] =
    {castor::dlf::Param("VID", vid ),
     castor::dlf::Param("NS", nameserver)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 99, 2, params);
  }
 
 
  // Now the active segments of the tape
  while ((dtp = Cns_listtape (nameserver, vid, flags, &list)) != NULL) {
    flags = CNS_LIST_CONTINUE;

    if (dtp->s_status == 'D') {
    	nbsegs_notused++;
    	wasted_size += dtp->segsize;
    }
    else {
	    seg_list.push_back(dtp);
	    size_requested += dtp->segsize;
	    nbsegs++;
    }
  }
  (void) Cns_listtape (nameserver, vid, CNS_LIST_END, &list);


// MByte calculation:
  if ( size_requested > 0 )
	size_requested /= 100000000;
  if ( wasted_size > 0 )
  	wasted_size /= 100000000;

// logging

  stage_trace(3,
  	"Vid %s: %d segments to repack (%.6f MB), %d segments wasted (%G MB)\n",
  	 vid, nbsegs, size_requested, nbsegs_notused, wasted_size);
  {
  	castor::dlf::Param params[] =
    {castor::dlf::Param("VID", vid ),
     castor::dlf::Param("Segments to Repack", nbsegs),
     castor::dlf::Param("Requested Space", size_requested),
     castor::dlf::Param("Segments wasted", nbsegs_notused),
     castor::dlf::Param("Space wasted", wasted_size)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 99, 4, params);
  }
}


//------------------------------------------------------------------------------
// Stops the Thread
//------------------------------------------------------------------------------
void RepackWorker::stop()
{
  stage_trace(3,"Thread No. %d stopped!", counter );
}


//------------------------------------------------------------------------------
// Gets the Tape information
//------------------------------------------------------------------------------
int RepackWorker::getTapeInfo(const std::string buf){
	
	char p_stat[9];
	struct vmgr_tape_info tape_info;
	char* vid = (char*)buf.c_str();	// we copy the buf, so its faster
	
	typedef std::map<int,std::string> TapeDriveStatus;
	TapeDriveStatus statusname;
	statusname[DISABLED] = "Disabled";
	statusname[EXPORTED] = "Exported";
	statusname[TAPE_BUSY] = "Busy";
	statusname[TAPE_FULL] = "Full";
	statusname[TAPE_RDONLY] = "TAPE_RDONLY";
	statusname[ARCHIVED] = "Archived";
	
	
	if (vmgr_querytape (vid, 0, &tape_info, NULL) < 0) {
		stage_trace (3, "No such volume %s",vid );
		return (-1);
	}
	if ((tape_info.status & TAPE_FULL) == 0) {
		if (tape_info.status == 0) strcpy (p_stat, "FREE");
		else if (tape_info.status & TAPE_BUSY) strcpy (p_stat, "BUSY");
		else if (tape_info.status & TAPE_RDONLY) strcpy (p_stat, "RDONLY");
		else if (tape_info.status & EXPORTED) strcpy (p_stat, "EXPORTED");
		else if (tape_info.status & DISABLED) strcpy (p_stat, "DISABLED");
		else strcpy (p_stat, "?");
		stage_trace(3, "Volume %s has status %s\n", vid, p_stat);
		return (-1);
	}
	stage_trace(3, "Volume %s in Pool %s : %s !\n", 
				vid, tape_info.poolname, statusname[tape_info.status].c_str());
}



//------------------------------------------------------------------------------
// initialises the thread
//------------------------------------------------------------------------------
void RepackWorker::init(void* param)
{
  stage_trace (3, "Thread No. %d init", counter);
  this->param = param;
}





   }  // END Namespace Repack
}  // END Namespace Castor

