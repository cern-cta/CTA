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
 * @(#)$RCSfile: RepackWorker.cpp,v $ $Revision: 1.47 $ $Release$ $Date: 2009/06/18 15:30:29 $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/



#include "RepackWorker.hpp"
#include "RepackCommandCode.hpp"
#include "RepackUtility.hpp"
#include "castor/IObject.hpp"
#include "castor/BaseObject.hpp"
#include "castor/io/ServerSocket.hpp"
#include "FileListHelper.hpp"
#include "RepackResponse.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "vmgr_api.h"

#include <sys/types.h>
#include <time.h>
#include <common.h> 
#include <vector>
#include <errno.h>
#include "IRepackSvc.hpp"


namespace castor {


 namespace repack {
  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RepackWorker::RepackWorker(RepackServer* pserver)
{ 
  ptr_server = pserver;
}



//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RepackWorker::~RepackWorker() throw()
{

}



//------------------------------------------------------------------------------
// runs the thread starts by threadassign()
//------------------------------------------------------------------------------
void RepackWorker::run(void* param) 
{
  RepackAck* ack=NULL;
  RepackRequest* rreq = NULL;
  castor::io::ServerSocket* sock=NULL;    
    
  unsigned short port=0;
  unsigned long ip=0;

  // connect to the db
  // service to access the database

  castor::IService* dbSvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
  castor::repack::IRepackSvc* oraSvc = dynamic_cast<castor::repack::IRepackSvc*>(dbSvc);
  

  if (0 == oraSvc) {    
   castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1 , 0, NULL);
   return;
  }
 
  // we will continue if no error have been logged in ack. 

  sock = (castor::io::ServerSocket*) param;

  // Retrieve info on the client

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

  // get the incoming request
  try {
    castor::IObject* obj = sock->readObject();
    rreq = dynamic_cast<castor::repack::RepackRequest*>(obj);
    if (0 == rreq) {
      delete obj; 
      // "Invalid Request" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 6);
      ack=new RepackAck();
      RepackResponse* resp=new RepackResponse();
      resp->setErrorCode(EINVAL);
      resp->setErrorMessage("Invalid Request!");
      ack->addRepackresponse(resp);
    }
  } catch (castor::exception::Exception e) {
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 1, params);
    ack=new RepackAck();
    RepackResponse* resp=new RepackResponse();
    resp->setErrorCode(EINVAL);
    resp->setErrorMessage(e.getMessage().str());
    ack->addRepackresponse(resp);

  }
    
  
/****************************************************************************/

    if (ack == NULL) { //everything went fine up to know
 
      // Performe the request 

      castor::dlf::Param params[] =
	{castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
	 castor::dlf::Param("Port", port),
	 castor::dlf::Param("Command", rreq->command())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 5, 3, params);
      try {

	switch ( rreq->command() ){

	case REMOVE:            
	  ack=removeRequest(rreq,oraSvc);
	  break;
  
	case RESTART:           
	  ack=restartRequest(rreq,oraSvc);
	  break;

	case REPACK:            
	  ack=handleRepack(rreq,oraSvc);
	  break;

	case GET_STATUS:
	  ack=getStatus(rreq,oraSvc);
	  break;

	case GET_ERROR:
	  ack=getSubRequestsWithSegments(rreq,oraSvc); 
	  break;
      
	case GET_NS_STATUS:             
	  ack=getSubRequestsWithSegments(rreq,oraSvc);
	  break;

	case GET_STATUS_ALL:   
	  ack=getStatusAll(rreq,oraSvc);
	  break;

	case ARCHIVE:           
	  ack=archiveSubRequests(rreq,oraSvc);
	  break;

	case ARCHIVE_ALL:       
	  ack=archiveAllSubRequests(rreq,oraSvc);
	  break;
	default: 
	  RepackResponse* globalError=new RepackResponse();
	  ack=new RepackAck();
	  globalError->setErrorCode(-1);
	  globalError->setErrorMessage("Unknow command");
	  ack->addRepackresponse(globalError);

	}


      } catch (castor::exception::Exception e){
	
	castor::dlf::Param params[] = {castor::dlf::Param("ErrorCode", e.code()),
				       castor::dlf::Param("Precise Message", e.getMessage().str())};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 67, 2,params);
	
	RepackResponse* globalError=new RepackResponse();
ack=new RepackAck();
	globalError->setErrorCode(e.code());
	globalError->setErrorMessage(e.getMessage().str());
	ack->addRepackresponse(globalError);
       
      } 
    }

    if (ack !=NULL && rreq !=NULL)
      ack->setCommand(rreq->command());

/****************************************************************************/ 

      // Answer the client

    try {
    
      castor::dlf::Param params[] =
	{castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
	 castor::dlf::Param("Port", port),
	 castor::dlf::Param("Command", ack->command())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 8, 3, params);

      sock->sendObject(*ack);

    } catch (castor::exception::Exception e) {
  	
      // "Unable to send Ack to client" message
      castor::dlf::Param params[] =
	{castor::dlf::Param("Standard Message", sstrerror(e.code())),
	 castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 9, 2, params);
      std::cerr << e.getMessage();
    }



    /** Clean everything. */ 
 
    if(rreq !=NULL) freeRepackObj(rreq);
    if (ack!= NULL) freeRepackObj(ack);
    rreq = NULL;
    ack=NULL;

    if (sock !=NULL) delete sock;  // originally created from RepackServer
    sock=NULL;
}



//------------------------------------------------------------------------------
// Stops the Thread
//------------------------------------------------------------------------------
void RepackWorker::stop() 
{


}

//------------------------------------------------------------------------------
// Retrieves the subrequest for client answer
//------------------------------------------------------------------------------

RepackAck*  RepackWorker::getSubRequestsWithSegments(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception)
{

  if ( rreq== NULL || rreq->repacksubrequest().size()==0 ) {
    castor::exception::Exception e(EINVAL);
    throw e;
  }

  RepackAck* ack=new RepackAck();

  /** Get the SubRequest. We query by VID and receive a full subrequest */

  // we give all the segments information to gather information from the name server

  std::vector<RepackSubRequest*>::iterator tape=rreq->repacksubrequest().begin();
  RepackResponse* resp=NULL;
  while (tape != rreq->repacksubrequest().end()){
    try {

      resp=oraSvc->getSubRequestByVid((*tape)->vid(),true); // get the segments

    } catch (castor::exception::Exception e){
      RepackResponse* resp= new RepackResponse();
      resp->setErrorCode(e.code());
      resp->setErrorMessage(e.getMessage().str());
      
    }
    ack->addRepackresponse(resp); 
    tape++;
  }
  return ack;
}


//------------------------------------------------------------------------------
// Retrieves all the not archived subrequests in the repack system 
//------------------------------------------------------------------------------

RepackAck*  RepackWorker::getStatusAll(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception)
{

  if ( rreq== NULL) {
    castor::exception::Exception e(EINVAL);
    throw e;
  }
  RepackAck* result = oraSvc->getAllSubRequests();
  return result;
}


//------------------------------------------------------------------------------
// Archive the finished tapes
//------------------------------------------------------------------------------

RepackAck* RepackWorker::archiveSubRequests(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception)
{ 
  if ( rreq== NULL) {
    castor::exception::Exception e(EINVAL);
    throw e;
  }

  RepackAck* result= oraSvc->changeSubRequestsStatus(rreq->repacksubrequest(),RSUBREQUEST_ARCHIVED);
  return result;
}


//------------------------------------------------------------------------------
// Archives all finished tapes
//------------------------------------------------------------------------------
RepackAck* RepackWorker::archiveAllSubRequests(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception)
{
  if ( rreq== NULL) {
    castor::exception::Exception e(EINVAL);
    throw e;
  }
  RepackAck* result=oraSvc->changeAllSubRequestsStatus(RSUBREQUEST_ARCHIVED);
  return result;
}


//------------------------------------------------------------------------------
// Restart a  repack subrequest
//------------------------------------------------------------------------------
RepackAck* RepackWorker::restartRequest(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception)
{   
  if ( rreq== NULL) {
    castor::exception::Exception e(EINVAL);
    throw e;
  }
  RepackAck* result=oraSvc->changeSubRequestsStatus(rreq->repacksubrequest(),RSUBREQUEST_TOBERESTARTED);
  return result;
}



//------------------------------------------------------------------------------
// Remove a request from repack
//------------------------------------------------------------------------------
RepackAck* RepackWorker::removeRequest(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception)
{
  if ( rreq== NULL) {
    castor::exception::Exception e(EINVAL);
    throw e;
  }
  RepackAck* result=oraSvc->changeSubRequestsStatus(rreq->repacksubrequest(),RSUBREQUEST_TOBEREMOVED);    
  return result;
 
}



//------------------------------------------------------------------------------
// handleRepack
//------------------------------------------------------------------------------
RepackAck* RepackWorker::handleRepack(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception)
{
  RepackAck* ack= NULL;
  std::vector<RepackResponse*> badVmgrTapes;

  if (rreq==NULL) {
    castor::exception::Exception e(EINVAL);
    throw e;
  }

  // If an input tapepool is given, the tapepool tapes are retrieved from vmgr 
 
  if (  rreq->pool().length() > 0){
    // get the tapes if the repack of a pool is issued
    getPoolVmgrInfo(rreq); 
  } 
  
  // all the input tape are checked querying the vmgr
  
  Cuuid_t cuuid = nullCuuid;
  Cuuid_create(&cuuid);
  char buf[CUUID_STRING_LEN+1];
  Cuuid2string(buf, CUUID_STRING_LEN+1, &cuuid);
  std::string tmp (buf,CUUID_STRING_LEN);

  if (rreq->repacksubrequest().empty()){
    castor::exception::Exception e(EINVAL);
    throw e;
  }

  std::vector<RepackSubRequest*>::iterator tape=rreq->repacksubrequest().begin(); 

  std::vector<RepackResponse*>::iterator errorVmgr;
  RepackRequest* requestToSubmit=new RepackRequest();
 
  //copied the request  

  requestToSubmit->setMachine(rreq->machine());
  requestToSubmit->setUserName(rreq->userName());
  requestToSubmit->setCreationTime(rreq->creationTime());
  requestToSubmit->setPool(rreq->pool());
  requestToSubmit->setPid(rreq->pid());
  requestToSubmit->setSvcclass(rreq->svcclass());
  requestToSubmit->setCommand(rreq->command());
  requestToSubmit->setStager(rreq->stager());
  requestToSubmit->setUserId(rreq->userId());
  requestToSubmit->setGroupId(rreq->groupId());
  requestToSubmit->setRetryMax(rreq->retryMax());
  requestToSubmit->setReclaim(rreq->reclaim());
  requestToSubmit->setFinalPool(rreq->finalPool());
 
  while ( tape != rreq->repacksubrequest().end() ){
    // answer that the submition succeeded 
    try{
      checkTapeVmgrStatus((*tape)->vid());
      (*tape)->setStatus(RSUBREQUEST_TOBECHECKED);  
      (*tape)->setCuuid(tmp);
      (*tape)->setRetryNb(rreq->retryMax()); 
      requestToSubmit->addRepacksubrequest(*tape);
      
    } catch (castor::exception::Exception e) { 
      // failure vmgr 
      RepackResponse* resp=new RepackResponse();
      RepackSubRequest* sub= new RepackSubRequest();
      sub->setVid((*tape)->vid());
      sub->setRepackrequest(NULL);
      resp->setErrorCode(e.code()); 
      resp->setErrorMessage(e.getMessage().str());
      resp->setRepacksubrequest(sub);
      badVmgrTapes.push_back(resp);  
    }
    tape++;
    
  }

  /// set the default serviceclass if none is given
  if ( requestToSubmit->svcclass().length() == 0 ){
    requestToSubmit->setSvcclass(ptr_server->getServiceClass());
  }

  /// set the default stager if none is given 
  if ( requestToSubmit->stager().length() == 0 ){
    requestToSubmit->setStager(ptr_server->getStagerName());
  }
 
  if (!requestToSubmit->repacksubrequest().empty())
    ack = oraSvc->storeRequest(requestToSubmit);

  if (!badVmgrTapes.empty()){
    // report failure due to vmgr problems
    errorVmgr=badVmgrTapes.begin();
    while (errorVmgr != badVmgrTapes.end()){
      ack->addRepackresponse(*errorVmgr);
      errorVmgr++;
    }
  }  

  delete requestToSubmit; // just the request
  return ack;  		
}


//------------------------------------------------------------------------------
// Retrieves Information about the Pool of the Request
//------------------------------------------------------------------------------
void RepackWorker::getPoolVmgrInfo(castor::repack::RepackRequest* rreq) throw (castor::exception::Exception)
{
	char *pool_name;
	int flags;
	vmgr_list list;
	struct vmgr_tape_info *lp;
	std::vector<RepackSubRequest*>::iterator tape;

	//check if exists 
	
	pool_name = (char*)rreq->pool().c_str();
			
	serrno=0;
	if (vmgr_querypool (pool_name, NULL, NULL, NULL, NULL) < 0) {
	  castor::exception::Exception e(serrno);
	  throw e;
	}
			
	// get the tapes, check them and add them to the Request 
	flags = VMGR_LIST_BEGIN;
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 10, 0, 0);
			
	while ((lp = vmgr_listtape (NULL, pool_name, flags, &list)) != NULL) {
	  flags = VMGR_LIST_CONTINUE;
	  RepackSubRequest* tmp = new RepackSubRequest();
	  tmp->setVid(lp->vid);
	  tmp->setRepackrequest(rreq);
	  rreq->addRepacksubrequest(tmp);
  
	}
			
	vmgr_listtape (NULL, pool_name, VMGR_LIST_END, &list);
}


//------------------------------------------------------------------------------
// Validates the status of the tape
//------------------------------------------------------------------------------

void RepackWorker::checkTapeVmgrStatus(std::string tapename) throw (castor::exception::Exception)
{

  /* check if the volume exists and get its status */
  struct vmgr_tape_info tape_info;
  char *vid = (char*)tapename.c_str();
  serrno=0;
  if (vmgr_querytape (vid, 0, &tape_info, NULL) < 0) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("VID", vid),
       castor::dlf::Param("ErrorText", sstrerror(serrno))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 11, 1, params);
    castor::exception::Exception e(serrno);
    throw e;
  }

  if ( !(tape_info.status & TAPE_FULL) && !(tape_info.status & TAPE_RDONLY)) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("VID", vid)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 12, 1, params);
    castor::exception::Exception e(EINVAL);
    throw e;
  }

}

//-----------------------------------------------------------------------------
// Get Status
//-----------------------------------------------------------------------------

 RepackAck* RepackWorker::getStatus(RepackRequest* rreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception){
  
   if ( rreq== NULL) {
     castor::exception::Exception e(EINVAL);
     throw e;
   }
   RepackAck* ack=new RepackAck();
   std::vector<castor::repack::RepackSubRequest*> sreqs=rreq->repacksubrequest();
   std::vector<castor::repack::RepackSubRequest*>::iterator sreq=sreqs.begin();
   RepackResponse* resp=NULL;

   while (sreq != sreqs.end()){  

     /** get the information from the db  */
     try {

       resp=oraSvc->getSubRequestByVid((*sreq)->vid(),false); // no segments

     } catch (castor::exception::Exception e) {
       RepackResponse* resp= new RepackResponse();
       resp->setErrorCode(e.code());
       resp->setErrorMessage(e.getMessage().str());
     }
     ack->addRepackresponse(resp);
     sreq++;
     
   }
   return ack;
 }


}  // END Namespace Repack
}  // END Namespace Castor

