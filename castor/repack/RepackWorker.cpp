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
 * @(#)$RCSfile: RepackWorker.cpp,v $ $Revision: 1.45 $ $Release$ $Date: 2008/09/09 09:18:40 $ $Author: gtaur $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/



#include "RepackWorker.hpp"
#include "castor/Services.hpp"
#include "RepackAck.hpp"
#include "RepackCommandCode.hpp"
#include "RepackUtility.hpp"



namespace castor {


 namespace repack {
  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RepackWorker::RepackWorker(RepackServer* pserver)
{
  m_dbSvc =NULL;  
  ptr_server = pserver;
}



//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RepackWorker::~RepackWorker() throw()
{
    if (m_dbSvc != NULL) delete m_dbSvc;
    m_dbSvc=NULL;

}



//------------------------------------------------------------------------------
// runs the thread starts by threadassign()
//------------------------------------------------------------------------------
void RepackWorker::run(void* param) 
{
  RepackAck* ack=NULL;
  RepackRequest* rreq = NULL;
  castor::io::ServerSocket* sock=NULL; 

  try {
   
    if ( m_dbSvc == NULL ){
 // service to access the database

      castor::IService* orasvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
      m_dbSvc = dynamic_cast<castor::repack::IRepackSvc*>(orasvc);
      if (m_dbSvc == 0) {
      // FATAL ERROR
	castor::dlf::Param params0[] =
	  {castor::dlf::Param("Standard Message", "RepackWorker fatal error"),};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params0);
	ack=new RepackAck();
	RepackResponse* resp=new RepackResponse();
	resp->setErrorCode(-1);
	resp->setErrorMessage("Repack Server not available at the moment");
	ack->addRepackresponse(resp);
      }
  
    }

    unsigned short port=0;
    unsigned long ip=0;
 
    // we will continue if no error have been logged in ack. 
 
    if (ack == NULL) {
      stage_trace(3, "RepackWorker started and Request is now handled!");

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
	stage_trace(3, (char*)e.getMessage().str().c_str());
      }
    }
  
/****************************************************************************/
    if (ack == NULL) { //everything went fine up to know
 
      // Performe the request 

      castor::dlf::Param params[] =
	{castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
	 castor::dlf::Param("Port", port),
	 castor::dlf::Param("Command", rreq->command())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 5, 3, params);
    
      switch ( rreq->command() ){

      case REMOVE:            
	ack=removeRequest(rreq);
	break;
  
      case RESTART:           
	ack=restartRequest(rreq);
	break;

      case REPACK:            
	ack=handleRepack(rreq);
	break;

      case GET_STATUS:
	ack=getStatus(rreq);
	break;

      case GET_ERROR:
	ack=queryForErrors(rreq); 
	break;
      
      case GET_NS_STATUS:             
	ack=getNsStatus(rreq);
	break;

      case GET_STATUS_ALL:   
	ack=getStatusAll(rreq);
	break;

      case ARCHIVE:           
	ack=archiveSubRequests(rreq);
	break;

      case ARCHIVE_ALL:       
	ack=archiveAllSubRequests(rreq);
	break;
      default: 
	RepackResponse* globalError=new RepackResponse();
	ack=new RepackAck();
	globalError->setErrorCode(-1);
	globalError->setErrorMessage("Unknow command");
	ack->addRepackresponse(globalError);

      }
      ack->setCommand(rreq->command());
    }

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

  } catch (...){}

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

RepackAck*  RepackWorker::getNsStatus(RepackRequest* rreq) throw ()
{
  /** this method takes only 1! subrequest, this is normaly ensured by the 
    * repack client 
    */

  RepackAck* ack=new RepackAck();

  if ( rreq== NULL || rreq->repacksubrequest().size()==0 ) {
    RepackResponse* globalError=new RepackResponse();
    globalError->setErrorCode(-1);
    globalError->setErrorMessage("Incomplete request");
    ack->addRepackresponse(globalError);
    return ack;
    
  }

  /** Get the SubRequest. We query by VID and receive a full subrequest */

  // we give all the segments information to gather information from the name server

  std::vector<RepackSubRequest*>::iterator tape=rreq->repacksubrequest().begin();
  
  while (tape != rreq->repacksubrequest().end()){
    RepackResponse* resp= m_dbSvc->getSubRequestByVid((*tape)->vid(),true); // get the segments
    ack->addRepackresponse(resp); 
    tape++;
  }
  return ack;
}


//------------------------------------------------------------------------------
// Retrieves all the not archived subrequests in the repack system 
//------------------------------------------------------------------------------

RepackAck*  RepackWorker::getStatusAll(RepackRequest* rreq) throw ()
{
  RepackAck* result = m_dbSvc->getAllSubRequests();
  return result;
}


//------------------------------------------------------------------------------
// Archive the finished tapes
//------------------------------------------------------------------------------

RepackAck* RepackWorker::archiveSubRequests(RepackRequest* rreq) throw ()
{ 
  RepackAck* result= m_dbSvc->changeSubRequestsStatus(rreq->repacksubrequest(),RSUBREQUEST_ARCHIVED);
  return result;
}


//------------------------------------------------------------------------------
// Archives all finished tapes
//------------------------------------------------------------------------------
RepackAck* RepackWorker::archiveAllSubRequests(RepackRequest* rreq) throw ()
{
 RepackAck* result=m_dbSvc->changeAllSubRequestsStatus(RSUBREQUEST_ARCHIVED);
 return result;
}


//------------------------------------------------------------------------------
// Restart a  repack subrequest
//------------------------------------------------------------------------------
RepackAck* RepackWorker::restartRequest(RepackRequest* rreq) throw ()
{   
   RepackAck* result=m_dbSvc->changeSubRequestsStatus(rreq->repacksubrequest(),RSUBREQUEST_TOBERESTARTED);
   return result;
}



//------------------------------------------------------------------------------
// Remove a request from repack
//------------------------------------------------------------------------------
RepackAck* RepackWorker::removeRequest(RepackRequest* rreq) throw ()
{
  RepackAck* result=m_dbSvc->changeSubRequestsStatus(rreq->repacksubrequest(),RSUBREQUEST_TOBEREMOVED);    
  return result;
 
}



//------------------------------------------------------------------------------
// handleRepack
//------------------------------------------------------------------------------
RepackAck* RepackWorker::handleRepack(RepackRequest* rreq) throw ()
{
  RepackAck* ack= NULL;
  std::vector<RepackResponse*> badVmgrTapes;

  if (rreq==NULL) {
    RepackResponse* result=new RepackResponse();
    result->setErrorCode(-1);
    result->setErrorMessage("invalid request");
    ack->addRepackresponse(result);
    return ack;
  }

  // If an input tapepool is given, the tapepool tapes are retrieved from vmgr 
 
  if (  rreq->pool().length() > 0){
     if ( getPoolVmgrInfo(rreq)) {
       RepackResponse* result=new RepackResponse();
       result->setErrorCode(-1);
       result->setErrorMessage("invalid tapepool in vmgr");
       return ack;
     }
  } 
  
  // all the input tape are checked querying the vmgr
  
  Cuuid_t cuuid = nullCuuid;
  Cuuid_create(&cuuid);
  char buf[CUUID_STRING_LEN+1];
  Cuuid2string(buf, CUUID_STRING_LEN+1, &cuuid);
  std::string tmp (buf,CUUID_STRING_LEN);

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

    if ( checkTapeVmgrStatus((*tape)->vid())) {
       (*tape)->setStatus(RSUBREQUEST_TOBECHECKED);  
       (*tape)->setCuuid(tmp);
       (*tape)->setRetryNb(rreq->retryMax()); 
       requestToSubmit->addRepacksubrequest(*tape);
      // answer that the submition succeeded 
       
    } else { 
      // failure vmgr 
       RepackResponse* resp=new RepackResponse();
       RepackSubRequest* sub= new RepackSubRequest();
       sub->setVid((*tape)->vid());
       sub->setRepackrequest(NULL);
       resp->setErrorCode(-1); 
       resp->setErrorMessage("invalid vmgr status");
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
    ack = m_dbSvc->storeRequest(requestToSubmit);
  if (ack==NULL){
    ack=new RepackAck();
    ack->setCommand(requestToSubmit->command());
  }

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
bool RepackWorker::getPoolVmgrInfo(castor::repack::RepackRequest* rreq) throw ()
{
	char *pool_name;
	int flags;
	vmgr_list list;
	struct vmgr_tape_info *lp;
	std::vector<RepackSubRequest*>::iterator tape;

	//check if exists 
	
	pool_name = (char*)rreq->pool().c_str();
			
	if (vmgr_querypool (pool_name, NULL, NULL, NULL, NULL) < 0) {
	  return false;
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
	return true;
}


//------------------------------------------------------------------------------
// Validates the status of the tape
//------------------------------------------------------------------------------

bool RepackWorker::checkTapeVmgrStatus(std::string tapename) throw ()
{

  /* check if the volume exists and get its status */
  struct vmgr_tape_info tape_info;
  char *vid = (char*)tapename.c_str();

  if (vmgr_querytape (vid, 0, &tape_info, NULL) < 0) {
    castor::dlf::Param params[] =
        {castor::dlf::Param("VID", vid),
         castor::dlf::Param("ErrorText", sstrerror(serrno))};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 11, 1, params);
      return false;
  }

  if ( !(tape_info.status & TAPE_FULL) && !(tape_info.status & TAPE_RDONLY)) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("VID", vid)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 12, 1, params);
    return false;
  }

  return true;
}

//-----------------------------------------------------------------------------
// Get Status
//-----------------------------------------------------------------------------

 RepackAck* RepackWorker::getStatus(RepackRequest* rreq) throw (){
    RepackAck* ack=new RepackAck();
    if (rreq==NULL) {
      RepackResponse* result=new RepackResponse();
      result->setErrorCode(-1);
      result->setErrorMessage("invalid request");
      ack->addRepackresponse(result);
      return ack;
    }

    std::vector<castor::repack::RepackSubRequest*> sreqs=rreq->repacksubrequest();
    std::vector<castor::repack::RepackSubRequest*>::iterator sreq=sreqs.begin();
    
    while (sreq != sreqs.end()){  

    /** get the information from the db  */

      RepackResponse* tape=m_dbSvc->getSubRequestByVid((*sreq)->vid(),false); // no segments
      ack->addRepackresponse(tape);
      sreq++;

    }
    return ack;
 }


//------------------------------------------------------------------------------
// Query for Errors 
//------------------------------------------------------------------------------

  RepackAck* RepackWorker::queryForErrors(RepackRequest* rreq) throw (){
    RepackAck* ack=new RepackAck();
    if (rreq==NULL) {
      RepackResponse* result=new RepackResponse();
      result->setErrorCode(-1);
      result->setErrorMessage("invalid request");
      ack->addRepackresponse(result);
      return ack;
    }

    std::vector<castor::repack::RepackSubRequest*> sreqs=rreq->repacksubrequest();
    std::vector<castor::repack::RepackSubRequest*>::iterator sreq=sreqs.begin();

    while (sreq != sreqs.end()){ 

    /** get the information from the db  */

      RepackResponse* tape=m_dbSvc->getSubRequestByVid((*sreq)->vid(),true); //segments
      ack->addRepackresponse(tape);
      sreq++;

    }
    return ack;
 }

 }  // END Namespace Repack
}  // END Namespace Castor

