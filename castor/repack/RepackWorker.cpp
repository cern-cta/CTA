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
 * @(#)$RCSfile: RepackWorker.cpp,v $ $Revision: 1.25 $ $Release$ $Date: 2006/10/11 17:41:49 $ $Author: felixehm $
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
RepackWorker::RepackWorker(RepackServer* pserver)
{
  m_databasehelper = new castor::repack::DatabaseHelper();
  ptr_server = pserver;
}



//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RepackWorker::~RepackWorker()
{
  delete m_databasehelper;
}



//------------------------------------------------------------------------------
// runs the htread starts by threadassign()
//------------------------------------------------------------------------------
void RepackWorker::run(void* param) 
{
  stage_trace(3, "RepackWorker started and Request is now handled!");
  // Response for client
  RepackAck ack;

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
      ack.setErrorCode(EINVAL);
      ack.setErrorMessage("Invalid Request!");
    }
  } catch (castor::exception::Exception e) {
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 1, params);
    ack.setErrorCode(EINVAL);
    ack.setErrorMessage(e.getMessage().str());
    stage_trace(3, (char*)e.getMessage().str().c_str());
    return;
  }
  
  
/****************************************************************************/
  try{
  
  // "New Request Arrival" message
  castor::dlf::Param params[] =
    {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
     castor::dlf::Param("Port", port),
     castor::dlf::Param("Command", rreq->command()),
     castor::dlf::Param("User", rreq->userName()),
     castor::dlf::Param("Machine", rreq->machine()),};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 1, 5, params);

  switch ( rreq->command() ){

  case REMOVE_TAPE:       removeRequest(rreq);
                          break;
  
  case RESTART:           restart(rreq);
                          break;

  case REPACK:            handleRepack(rreq);
                          ack.addRequest(rreq);
                          break;

  case GET_STATUS: /** the old RepackRequest is removed */
                          rreq = getStatus(rreq);
                          rreq->setCommand(GET_STATUS);
                          ack.addRequest(rreq);
                          break;

  case GET_STATUS_ALL:    getStatusAll(rreq);
                          ack.addRequest(rreq);
                          break;

  case GET_STATUS_ARCHIVED:
                          //getAllArchived(rreq);
                          ack.addRequest(rreq);
                          break;
  case ARCHIVE:           archiveSubRequests(rreq);
                          ack.addRequest(rreq);
                          break;

	  default:break;	/** Do nothing by default */
	}

  }catch (castor::exception::Internal e) {
    ack.setErrorCode(e.code());
    ack.setErrorMessage(e.getMessage().str());
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
    std::cerr << e.getMessage();
  }


  /** Clean everything. */  
  freeRepackObj(rreq);
  //delete rreq;
  rreq = NULL;
  delete sock;  // originally created from RepackServer

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
RepackRequest* RepackWorker::getStatus(RepackRequest* rreq) throw (castor::exception::Internal)
{
  /** this method takes only 1! subrequest, this is normaly ensured by the 
    * repack client 
    */
  RepackRequest* result = NULL;

  if ( rreq== NULL || rreq->subRequest().size()==0 ) {
    castor::exception::Internal ex;
    ex.getMessage() << "RepackWorker::getStatus(..) : Invalid Request" << std::endl;
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 6, 0, NULL);
    throw ex;
  }
  /** Get the SubRequest. We query by VID and recieve a full subrequest */
  std::vector<RepackSubRequest*>::iterator tape = rreq->subRequest().begin();
  RepackSubRequest* tmp = m_databasehelper->getSubRequestByVid( (*tape)->vid(), false );
  //m_databasehelper->unlock();
  /// but we don't need the segment data for the client. -> remove them

  if ( tmp != NULL ) {
    result = tmp->requestID();
  }
  delete (*tape);  // we have only one, must be  ensured by the repack client!
  
  /** we don't need the request from the client, we replace it with the 
    *            one from DB
    */
  rreq->subRequest().clear();
  delete rreq;
  return result;
}


//------------------------------------------------------------------------------
// Retrieves all subrequests in the repack system
//------------------------------------------------------------------------------
void RepackWorker::getStatusAll(RepackRequest* rreq) throw (castor::exception::Internal)
{
  std::vector<castor::repack::RepackSubRequest*>* result = 
						m_databasehelper->getAllSubRequests();
	
	std::vector<RepackSubRequest*>::iterator tape = result->begin();
	
	while ( tape != result->end() ){
		rreq->subRequest().push_back((*tape));
		tape++;
	}
	
	delete result;
}


//------------------------------------------------------------------------------
// Archives the finished tapes
//------------------------------------------------------------------------------
void RepackWorker::archiveSubRequests(RepackRequest* rreq) throw (castor::exception::Internal)
{ ///get all finished repack tapes
  std::vector<castor::repack::RepackSubRequest*>* result = 
            m_databasehelper->getAllSubRequestsStatus(SUBREQUEST_DONE);
  
  std::vector<RepackSubRequest*>::iterator tape = result->begin();
  /// for the answer we set the status to archived
  while ( tape != result->end() ){
    (*tape)->setStatus(SUBREQUEST_ARCHIVED);
    rreq->subRequest().push_back((*tape));
    tape++;
  }
  delete result;

  /// now the real archive.
  m_databasehelper->archive();
  //m_databasehelper->unlock();
}





//------------------------------------------------------------------------------
// Removes a request from repack
//------------------------------------------------------------------------------
void RepackWorker::restart(RepackRequest* rreq) throw (castor::exception::Internal)
{   
    /** more than one tape can be restarted at once */
    std::vector<RepackSubRequest*>::iterator tape = rreq->subRequest().begin();
    
    while ( tape != rreq->subRequest().end() ){
      if ( (*tape) == NULL )
        continue;
      /// this DB method returns only running processes (no archived ones)
      RepackSubRequest* tmp = 
        m_databasehelper->getSubRequestByVid( (*tape)->vid(), false );

      if (tmp != NULL ) {
        Cuuid_t cuuid = stringtoCuuid(tmp->cuuid());
        if ( tmp->status() == SUBREQUEST_ARCHIVED){
          castor::exception::Internal ex;
          ex.getMessage() << "This Tape is already archived (" << tmp->vid() << ")" 
                          << std::endl;
          delete tmp;
          throw ex;
        }
        if ( tmp->filesMigrating() || tmp->filesStaging() ){
          castor::exception::Internal ex;
          ex.getMessage() << "There are still files staging/migrating, Restart abort." <<std::endl;
          delete tmp;
          throw ex;
        }
        tmp->setStatus(SUBREQUEST_RESTART);
        m_databasehelper->updateSubRequest(tmp,false,cuuid);
        freeRepackObj(tmp);
      }
      tape++;
    }
}



//------------------------------------------------------------------------------
// Removes a request from repack
//------------------------------------------------------------------------------
void RepackWorker::removeRequest(RepackRequest* rreq) throw (castor::exception::Internal)
{
	
  std::vector<RepackSubRequest*>::iterator tape = rreq->subRequest().begin();
	while ( tape != rreq->subRequest().end() ){
		/** if the vid is not in the repack system, a exception in thrown and
      * send to the client
      */
		RepackSubRequest* tmp = 
			m_databasehelper->getSubRequestByVid( (*tape)->vid(), true );
    
    

		if ( tmp != NULL ) {
      Cuuid_t cuuid = stringtoCuuid(tmp->cuuid());
			tmp->setStatus(SUBREQUEST_READYFORCLEANUP);
      m_databasehelper->updateSubRequest(tmp,false,cuuid);
      freeRepackObj(tmp);
      //m_databasehelper->unlock();
		}
    tape++;
	}
}



//------------------------------------------------------------------------------
// handleRepack
//------------------------------------------------------------------------------
void RepackWorker::handleRepack(RepackRequest* rreq) throw (castor::exception::Internal)
{
	int tapecnt =0;
	/* check if the tape(s)/pool exist */
	if ( !getPoolInfo(rreq) ) {
		return;
	}
 
  /// set the default serviceclass if none is given
  if ( rreq->serviceclass().length() == 0 ){
    rreq->setServiceclass(ptr_server->getServiceClass());
  }

	
	for ( tapecnt = 0; tapecnt < rreq->subRequest().size() ; tapecnt++ ) 
	{
		RepackSubRequest* subRequest = rreq->subRequest().at(tapecnt);
		// set the status
		subRequest->setStatus(SUBREQUEST_READYFORSTAGING);
		// and for each subrequest a own cuuid, for DLF logging
		Cuuid_t cuuid = nullCuuid;
		Cuuid_create(&cuuid);
		char buf[CUUID_STRING_LEN+1];
		Cuuid2string(buf, CUUID_STRING_LEN+1, &cuuid);
		std::string tmp (buf,CUUID_STRING_LEN);
		subRequest->setCuuid(tmp);
	}

	/* Go to DB, but only if tapes were found !*/
	if ( tapecnt ){
	  	m_databasehelper->storeRequest(rreq);
      //m_databasehelper->unlock(); 
	}
		
}


//------------------------------------------------------------------------------
// Retrieves Information about the Pool of the Request
//------------------------------------------------------------------------------
int RepackWorker::getPoolInfo(castor::repack::RepackRequest* rreq) throw (castor::exception::Internal)
{
	char *pool_name;
	int flags;
	vmgr_list list;
	struct vmgr_tape_info *lp;
	unsigned int nbvol = 0;
	std::vector<RepackSubRequest*>::iterator tape;
	

	if ( rreq != NULL )	{
	   
		// Pool Handling 
		if ( rreq->pool().length() > 0 ) {
			//check if exists 
			pool_name = (char*)rreq->pool().c_str();
			
			if (vmgr_querypool (pool_name, NULL, NULL, NULL, NULL) < 0) {
				castor::exception::Internal ex;
				ex.getMessage() << "RepackWorker::getPoolInfo(..): "
					 	<< "No such Pool %s" << pool_name << std::endl;
				castor::dlf::Param params[] =
				{castor::dlf::Param("POOL", pool_name),
		     castor::dlf::Param("ERRORTEXT", sstrerror(serrno))};
		  	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 19, 2, params);
				throw ex;
			}
			
			// get the tapes, check them and add them to the Request 
			flags = VMGR_LIST_BEGIN;
			castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 20, 0, NULL);
			
			while ((lp = vmgr_listtape (NULL, pool_name, flags, &list)) != NULL) {
				flags = VMGR_LIST_CONTINUE;
        /** we don't throw an exception if we find a tape in this pool not 
            in RDONLY/FULL because they are not taken in account for repacking.
          */
				if ( checkTapeForRepack(lp->vid) ){
					RepackSubRequest* tmp = new RepackSubRequest();
					tmp->setVid(lp->vid);
					tmp->setRequestID(rreq);
					rreq->addSubRequest(tmp);
					nbvol++;
				}
				/** if the tape is unvalid for repacking, a message is already
				    written to DLF by checkTapeforRepack(..) 
            The lp is just a pointer to a vmgr_list struct, which we allocated,
            therefore no need for freeing 
         */
			}
			vmgr_listtape (NULL, pool_name, VMGR_LIST_END, &list);
			return nbvol;
		}


		/** No tape pool given, so check the given tapes */ 
		else {
      tape = rreq->subRequest().begin();
      while ( tape != rreq->subRequest().end() ){
      /** throws exception if tape is already in cue or has invalid status, as
          well as in any other error cases 
        */
        if ( !checkTapeForRepack((*tape)->vid()) ){
          castor::exception::Internal ex;
          ex.getMessage() << " Tape " << (*tape)->vid() 
                          << " is not marked as FULL or RDONLY !" 
                          << std::endl;
          throw ex;
          nbvol++;
          tape++;
        }
      }
    }
    return nbvol;
  }
  return -1;
}


//------------------------------------------------------------------------------
// checkTapeForRepack
//------------------------------------------------------------------------------
bool RepackWorker::checkTapeForRepack(std::string tapename) throw (castor::exception::Internal)
{
  /** checks if already in cue */
  if  ( m_databasehelper->is_stored(tapename)  ){
    castor::exception::Internal ex;
    ex.getMessage() << "Tape " << tapename << " already in repack cue." << std::endl;
    throw ex;
  }
  
  /** checks for valid status */
  return ( getTapeStatus(tapename) );
}


//------------------------------------------------------------------------------
// Validates the status of the tape
//------------------------------------------------------------------------------
int RepackWorker::getTapeStatus(std::string tapename) throw (castor::exception::Internal)
{
	
	struct vmgr_tape_info tape_info;
	char *vid = (char*)tapename.c_str();
	
	/* check if the volume exists and get its status */
  if (vmgr_querytape (vid, 0, &tape_info, NULL) < 0) {
    castor::exception::Internal ex;
    ex.getMessage() << "VMGR returns \"" << sstrerror(serrno) 
                    << "\" for Tape "
                    << tapename 
                    << " (wrong volume name?) " << std::endl;
    castor::dlf::Param params[] =
        {castor::dlf::Param("VID", vid),
         castor::dlf::Param("ErrorText", sstrerror(serrno))};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 16, 1, params);
    throw ex;
  }

  if ( !(tape_info.status & TAPE_FULL) && !(tape_info.status & TAPE_RDONLY)) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("VID", vid)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 18, 1, params);
    return 0;
  }

	return 1;
}




   }  // END Namespace Repack
}  // END Namespace Castor

