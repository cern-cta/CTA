/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/
#ifndef STAGER_JOB_REQUEST_HANDLER_HPP
#define STAGER_JOB_REQUEST_HANDLER_HPP 1

#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "StagerRequestHandler.hpp"

#include "../../../h/stager_uuid.h"
#include "../../../h/stager_constants.h"
#include "../../../h/serrno.h"
#include "../../../h/Cns_api.h"
#include "../../../h/expert_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../../h/u64subr.h"




#include "../../exception/Exception.hpp"
#include "../../IObject.hpp"
#include "../../ObjectSet.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHandler;

      class StagerJobRequestHandler : public virtual StagerRequestHandler{
	
      public: 
	/* empty destructor */
	virtual StagerJobRequestHandler::~StagerJobRequestHandler() throw();


    	/* main function which must be implemented on each handler */
	virtual void StagerJobRequestHandler::handle() throw(castor::exception::Exception) const = 0;


	

	/****************************************************************************************/
	/* job oriented block  */
	/****************************************************************************************/
	void StagerJobRequestHandler::jobOriented() throw(castor::exception::Exception);


	/****************************************************************************************/
	/* after asking the stagerService is it is toBeScheduled */
	/* - do a normal tape recall*/
	/* - check if it is to replicate:*/
	/*         +processReplica if it is needed: */
	/*                    +make the hostlist if it is needed */
	/****************************************************************************************/
	void StagerJobRequestHandler::switchScheduling(int caseToSchedule) throw(castor::exception::Exception);
	
	
	/************************************************************************************/
	/* return if it is to replicate considering: */
	/* - sources.size() */
	/* - maxReplicaNb */
	/* - replicationPolicy (call to the expert system) */
	/**********************************************************************************/
	bool StagerJobRequestHandler::replicaSwitch() throw(castor::exception::Exception);


	/***************************************************************************************************************************/
	/* if the replicationPolicy exists, ask the expert system to get maxReplicaNb for this file                                */
	/**************************************************************************************************************************/
	int StagerJobRequestHandler::checkReplicationPolicy() throw(castor::exception::Exception);


	/************************************************************************************/
	/* process the replicas = build rfs string (and hostlist) */
	/* diskServerName and mountPoint getting from sources (=diskCopyForRecall)*/
	/* - rfs + = ("|") + diskServerName + ":" + mountPoint */
	/* - hostlist + = (":") + diskServerName ()if it isn' t already in hostlist  */
	/*  */
	/**********************************************************************************/
	void StagerJobRequestHandler::processReplicaAndHostlist() throw(castor::exception::Exception);

	
	/*****************************************************************************************************/
	/* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
	/* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
	/**************************************************************************************************/
	inline void StagerJobRequestHandler::buildRmJobRequestPart() throw(castor::exception::Exception) 
	{
	  if(!rfs.empty()){
	    strncpy(this->rmjob.rfs,rfs.c_str(), CA_MAXPATHLEN);
	  }
	  
	  if((useHostlist)&&(hostlist.empty() == false)){
	    strncpy(rmjob.hostlist, hostlist.c_str(), CA_MAXHOSTNAMELEN);
	  }
	  
	  
	  
	  rmjob.ddisk = this->xsize;//depending on the request's type
	  rmjob.openflags = this->openflags;//depending on the request's type
	  
	  rmjob.castorFileId = stgCnsHelper->cnsFileid.fileid;
	  strncpy(rmjob.castorNsHost,stgCnsHelper->cnsFileid.server, CA_MAXHOSTNAMELEN);
	  rmjob.castorNsHost[CA_MAXHOSTNAMELEN] = '\0';
	  
	}/* used after processReplica (if it is necessary) */



	/***********************************************************************************************/
	/* virtual functions inherited from IObject                                                   */
	/*********************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;

      protected:
	
	int maxReplicaNb;
	std::string replicationPolicy;
	std::string default_protocol;
	bool useHostlist;

	std::string rfs;
	std::string hostlist;
	std::list< castor::stager::DiskCopyForRecall *> sources;

	int xsize;
	int openflags;
	bool replyToClient;

	struct rmjob rmjob;
	int nrmjob_out;
	struct rmjob* rmjob_out;
	

      }; //end class StagerJobRequestHandler

      

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif





