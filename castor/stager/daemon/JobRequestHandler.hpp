/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/

#ifndef STAGER_JOB_REQUEST_HANDLER_HPP
#define STAGER_JOB_REQUEST_HANDLER_HPP 1



#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "serno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.h"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{


      class StagerJobRequestHandler :public StagerRequestHandler{
	
      public: 
    	/* main function which must be implemented on each handler */
	virtual void StagerJobRequestHandler::handle(void *param) const = 0;


	

	/****************************************************************************************/
	/* job oriented block  */
	/****************************************************************************************/
	void StagerJobRequestHandler::jobOriented() throw();


	/****************************************************************************************/
	/* after asking the stagerService is it is toBeScheduled */
	/* - do a normal tape recall*/
	/* - check if it is to replicate:*/
	/*         +processReplica if it is needed: */
	/*                    +make the hostlist if it is needed */
	/****************************************************************************************/
	void StagerJobRequestHandler::switchScheduling(int caseToSchedule) throw();
	
	
	/************************************************************************************/
	/* return if it is to replicate considering: */
	/* - sources.size() */
	/* - maxReplicaNb */
	/* - replicationPolicy (call to the expert system) */
	/**********************************************************************************/
	bool StagerJobRequestHandler::replicaSwitch() throw();


	/***************************************************************************************************************************/
	/* if the replicationPolicy exists, ask the expert system to get maxReplicaNb for this file                                */
	/**************************************************************************************************************************/
	int StagerJobRequestHandler::checkReplicationPolicy() throw();


	/************************************************************************************/
	/* process the replicas = build rfs string (and hostlist) */
	/* diskServerName and mountPoint getting from sources (=diskCopyForRecall)*/
	/* - rfs + = ("|") + diskServerName + ":" + mountPoint */
	/* - hostlist + = (":") + diskServerName ()if it isn' t already in hostlist  */
	/*  */
	/**********************************************************************************/
	void StagerJobRequestHandler::processReplicaAndHostlist(bool useHostlist) throw();

	
	/*****************************************************************************************************/
	/* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
	/* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
	/**************************************************************************************************/
	inline void StagerJobRequestHandler::buildRmJobRequestPart() throw() ;//after processReplica (if it is necessary);


      protected:
	
	int maxReplicaNb;
	std::string replicationPolicy;

	bool useHostlist;

	std::string rfs;
	std::string hostlist;
	std::list< castor::stager::DiskCopyForRecall *> sources;

	int xsize;
	int openflags;
	std::string default_protocol;
	bool replyToClient;

	struct rmjob rmjob;
	struct rmjob nrmjob_out;
	struct rmjob rmjob_out;
	

      }//end class StagerJobRequestHandler

      

    }//end namespace dbService
  }//end namespace stager
}//end namspace castor


#endif
