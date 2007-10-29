/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/
#ifndef STAGER_JOB_REQUEST_HANDLER_HPP
#define STAGER_JOB_REQUEST_HANDLER_HPP 1

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"
#include "expert_api.h"

#include "serrno.h"
#include <errno.h>

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"




#include "castor/exception/Exception.hpp"
#include "castor/ObjectSet.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{

    
      class StagerJobRequestHandler : public virtual StagerRequestHandler{
	
      public:
        StagerJobRequestHandler() throw() : m_notifyJobManager(false) {};
        virtual ~StagerJobRequestHandler() throw() {};

	/*******************************************************************/
	/* function to set the handler's attributes according to its type */
	/*****************************************************************/
	virtual void handlerSettings() throw(castor::exception::Exception) = 0;

    	/* main function which must be implemented on each handler */
	virtual void handle() throw(castor::exception::Exception) = 0;


	

	/****************************************************************************************/
	/* job oriented block  */
	/****************************************************************************************/
	void jobOriented() throw(castor::exception::Exception);


	/********************************************/	
	/* for Get, Update                         */
	/*     switch(getDiskCopyForJob):         */                                     
	/*        case 0,1: (staged) jobManager  */
	/*        case 2: (waitRecall) createTapeCopyForRecall */
	/* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
	inline void switchDiskCopiesForJob() throw (castor::exception::Exception) { }; 


	/**********************************************/
	/* return if it is to replicate considering: */
	/* - sources.size() */
	/* - maxReplicaNb */
	/* - replicationPolicy (call to the expert system) */
	/**********************************************************************************/
	bool replicaSwitch() throw(castor::exception::Exception);


	/***************************************************************************************************************************/
	/* if the replicationPolicy exists, ask the expert system to get maxReplicaNb for this file                                */
	/**************************************************************************************************************************/
	int checkReplicationPolicy() throw(castor::exception::Exception);


	/************************************************************************************/
	/* process the replicas = build rfs string (and hostlist) */
	/* diskServerName and mountPoint getting from sources (=diskCopyForRecall)*/
	/* - rfs + = ("|") + diskServerName + ":" + mountPoint */
	/**********************************************************************************/
	void processReplica() throw(castor::exception::Exception);

	
	/*******************************************************************************************/
	/* build the rmjob needed structures(buildRmJobHelperPart() and buildRmJobRequestPart())  */
	/* and submit the job  */
	/****************************************************************************************/
	void jobManagerPart() throw(castor::exception::Exception);
  
  
	bool notifyJobManager() {
	  return m_notifyJobManager;
	}
	
      protected:
	
	unsigned int maxReplicaNb;
	std::string replicationPolicy;
	std::string default_protocol;


	std::string rfs;

	std::list<castor::stager::DiskCopyForRecall *> sources;

	unsigned int xsize;	
  
	bool m_notifyJobManager;

      }; //end class StagerJobRequestHandler

      

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif





