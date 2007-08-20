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
#include "serrno.h"
#include "Cns_api.h"
#include "expert_api.h"

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
	/* empty destructor */
	virtual ~StagerJobRequestHandler() throw() {};


    	/* main function which must be implemented on each handler */
	virtual void handle() throw(castor::exception::Exception) = 0;


	

	/****************************************************************************************/
	/* job oriented block  */
	/****************************************************************************************/
	void jobOriented() throw(castor::exception::Exception);


	/****************************************************************************************/
	/* after asking the stagerService is it is toBeScheduled */
	/* - do a normal tape recall*/
	/* - check if it is to replicate:*/
	/*         +processReplica if it is needed: */
	/*                    +make the hostlist if it is needed */
	/****************************************************************************************/
	void switchScheduling(int caseToSchedule) throw(castor::exception::Exception);
	
	
	/************************************************************************************/
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


	/********************************************************************/
	/* for Get, Update, put to sent the notification to the jobManager */
	/******************************************************************/
	void StagerJobRequestHandler::notifyJobManager() throw(castor::exception::Exception);
	

	/* coming from the latest stager_db_service.c */
	/* --------------------------------------------------- */
	/* sendNotification()                                  */
	/*                                                     */
	/* Send a notification message to another CASTOR2      */
	/* daemon using the NotificationThread model. This     */
	/* will wake up the process and trigger it to perform  */
	/* a dedicated action.                                 */
	/*                                                     */
	/* This function is copied from BaseServer.cpp. We     */
	/* could have wrapped the C++ call to be callable in   */
	/* C. However, as the stager is being re-written in    */
	/* C++ anyway we take this shortcut.                   */
	/*                                                     */
	/* Input:  (const char *) host - host to notify        */
	/*         (const int) port - notification por         */
	/*         (const int) nbThreads - number of threads   */
	/*                      to wake up on the server       */
	/*                                                     */
	/* Return: nothing (void). All errors ignored          */
	/* --------------------------------------------------- */
	void sendNotification(const char *host, const int port, const int nbThreads);
      protected:
	
	int maxReplicaNb;
	std::string replicationPolicy;
	std::string default_protocol;


	std::string rfs;

	std::list<castor::stager::DiskCopyForRecall *> sources;

	int xsize;	

      }; //end class StagerJobRequestHandler

      

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif





