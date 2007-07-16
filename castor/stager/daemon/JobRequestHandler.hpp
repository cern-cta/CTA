/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/
#ifndef STAGER_JOB_REQUEST_HANDLER_HPP
#define STAGER_JOB_REQUEST_HANDLER_HPP 1

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "serrno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"




#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"
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
	/* - hostlist + = (":") + diskServerName ()if it isn' t already in hostlist  */
	/*  */
	/**********************************************************************************/
	void processReplicaAndHostlist() throw(castor::exception::Exception);

	
	/*****************************************************************************************************/
	/* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
	/* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
	/**************************************************************************************************/
	inline void buildRmJobRequestPart() throw(castor::exception::Exception) 
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
	std::list<castor::stager::DiskCopyForRecall *> sources;

	int xsize;
	int openflags;
	bool replyToClient;

	struct rmjob rmjob;
	int nrmjob_out;
	struct rmjob* rmjob_out;
	
	/* flag to dont reply to the client, changeSubrequestStatus and archiveSubrequest */
	/* when "isSubRequestToSchedule" returns "4" or when "diskCopyForRecall == NULL" */
	bool caseSubrequestFailed;

      }; //end class StagerJobRequestHandler

      

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif





