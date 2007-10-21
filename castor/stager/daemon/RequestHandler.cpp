/*******************************************************************************************************/
/* Base class for StagerJobRequestHandler and all the fileRequest handlers                            */
/* Basically: handle() as METHOD  and  (stgRequestHelper,stgCnsHelper,stgReplyHelper)  as ATTRIBUTES */
/****************************************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"
#include "castor/ObjectSet.hpp"



#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"


#include "serrno.h"
#include <serrno.h>

#include <string>
#include <iostream>




namespace castor{
  namespace stager{
    namespace dbService{

      /********************************************************************/
      /* function to perform the common flow for all the Stager__Handler */
      /* basically, calls to the helpers to create the objects and   */
      /* overwriten in RmHandler, UpdateHandler, PrepareToUpdateHandler */
      /* to make the necessary links on DB */
      /************************************/
      void StagerRequestHandler::preHandle() throw(castor::exception::Exception)
      {
		
	
	/* get the uuid subrequest string version and check if it is valid */
	/* we can create one !*/
	stgRequestHelper->setSubrequestUuid();
	
	/* get the associated client and set the iClientAsString variable */
	stgRequestHelper->getIClient();
	
	
	/* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
	stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
	
	/* get the uuid request string version and check if it is valid */
	stgRequestHelper->setRequestUuid();
	
	
	/* get the svcClass */
	stgRequestHelper->getSvcClass();
	
	
	/* create and fill request->svcClass link on DB */
	stgRequestHelper->linkRequestToSvcClassOnDB();
		
	/* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
	/* create the file if it is needed/possible */
	stgCnsHelper->checkAndSetFileOnNameServer(this->typeRequest, stgRequestHelper->subrequest->flags(), stgRequestHelper->subrequest->modeBits(), stgRequestHelper->svcClass);
	
	/* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
	stgRequestHelper->checkFilePermission();
	
	castor::dlf::Param parameter[] = {castor::dlf::Param("Standard Message","(RequestSvc) Detailed subrequest(fileName,{euid,egid},{userName,groupName},mode mask, cliendId, svcClassName,cnsFileid.fileid, cnsFileid.server"),
					  castor::dlf::Param("Standard Message",stgCnsHelper->subrequestFileName),
					  castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->euid),
					  castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->egid),
					  castor::dlf::Param("Standard Message",stgRequestHelper->username),
					  castor::dlf::Param("Standard Message",stgRequestHelper->groupname),
					  castor::dlf::Param("Standard Message",stgRequestHelper->fileRequest->mask()),
					  castor::dlf::Param("Standard Message",stgRequestHelper->iClient->id()),
					  castor::dlf::Param("Standard Message",stgRequestHelper->svcClassName),
					  castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.fileid),
					  castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.server)};
	castor::dlf::dlf_writep( nullCuuid, DLF_LVL_USAGE, 1, 11, parameter);
       
      }
      
   }//end namespace dbService
  }//end namespace stager
}//end namespace castor
