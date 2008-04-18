/*******************************************************************************************************/
/* Base class for JobRequestHandler and all the fileRequest handlers                            */
/* Basically: handle() as METHOD  and  (stgRequestHelper,stgCnsHelper,stgReplyHelper)  as ATTRIBUTES */
/****************************************************************************************************/


#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/RequestHandler.hpp"
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

#include "serrno.h"
#include <serrno.h>

#include <string>
#include <iostream>




namespace castor{
  namespace stager{
    namespace daemon{
      
      /********************************************************************/
      /* function to perform the common flow for all the __Handler */
      /* basically, calls to the helpers to create the objects and   */
      /* overwriten in RmHandler, UpdateHandler, PrepareToUpdateHandler */
      /* to make the necessary links on DB */
      /************************************/
      void RequestHandler::preHandle() throw(castor::exception::Exception)
      {
        
        /* get the uuid request string version and check if it is valid */
        stgRequestHelper->setRequestUuid();
        
        /* we create the CnsHelper inside and we pass the requestUuid needed for logging */
        this->stgCnsHelper = new CnsHelper(stgRequestHelper->requestUuid);
        
        /* set the username and groupname needed to print them on the log */
        stgRequestHelper->setUsernameAndGroupname();
        
        /* get the uuid subrequest string version and check if it is valid */
        /* we can create one !*/
        stgRequestHelper->setSubrequestUuid();
        
        /* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
        stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
        
        /* get the svcClass */
        stgRequestHelper->getSvcClass();
        
        /* create and fill request->svcClass link on DB */
        stgRequestHelper->linkRequestToSvcClassOnDB();
                
        /* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
        /* create the file if it is needed/possible */
        bool fileCreated = stgCnsHelper->checkFileOnNameServer(stgRequestHelper->subrequest, stgRequestHelper->svcClass);
        
        /* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
        stgRequestHelper->checkFilePermission(fileCreated, stgCnsHelper);
      }
      
      
      /*******************************************************/
      /* since the CnsHelper is created in this class */
      /* it will be deleted in its destructor */
      RequestHandler:: ~RequestHandler() throw(){
        if(stgCnsHelper)
          delete stgCnsHelper;
        stgCnsHelper = 0;
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
