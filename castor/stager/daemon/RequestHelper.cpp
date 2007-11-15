/******************************************************************************************************************/
/* Helper class containing the objects and methods which interact to performe the processing of the request      */
/* it is needed to provide:                                                                                     */
/*     - a common place where its objects can communicate                                                      */
/*     - a way to pass the set of objects from the main flow (StagerDBService thread) to the specific handler */
/* It is an attribute for all the request handlers                                                           */
/* **********************************************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"


#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"


#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"
#include "osdep.h"
/* to get a instance of the services */
#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/db/DbCnvSvc.hpp"



#include "castor/IClientFactory.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/Constants.hpp"

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"

#include "serrno.h"
#include <errno.h>
#include <vector>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>




namespace castor{
  namespace stager{
    namespace dbService{
      
      
      /* constructor-> return the request type, called on the different thread (job, pre, stg) */
      StagerRequestHelper::StagerRequestHelper(castor::stager::SubRequest* subRequestToProcess, int &typeRequest) throw(castor::exception::Exception){	
        
        try{
          // get thread-safe pointers to services. They were already initialized
          // in the main, so we are sure pointers are valid
          castor::IService* svc = castor::BaseObject::services()->
            service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
          stagerService = dynamic_cast<castor::stager::IStagerSvc*>(svc);
          
          svc = castor::BaseObject::services()->
            service("DbCnvSvc", castor::SVC_DBCNV);
          dbService = dynamic_cast<castor::db::DbCnvSvc*>(svc);
          
          this->baseAddr = new castor::BaseAddress;
          svcClass = 0;
          
          baseAddr->setCnvSvcName("DbCnvSvc");
          baseAddr->setCnvSvcType(SVC_DBCNV);
          
          this->subrequest=subRequestToProcess;
          this->default_protocol = "rfio";
          
          dbService->fillObj(baseAddr, subrequest, castor::OBJ_FileRequest, false); 
          this->fileRequest=subrequest->request();
          
          typeRequest = fileRequest->type();
        }
        catch(castor::exception::Exception e){
          // should never happen: the db service is initialized in the main as well
          castor::dlf::Param params[]={	castor::dlf::Param("Function:","StagerRequestHelper constructor")};
          
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_SERVICES_EXCEPTION, 1 ,params);	    	  
          e.getMessage()<< "Error on the Database";
          throw e;  
          
          
        }
      }
      
      
      /* destructor */
      StagerRequestHelper::~StagerRequestHelper() throw()
      {
        delete baseAddr;
        if(castorFile) {
          if(castorFile->fileClass())
            delete castorFile->fileClass();
          delete castorFile;
        }
        if(fileRequest) {
          delete fileRequest;  // will delete subrequest too
        }
        else if(subrequest) {
          delete subrequest;
        }
        if(svcClass)
          delete svcClass;
      }
      
      
      /***********************************************/
      /* get request uuid (we cannon' t create it!) */
      /* needed to log on dlf */ 
      /*********************************************/
      void StagerRequestHelper::setRequestUuid() throw(castor::exception::Exception)
      {
        try{
          
          if(fileRequest->reqId().empty()){/* we cannon' t generate one!*/
            castor::exception::Exception ex(SEINTERNAL); 
            throw ex;
          }else{/*convert to the Cuuid_t version and copy in our thread safe variable */
            
            if (string2Cuuid(&(this->requestUuid),(char*) fileRequest->reqId().c_str()) != 0) {
              castor::exception::Exception ex(SEINTERNAL);	      
              throw ex;
            }
          }
        }catch(castor::exception::Exception e){
          
          castor::dlf::Param params[]={ castor::dlf::Param("fileName:", subrequest->fileName()),
            castor::dlf::Param("Function:","StagerRequestHelper->setRequestUuid")
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_REQUESTUUID_EXCEPTION, 2 ,params);	    
          
          e.getMessage()<< "Impossible to perform the request";
          throw e; 
        }
        
      }
      
      /* since we already got the requestUuid; from now on, we dont need to use the nullCuuid for dlf */
      
      /************************************************************************************/
      /* set the username and groupname string versions using id2name c function  */
      /**********************************************************************************/
      void StagerRequestHelper::setUsernameAndGroupname() throw(castor::exception::Exception){
        struct passwd *this_passwd = 0;
        struct group *this_gr = 0;
        try{
          uid_t euid = fileRequest->euid();
          uid_t egid = fileRequest->egid();
          
          if((this_passwd = Cgetpwuid(euid)) == NULL){
            castor::exception::Exception ex(SEUSERUNKN);
            throw ex;
          }
          
          if(egid != this_passwd->pw_gid){
            castor::exception::Exception ex(SEINTERNAL);     
            throw ex;
          }
          
          if((this_gr=Cgetgrgid(egid))==NULL){
            castor::exception::Exception ex(SEUSERUNKN);
            throw ex;
            
          }
          
          username = this_passwd->pw_name;
          groupname = this_gr->gr_name;
        }catch(castor::exception::Exception e){
          castor::dlf::Param params[]={ castor::dlf::Param("fileName",subrequest->fileName()),
            castor::dlf::Param("Function:", "StagerRequestHelper->setUsernameAndGroupname")
          };
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_USER_INVALID, 2 ,params);	    
          
          e.getMessage()<< "Invalid user";
          throw e;    
        }
        
      }
      
      /******************************************************************************************/
      /* get and (create or initialize) Cuuid_t subrequest and request                         */
      /* and copy to the thread-safe variables (subrequestUuid and requestUuid)               */
      /***************************************************************************************/
      /* get or create subrequest uuid */
      void StagerRequestHelper::setSubrequestUuid() throw(castor::exception::Exception)
      {
        
        try{
          char subrequest_uuid_as_string[CUUID_STRING_LEN+1];	
          
          if (subrequest->subreqId().empty()){/* we create a new Cuuid_t, copy to thread-safe variable, and update it on subrequest...*/
            Cuuid_create(&(this->subrequestUuid));
            
            
            /* convert to the string version, needed to update the subrequest and fill it on DB*/ 
            if(Cuuid2string(subrequest_uuid_as_string, CUUID_STRING_LEN+1, &(this->subrequestUuid)) != 0){
              castor::exception::Exception ex(SEINTERNAL);
              throw ex;
              
            }else{
              subrequest_uuid_as_string[CUUID_STRING_LEN] = '\0';
              /* update on subrequest */
              subrequest->setSubreqId(subrequest_uuid_as_string);
              /* update it in DB*/
              dbService->updateRep(baseAddr, subrequest, true);	
              
            }
            
          }else{ /* translate to the Cuuid_t version and copy to our thread-safe variable */
            
            if( string2Cuuid(&(this->subrequestUuid), (char *) subrequest_uuid_as_string)!=0){
              castor::exception::Exception ex(SEINTERNAL);
              throw ex;
            }
            
          }
        }catch(castor::exception::Exception e){
          castor::dlf::Param params[]={	castor::dlf::Param("fileName",subrequest->fileName()),
            castor::dlf::Param("UserName",username),
            castor::dlf::Param("GroupName", groupname),
            castor::dlf::Param("Function","StagerRequestHelper->setSubrequestUuid")
          };
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_SUBREQUESTUUID_EXCEPTION, 4 ,params);	   
          
          e.getMessage()<< "Impossible to perform the request";
          throw e;  
          
        }
      }
      
      
      /***********************************************************************************/
      /* get the link (fillObject~SELECT) between fileRequest and its associated client */
      /* using dbService, and get the client                                           */
      /********************************************************************************/
      void StagerRequestHelper::getIClient() throw(castor::exception::Exception){
        
        dbService->fillObj(baseAddr, fileRequest,castor::OBJ_IClient, false);//196
        this->iClient=fileRequest->client();
        this->iClientAsString = IClientFactory::client2String(*(this->iClient));/* IClientFactory singleton */
        
      }
      
      
      
      
      /****************************************************************************************/
      /* get svClass by selecting with stagerService                                         */
      /* (using the svcClassName:getting from request OR defaultName (!!update on request)) */
      /*************************************************************************************/
      void StagerRequestHelper::getSvcClass() throw(castor::exception::Exception){
        this->svcClassName=fileRequest->svcClassName(); 
        
        if(this->svcClassName.empty()){  /* we set the default svcClassName */
          this->svcClassName="default";
          fileRequest->setSvcClassName(this->svcClassName);
        }
        
        
        svcClass=stagerService->selectSvcClass(this->svcClassName);  //check if it is NULL
        if(this->svcClass == NULL) {
          logToDlf(DLF_LVL_USER_ERROR, STAGER_SVCCLASS_EXCEPTION);
          
          castor::exception::Exception ex(SESVCCLASSNFND);
          ex.getMessage()<<"Service Class not found";
          throw ex;
        }
      } 
      
      
      /*******************************************************************************/
      /* update request in DB, create and fill request->svcClass link on DB         */ 
      /*****************************************************************************/
      void StagerRequestHelper::linkRequestToSvcClassOnDB() throw(castor::exception::Exception){
        
        /* update request on DB */
        dbService->updateRep(baseAddr, fileRequest, false);    
        fileRequest->setSvcClass(svcClass);
        
        /* fill the svcClass object using the request as a key  */
        dbService->fillRep(baseAddr, fileRequest,castor::OBJ_SvcClass, false);
        
      }
      
      
      /*******************************************************************************************************************************************/
      /*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) ): called in StagerRequest.jobOriented()*/
      /*****************************************************************************************************************************************/      
      void StagerRequestHelper::getCastorFileFromSvcClass(StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        try{
          // get the fileClass by name
          fileClass = stagerService->selectFileClass(stgCnsHelper->cnsFileclass.name);
          if(fileClass == 0) {
            logToDlf(DLF_LVL_USER_ERROR, STAGER_SVCCLASS_EXCEPTION);
            castor::exception::Internal ex;
            ex.getMessage() << "No fileclass " << stgCnsHelper->cnsFileclass.name << " in DB";
            throw ex;
          }
          u_signed64 fileClassId = fileClass->id();
          u_signed64 svcClassId = svcClass->id();
        
          // get the castorFile from the stagerService and fill it on the subrequest
          castorFile = stagerService->selectCastorFile(stgCnsHelper->cnsFileid.fileid,
            stgCnsHelper->cnsFileid.server,svcClassId, fileClassId, stgCnsHelper->cnsFilestat.filesize,subrequest->fileName());
          
          subrequest->setCastorFile(castorFile);
          castorFile->setFileClass(fileClass);
          
          // create links in db and in memory
          dbService->fillRep(baseAddr, subrequest, castor::OBJ_CastorFile, false);
          //dbService->fillObj(baseAddr, castorFile, castor::OBJ_SvcClass, false);   not needed?!	
          dbService->fillRep(baseAddr, castorFile, castor::OBJ_FileClass, false);
        }
        catch(castor::exception::Exception e){
          logToDlf(DLF_LVL_ERROR, STAGER_CASTORFILE_EXCEPTION, &(stgCnsHelper->cnsFileid));
          
          castor::exception::Exception ex(e.code());
          ex.getMessage()<<"Impossible to perform the request: " << e.getMessage().str();
          throw(ex);
        }
      }
      
      
      /*****************************************************************************************************/
      /* check if the user (euid,egid) has the ritght permission for the request's type                   */
      /* note that we don' t check the permissions for SetFileGCWeight and PutDone request (true)        */
      /**************************************************************************************************/
      bool StagerRequestHelper::checkFilePermission() throw(castor::exception::Exception)
      {
        
        bool filePermission = true;
        try{
          
          int type =  this->fileRequest->type();
          std::string filename = this->subrequest->fileName();
          uid_t euid = this->fileRequest->euid();
          uid_t egid = this->fileRequest->egid();
          
          
          switch(type) {
            case OBJ_StageGetRequest:
            case OBJ_StagePrepareToGetRequest:
            case OBJ_StageRepackRequest:
            filePermission=R_OK;
            if ( Cns_accessUser(filename.c_str(),R_OK,euid,egid) == -1 ) {
              filePermission=false; // even if we treat internally the exception, lets gonna use it as a flag
              castor::exception::Exception ex(SEINTERNAL);
              throw ex;
              
            }	   
            break;
            
            case OBJ_StagePrepareToPutRequest:
            case OBJ_StagePrepareToUpdateRequest:
            case OBJ_StagePutRequest:
            case OBJ_StageRmRequest:
            case OBJ_StageUpdateRequest:
            case OBJ_StagePutDoneRequest:
            case OBJ_SetFileGCWeight:
            filePermission=W_OK;
            if ( Cns_accessUser(filename.c_str(),W_OK,euid,egid) == -1 ) {
              filePermission=false; // even if we treat internally the exception, lets gonna use it as a flag
              castor::exception::Exception ex(SEINTERNAL);
              throw ex;
              
            }
            break;
            
            default:
            break;
          }
        }catch(castor::exception::Exception e){
          logToDlf(DLF_LVL_USER_ERROR, STAGER_USER_PERMISSION);	    
          e.getMessage()<< "Access denied";
          throw e;  
        }
        return(filePermission);
      }
      
            
      void StagerRequestHelper::logToDlf(int level, int messageNb, Cns_fileid* fid) throw()
      {
        castor::dlf::Param params[] = {
          castor::dlf::Param(subrequestUuid),
          castor::dlf::Param("reqType", 
            (fileRequest->type() < castor::ObjectsIdsNb ? 
             castor::ObjectsIdStrings[fileRequest->type()] : "Unknown")),
          castor::dlf::Param("fileName", subrequest->fileName()),
          castor::dlf::Param("userName", username),
          castor::dlf::Param("groupName", groupname),
          castor::dlf::Param("svcClass", svcClassName)
        };
        castor::dlf::dlf_writep(requestUuid, level, messageNb, 6, params, fid);	    
      } 
        
      
      
      
      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
