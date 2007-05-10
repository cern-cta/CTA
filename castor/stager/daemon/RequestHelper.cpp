/*************************************************************************************/
/* main container for the classes and methods needed for the stager_db_service.cpp  */
/* *********************************************************************************/


#include "castor/stager/StagerRequestHelper.hpp"
#include "castor/stager/StagerCnsHelper.hpp"
#include "castor/stager/StagerReplyHelper.hpp"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/Subrequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"

#include "stager_uuid.h"
#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{

      /* contructor */
      castor::stager::StagerRequestHelper::StagerRequestHelper(){
	
	this->stagerService =  new castor::stager::IStagerSvc*; 
	this->dbService = new castor::Services*;
	this->baseAddr =  new castor::BaseAddress*; /* the settings ll be done afterwards */
	
	svcClass_to_free = false;
	partitionMask[0] = '\0';

	types = {OBJ_StageGetRequest,OBJ_StagePrepareToGetRequest,OBJ_StageRepackRequest,OBJ_StagePutRequest,OBJ_StagePrepareToPutRequest,OBJ_StageUpdateRequest,OBJ_StagePrepareToUpdateRequest,OBJ_StageRmRequest,OBJ_SetFileGCWeight,OBJ_StagePutDoneRequest};

      }



      /* destructor */
      castor::stager::StagerRequestHelper::~StagerRequestHelper()
      {
	delete stagerService;
	delete dbService;
	delete baseAddr;
      }



      /********************************************************/
      /*******************************************************/
      /* methods to get/create the fields of the collection */
      /*****************************************************/
      /****************************************************/
   

      /******************************************************************************************/
      /* stagerService, dbService, baseAddr creation: PURE CONSTRUCTOR CALLS*/
      /****************************************************************************************/
      
     

    

      /* used in StagerRmRequest.run()*/
      // to do!!
      void friendStagerServiceStageRm(castor::stager::StagerCnsHelper stgCnsHelper)
      {
	std::string cnsServer(stgCnsHelper.cnsFileidServer());
	this->stagerService->stageRm(stgCnsHelper.cnsFileidFileid(),cnsServer);
      }

      /* used in StagerSetFileGCWeightRequest.run() */
      // to do!!
      void friendStagerServiceSetFileGCWeight(castor::stager::StagerCnsHelper stgCnsHelper,float weight)
      {
	std::string cnsServer(stgCnsHelper.cnsFileidServer());
	this->stagerService->setFileGCWeight(stgCnsHelper.cnsFileidFileid(), cnsServer, weight);
      }



     
      void dbServiceUpdateRep()
      {

	dbService->updateRep(baseAddr, subrequest, STAGER_AUTOCOMMIT_TRUE);
      }
      
      /* baseAddr settings */ 
      void castor::stager::StagerRequestHelper::settingBaseAddress()
      {
	  /* settings */
	  baseAddr->setCnvSvcName("DbCnvSvc");
	  baseAddr->setCnvSvcType("SVC_DB_CNV");

      }
     
      


      /**************************************************************************************/
      /* get/create subrequest->  fileRequest, and many subrequest's attributes            */
      /* and set the subrequest's status and nextStatus                                   */
      /***********************************************************************************/
      
      /* get subrequest using stagerService (and also type) */
      // to keep it!!
      void castor::stager::StagerRequestHelper::getSubrequest()
      {
	this->subrequest=stagerService->subRequestToDo(&types);
      }
      


      


      /* at the end of each subrequest's handler, call this function to update the subrequest.status */
      /* and subrequest.nextStatus, if it is needed  */
      void checkAndUpdateSubrequestStatusAndNext(SubRequestStatusCodes status, SubRequestGetNextStatusCodes nextStatus)
      {
	
      }
     


    




      /* get the link (fillObject~SELECT) between the subrequest and its associated fileRequest  */ 
      /* using dbService, and get the fileRequest */ 
      void castor::stager::StagerRequestHelper::getFileRequest() 
      {
	dbService->fillObj(baseAddr, subrequest, OBJ_FileRequest, 0);
	fileRequest=subrequest->request();
      }
    
    


      /*****************************************************************************/
      /* get the link (fillObject~SELECT) between request and its associated client*/
      /* using dbService, and get the client                                     */
      /**************************************************************************/
      void castor::stager::StagerRequestHelper::getIClient()
      {
	  dbService->fillObj(baseAddr, fileRequest,OBJ_IClient, 0);
	  iClient=fileRequest->client();
	  iClientAsString=IClientFactory.client2string(iClient);/* IClientFactory singleton */
      }
	


      
      
      /****************************************************************************************/
      /* get svClass by selecting with stagerService                                         */
      /* (using the svcClassName:getting from request OR defaultName (!!update on request)) */
      /* and get its attributes: maxReplicationNb and replicationPollicy                   */
      /************************************************************************************/
      void castor::stager::StagerRequestHelper::getSvcClass()
      {

	  std::string className=fileRequest->svcClassName(); 
	  
	  if((className==NULL)||(className.empty())){  /* we set the default className */
	    className="default";
	    fileRequest->setSvcClassName(className);
	    
	    className=fileRequest->svcClassName(); /* we retrieve it to know if it has been correctly update */
	    if((className==NULL)||(className.empty())){
	      //print message error and throw exception!
	    }
	  }
	  svcClass=stagerService->selectSvcClass(className);//check if it is NULL
	  if(svcClass == NULL){
	    svcClass_to_free = true;
	  }
      }
      
      /* get the maxReplicaNb and the replicationPolicy svcClass's attributes */
      int svcClassMaxReplicNb()
      {
	return(this->svcClass->maxReplicaNb());
      }
      std::string svcClassReplicationPolicy()
      {
	return(this->svcClass->replicationPolicy());
      }
      
      
      /*******************************************************************************/
      /* update request in DB, create and fill request->svcClass link on DB         */ 
      /*****************************************************************************/
      void castor::stager::StagerRequestHelper::linkRequestToSvcClassOnDB()
      {
	/* update request on DB */
	dbService->updateRep(baseAddr, fileRequest, STAGER_AUTOCOMMIT_TRUE);//EXCEPTION!
	fileRequest->setSvcClass(svcClass);
 	/* fill the svcClass object using the request as a key  */
	dbService->fillRep(baseAddr, fileRequest,OBJ_SvcClass,STAGER_AUTOCOMMIT_TRUE);
      }


      
      
      
      


      /*******************************************************************************/
      /* get the castoFile associated to the subrequest                             */ 
      /*****************************************************************************/
      void castor::stager::StagerRequestHelper::getCastorFile()
      {
	this->castorFile=subrequest->castorFile();
      }
      

      
      
      /****************************************************************************************/
      /* get fileClass by selecting with stagerService                                       */
      /* using the CnsfileClass.name as key      (in StagerRequest.JobOriented)             */
      /*************************************************************************************/
      void castor::stager::StagerRequestHelper::getFileClass(char* nameClass)
      {
	
	  std::string fileClassName(nameClass);
	  fileClass=stagerService->selectFileClass(fileClassName);//throw exception if it is null
      }



      
      /******************************************************************************************/
      /* get and (create or initialize) Cuuid_t subrequest and request                         */
      /* and copy to the thread-safe variables (subrequestUuid and requestUuid)               */
      /***************************************************************************************/

      /* get or create subrequest uuid */
      void castor::stager::StagerRequestHelper::setSubrequestUuid()
      {
	Cuuid_t subrequest_uuid=NOT_VALID;//if we manage to get the cuuid version, we update 
	char subrequest_uuid_as_string[CUUID_STRING_LEN+1];	

	if (subrequest->subreqId().empty()){/* we create a new Cuuid_t, copy to thread-safe variable, and update it on subrequest...*/
	  Cuuid_create(&subrequest_uuid);
	  this->subrequestUuid = subrequest_uuid;/* COPY TO THREAD-SAFE VARIABLE*/
	  
	  /* convert to the string version, needed to update the subrequest and fill it on DB*/ 
	  if(Cuuid2string(subrequest_uuid_as_string, CUUID_STRING_LEN+1, &subrequest_uuid) != 0){
	    /* Is that fatal ? No a priori */
	    /* to translate to the dlf mode:   STAGER_LOG_SYSCALL(fileid,"string2Cuuid"); */
	  }else{
	    subrequest_uuid_as_string[CUUID_STRING_LEN] = '\0';
	    /* update on subrequest */
	    subrequest->setSubreqId(subrequest_uuid_as_string);
	    /* update it in DB*/
	    
	    if(dbService->updateRep(baseAddr, subrequest, STAGER_AUTOCOMMIT_TRUE) != 0 ){
		/* throw exception!!!*/
	    }
	    
	  }
	  
	}else{ /* translate to the Cuuid_t version and copy to our thread-safe variable */
	  if( string2Cuuid(&subrequest_uuid, (char *) subrequest_uuid_as_string)!=0){
	    /* Is that fatal ? No a priori */
	    /* to translate to the dlf mode:   STAGER_LOG_SYSCALL(fileid,"string2Cuuid"); */
	  }else{
	    this->subrequestUuid = subrequest_uuid; /* COPY TO THREAD-SAFE VARIABLE*/ 
	  }
	}
		
      }
      
      /* get request uuid (we cannon' t create it!) */ 
      void castor::stager::StagerRequestHelper::setRequestUuid()
      {
	Cuuid_t request_uuid;

	if(fileRequest->reqId().empty()){/* we cannon' t generate one!*/
	  /* to replace with the dlf: STAGER_LOG_WARNING(fileid,"Request has no uuid");*/
	  
	}else{/*convert to the Cuuid_t version and copy in our thread safe variable */
	   
	  char* uuid_as_string = fileRequest->reqId();
	  if (string2Cuuid(&request_uuid,(char *) uuid_as_string) != 0) {
	    /* Is that fatal ? No a priori */
	    STAGER_LOG_SYSCALL(fileid,"string2Cuuid");
	  } else {
	    this->requestUuid = request_uuid;
	  }
	}

      }





     /****************************************************************************************************/
      /*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) ): called in StagerRequest.jobOriented()*/
      /**************************************************************************************************/
      
      void friendLinkCastorFileToServiceClass(castor::stager::StagerCnsHelper stgCnsHelper)
      {
	unsigned64 fileClassId = fileClass->id();
	unsigned64 svcClassId = svcClass->id();
	
	/* get the castorFile from the stagerService and fill it on the subrequest */
	
	/* we use the stagerService.selectCastorFile to get it from DB */
	castorFile = stagerService->selectCastorFile(stgCnsHelper.cnsFileid.fileid, stgCnsHelper.cnsFileid.server,svcClassId, fileClassId, stgCnsHelper.cnsFilestat.filesize,subrequest->fileName());

	subrequest->setCastorFile(castorFile);
	
	/* create the subrequest-> castorFile on DB*/
	dbService->fillRep(baseAddr, subrequest, OBJ_CastorFile, STAGER_AUTOCOMMIT_TRUE);//throw exception
	
	/* create the castorFile-> svcClass link on DB */
	dbService->fillObj(baseAddr, castorFile, OBJ_SvcClass, 0);//throw exception	
      }



      /****************************************************************************************************/
      /*  check if the castorFile is linked to the service Class: called in StagerRequest.jobOriented()*/
      /**************************************************************************************************/
      void isCastorFileLinkedToSvcClass()
      {
	castor::stager::SvcClass* svcClass_from_castorFile = castorFile->svcClass();

	if(svcClass_from_castorFile == NULL){
	  castorFile->setSvcClass(svcClass);
	  
	  if(this->castorFile == NULL){
	    //throw exception
	  }
	  dbService->fillRep(baseAddr, castorFile, OBJ_SvcClass, STAGER_AUTOCOMMIT_TRUE);//THROW EXCEPTION!

	}else{
	  if(svcClass_to_free){
	    delete [] svcClass; /***** just a call to the delete cpp (including the destructor) ***/
	    this->svcClass_to_free = false;
	  }
	  svcClass = castorFile->svcClass();
	}
      }



      
      /****************************************************************************************************/
      /*  initialize the partition mask with svcClass.name()  or get it:called in StagerRequest.jobOriented() */
      /**************************************************************************************************/
      std::string getOrInitiatePartitionMask()
      {
	if(svcClass->name().empty()){
	  //throw exception!!!
	}else{
	  std::string svcClassName =  svcClass->name();
	  strncpy(partitionMask, svcClassName.c_str(),RM_MAXPARTITIONLEN+1);
	}

	return(this->partitionMask);
      }
      



      /****************************************************************************************************/
      /*  fill castorFile's FileClass: called in StagerRequest.jobOriented()                             */
      /**************************************************************************************************/
      void setFileClassOnCastorFile()
      {
	castorFile->setFileClass(fileClass);
	
	if(castorFile == NULL ){
	    //throw exception!
	}

	dbService->fillRep(baseAddr, castorFile, OBJ_FileClass, STAGER_AUTOCOMMIT_TRUE);
      }


     



      /************************************************************************************/
      /* set the username and groupname string versions using id2name c function  */
      /**********************************************************************************/
      void castor::stager::StagerRequestHelper::setUsernameAndGroupname()
      {
	struct passwd *this_passwd;
	struct group *this_gr;
	
	uid_t euid = fileRequest->euid();
	uid_t egid = fileRequest->egid();

	if((this_passw=Cgetpwuid(euid)) == NULL){
	  serrno=SEURSERUNKN;
	  //throw exception
	}
	
	if(egid != this_passwd->pw_gid){
	  // print:  char buff[1024+1]; Cnsprint...    
	  serrno=EINVAL;
	  //throw exception
	}

	if((this_gr=Cgetgrgid(egid))==NULL){
	  serrno=SEUSERUNKN;
	  //throw exception
      	}

	if(username!=NULL){
	  username.copy(this_passwd->pw_name,RM_MAXUSRNAMELEN);
	}

	if(groupname!=NULL){
	  groupname.copy(this_gr->gr_name,RM_MAXGRPNAMELEN);
	}
       
      }


     


      /****************************************************************************************************/
      /* depending on fileExist and type, check the file needed is to be created or throw exception    */
      /********************************************************************************************/
      bool castor::stager::StagerRequestHelper::isFileToCreateOrException(bool fileExist)
      {

	bool toCreate=FALSE;
	int type = this->subrequest.type();
	int subrequestFlags = this->subrequest.flags();

	if(!fileExist){
	  if ((OBJ_StagePutRequest == type) || (OBJ_StagePrepareToPutRequest == type)||((O_CREAT == (subRequestFlags & O_CREAT))&&((OBJ_StageUpdateRequest == type) ||(OBJ_StagePrepareToUpdateRequest == type)))) {
	    if (serrno == ENOENT) {
	      toCreate=TRUE;
	    } else {
	      
	      //throw exception
	    }
	  }else if(OBJ_StageGetRequest == type) || (OBJ_StagePrepareToGetRequest == type) ||(OBJ_StageRepackRequest == type) ||(OBJ_StageUpdateRequest == type) ||                          (OBJ_StagePrepareToUpdateRequest == type)||(OBJ_StageRmRequest == type) ||(OBJ_SetFileGCWeight == type) ||(OBJ_StagePutDoneRequest == type)){
	  //throw exception
	}
	return(toCreate);
      }



      /*****************************************************************************************************/
      /* check if the user (euid,egid) has the ritght permission for the request's type                   */
      /* note that we don' t check the permissions for SetFileGCWeight and PutDone request (true)        */
      /**************************************************************************************************/
      bool castor::stager::StagerRequestHelper::checkFilePermission()
      {
	bool filePermission = true;
	int type =  this->subrequest.type();
	std::string filename = this->subrequest.filename();
	uid_t euid = this->fileRequest.euid();
	uid_t egid = this->fileRequest.egid();


	switch(type) {
	case OBJ_StageGetRequest:
	case OBJ_StagePrepareToGetRequest:
	case OBJ_StageRepackRequest:
	  filePermission=R_OK;
	  if ( Cns_accessUser(filename,R_OK,euid,egid) == -1 ) {
	    //throw exception
	    filePermission=false; // even if we treat internally the exception, lets gonna use it as a flag
	  }
	  break;
	case OBJ_StagePrepareToPutRequest:
	case OBJ_StagePrepareToUpdateRequest:
	case OBJ_StagePutRequest:
	case OBJ_StageRmRequest:
	case OBJ_StageUpdateRequest:
	  filePermission=W_OK;
	  if ( Cns_accessUser(filename,W_OK,euid,egid) == -1 ) {
	    //throw exception
	    filePermission=false; // even if we treat internally the exception, lets gonna use it as a flag
	    
	  }
	  break;
	default:
	  break;
	}
	return(filePermission);
      }


      /*****************************************************************************************************/
      /* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
      /* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
      /**************************************************************************************************/
      struct rmjob buildRmJobHelperPart(struct rmjob &rmjob)//after processReplica (if it is necessary)
      {


	rmjob.uid = (uid_t) fileRequest->euid();
	rmjob.gid = (uid_t) fileRequest->egid();
	
	strncpy(rmjob.uname, username.c_str(),RM_MAXUSRNAMELEN);
	rmjob.uname[RM_MAXUSRNAMELEN] = '\0';
	strncpy(rmjob.gname, groupname.c_str(),RM_MAXGRPNAMELEN);
	rmjob.gname[RM_MAXGRPNAMELEN] = '\0';

	
	strncpy(rmjob.partitionmask,getOrInitiatePartitionMask(),RM_MAXPARTITIONLEN);//partitionMask: char *
	rmjob.partitionmask[RM_MAXPARTITIONLEN] = '\0';

	std::string features;
	features.rsize(RM_MAXFEATURELEN+1);

	char* className = fileRequest->svcClassName().c_str(); 
	if((className == NULL)||(className[0] == '\0')){
	  className = "default" ;
	}
	features.copy(svcClass.className(), RM_MAXFEATURELEN, 0);
	std::string protocol = subrequest->protocol();
	if(!protocol().empty()){
	  if((features.size()+1+protocol.size())<RM_MAXFEATURELEN){
	    features+= ":";
	    features+= protocol;
	    
	  }
	}else{
	  if((features.size()+1+default_protocol.size())<RM_MAXFEATURELEN){
	    features+=":"+default_protocol;
	  }
	}
	
	strncpy(rmjob.rfeatures, features.c_str(), RM_MAXFEATURELEN);
	               

	
	u64tostr(subrequest.id(),rmjob.stageid,0);
	strcat(rmjob.stageid,"@");
	
	//adding request and subrequest uuid:
	{
	  Cuuid_t thisuuid=requestUuid;
	  Cuuid2string(rmjob.requestid, CUUID_STRING_LEN+1,&thisuuid);
	  thisuuid=subrequestUuid;
	  Cuuid2string(rmjob.subrequestid, CUUID_STRING-LEN+1,&thisuuid);
	}


	std::string client_as_string = IClientFactory.client2String(iClient);
	rmjob.clientStruct = client_as_string.c_str();//what is the max len for client?
	strcpy(rmjob.exec,"/usr/bin/stagerJob.sh");
	strncpy(rmjob.account,fileRequest->svcClassName().c_str(),RM_MAXACCOUNTNAMELEN);
	
	return(rmjob);
      }



     
      /*******************************************************************************/
      /* to get the real subrequest size required on disk                           */
      /* this function is just called for the Update and PrepareToUpdate subrequest*/
      /****************************************************************************/
      u_signed64 castor::stager::StagerRequestHelper::getRealSubrequestXsize(u_signed64 cnsFilestatXsize)
      {
	u_signed64 realXsize;

	
	realXsize = this->subrequest->xsize();
	if(realXsize > 0){
	  
	  if(realXsize < cnsFilestatXsize){
	    //print an error message
	    //user is requesting less bytes than the real size
	  }
	}else{//the user is requiring no bytes!
	  u_signed64 defaultFileSize = this->svcClass->defaultFileSize();
	  if(defaultFileSize <= 0){
	    //print a warning 
	    defaultFilesize = DEFAULTFILESIZE;
	    reaXsize = defaultFilesize;
	    
	  }
	}
	return(realXsize);
      }
      

      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
