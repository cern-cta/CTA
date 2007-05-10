/*********************************************************************************************************************************/
/* Main class container, it includes all the commons objects of all the subrequest                                              */
/* not including: Cns (cos it is more c oriented) and objects related with the response to the client  (cos the aren t commons)*/ 
/******************************************************************************************************************************/




#ifndef STAGER_REQUEST_HELPER_HPP
#define STAGER_REQUEST_HELPER_HPP 1



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



namespace castor{
  namespace stager{
    namespace dbService{

      /* for forward declaration */
      class castor::stager::StagerReplyHelper;



      class StagerRequestHelper :public::castor::IObject{

	friend class castor::stager::StagerReplyHelper;
     

      public:
	  	
	/* services needed: database and stager services*/
	castor::stager::IStagerSvc* stagerService;
	castor::Services* dbService;
    

	/* BaseAddress */
	castor::BaseAddress* baseAddr;
	

	/* subrequest and fileRequest  */
	castor::stager::Subrequest* subrequest;
	
	castor::stager::FileRequest* fileRequest;

	/* client associated to the request */
	castor::IClient* iClient;
	std::string iClientAsString;
	
	/* service class */
	castor::stager::SvcClass* svcClass;
	bool svcClass_to_free;
	char partitionMask[RM_MAXPARTITIONLEN+1];/* initialized to svcClass.name on stagerRequest.jobOriented ()*/
	
	/* castorFile attached to the subrequest*/
	castor::stager::CastorFile* castorFile;
	
	/* get from the stagerService using as key Cnsfileclass.name (JOB ORIENTED)*/
	castor::stager::FileClass* fileclass;
       
	std::string username;
	//[RM_MAXUSRNAMELEN+1];
	std::string groupname;
	//[RM_MAXGRPNAMELEN+1];

	/* Cuuid_t thread safe variables */ 
	Cuuid_t subrequestUuid;
	Cuuid_t requestUuid;
	std::vector<ObjectsIds> types;

	/* list of flags for the object creation */
	


	/****************/
	/* constructor */
	/* destructor */
	StagerRequestHelper();
	~StagerRequestHelper();
	

	/* methods to get the fields of the collection */



	/******************************************************************************************/
	/* stagerService, dbService, baseAddr(and cast to iAddr) creation: PURE CONSTRUCTOR CALLS*/
	/****************************************************************************************/


	/* interface to call  createRecallCandidate() */
	inline void stagerServiceCreateRecallCandidate();
      

	/* interface to call IStagerSvc.isSubrequestToBeScheduled() (return toBeScheduled)*/
	/* ojito con DiskCopyForRecall**   ahora es un doble puntero */
	/* interface to call isSubrequestToBeScheduled() */
	inline int stagerServiceIsSubrequestToSchedule(std::list< castor::stager::DiskCopyForRecall * > **sources);

      /* interface to recreateCastorFile */
	castor::stager::DiskCopyForRecall* castor::stager::StagerRequestHelper::stagerServiceRecreateCastorFile();
      
     
	/* interface to update and check subrequest */
	inline bool stagerServiceUpdateAndCheckSubrequest();
	inline void friendStagerServiceStageRm(castor::stager::StagerCnsHelper stgCnsHelper);
	inline void friendStagerServiceSetFileGCWeight(castor::stager::StagerCnsHelper stgCnsHelper,float weight);


	/* dbService creation*/

	inline void dbServiceUpdateRep();

	/* baseAddr settings  */ 
	void castor::stager::StagerRequestHelper::settingBaseAddress();



	/**************************************************************************************/
	/* subrequest-> (iObj_subrequest) -> fileRequest -> request -> (iObj_request)        */
	/************************************************************************************/

	/* get subrequest using stagerService (and also type) */
	//to keep it!!
	inline void  castor::stager::StagerRequestHelper::getSubrequest();
	
	/****** functions to get the attributes of the request *********/
      

	

	/* at the end of each subrequest's handler, call this function to update the subrequest.status */
	/* and subrequest.nextStatus, if it is needed  */
	inline void checkAndUpdateSubrequestStatusAndNext(SubRequestStatusCodes status, SubRequestGetNextStatusCodes nextStatus);
     

	/****************/
	inline void castor::stager::StagerRequestHelper::getIClient();

	


	/****************************************************************************************/
	/* get svClass by selecting with stagerService                                         */
	/* (using the svcClassName:getting from request OR defaultName (!!update on request)) */
	/*************************************************************************************/
	inline void castor::stager::StagerRequestHelper::getSvcClass();


	/* get the maxReplicaNb and the replicationPolicy svcClass's attributes */
	inline int svcClassMaxReplicNb();
	
	inline std::string svcClassReplicationPolicy();
	
	

	/*******************************************************************************/
	/* update request in DB, create and fill request->svcClass link on DB         */ 
	/*****************************************************************************/
	inline void castor::stager::StagerRequestHelper::linkRequestToSvcClassOnDB();
	
	



	
	/************************************************************************************/
	/* set the username and groupname string versions using id2name c function  */
	/**********************************************************************************/
	inline void castor::stager::StagerRequestHelper::setUsernameAndGroupname();


	
	
      

	
	
	/*******************************************************************************/
	/* get the castoFile associated to the subrequest                             */ 
	/*****************************************************************************/
	inline void castor::stager::StagerRequestHelper::getCastorFile();
	

	/****************************************************************************************/
	/* get fileClass by selecting with stagerService                                       */
	/* using the CnsfileClass.name as key                                                 */
	/*************************************************************************************/
	inline void castor::stager::StagerRequestHelper::getFileClass(char* nameClass);
	

	/******************************************************************************************/
	/* get and (create or initialize) Cuuid_t subrequest and request                         */
	/* and copy to the thread-safe variables (subrequestUuid and requestUuid)*/
	/***************************************************************************************/
	/* get or create subrequest uuid */
	inline void castor::stager::StagerRequestHelper::setSubrequestUuid();
	/* get request uuid (we cannon' t create it!) */ 
      	inline void castor::stager::StagerRequestHelper::setRequestUuid();





	

	/****************************************************************************************************/
	/*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) )*/
	/**************************************************************************************************/
	inline void friendLinkCastorFileToServiceClass(castor::stager::StagerCnsHelper stgCnsHelper);



	/****************************************************************************************************/
	/*  fill castorFile's FileClass: called in StagerRequest.jobOriented()                             */
	/**************************************************************************************************/
	inline void setFileClassOnCastorFile();



	/****************************************************************************************************/
	/*  initialize the partition mask with svcClass.name()  or get it*/
	/**************************************************************************************************/
	inline std::string getOrInitiatePartitionMask();


	
	/****************************************************************************************************/
	/* depending on fileExist and type, check the file needed is to be created or throw exception    */
	/********************************************************************************************/
	inline bool castor::stager::StagerRequestHelper::isFileToCreateOrException(bool fileExist);

	


	/*****************************************************************************************************/
	/* check if the user (euid,egid) has the ritght permission for the request's type                   */
	/* note that we don' t check the permissions for SetFileGCWeight and PutDone request (true)        */
	/**************************************************************************************************/
	inline bool castor::stager::StagerRequestHelper::checkFilePermission();

	/***********************************************************************************/
	/* fill the corresponding fields of the rmjob struct                              */
	/* rmjob will be complete after calling: stg_____request.buildRmJobRequestPart() */
	/********************************************************************************/
	struct rmjob buildRmJobHelperPart(struct rmjob &rmjob);//after processReplica (if it is necessary)




	/****************************************************************************/
	/* to get the real subrequest size required on disk                        */
	/**************************************************************************/
	u_signed64 castor::stager::StagerRequestHelper::getRealSubrequestXsize(u_signed64 cnsFilestatXsize);

      }
    }
  }
}



#endif
