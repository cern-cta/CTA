/**********************************************************************************************/
/* RepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the JobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#include "castor/stager/daemon/RepackHandler.hpp"
#include "castor/stager/StageRepackRequest.hpp"

namespace castor{
  namespace stager{
    namespace daemon{
      
      RepackHandler::RepackHandler(RequestHelper* stgRequestHelper) throw (castor::exception::Exception) :
        PrepareToGetHandler(stgRequestHelper)
      {
        this->typeRequest = OBJ_StageRepackRequest;	
      }
      
      bool RepackHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {
        bool result = false;
        switch(stgRequestHelper->stagerService->processPrepareRequest(stgRequestHelper->subrequest)) {
          case -2:
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(stgCnsHelper->cnsFileid));
            stgRequestHelper->subrequest->setStatus(SUBREQUEST_WAITSUBREQ);
            result = true;
            break;
          
          case -1:
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
            break;
          
          case DISKCOPY_STAGED:   
            // the repack migration has been already started, just log it and answer the client
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_REPACK_MIGRATION, &(stgCnsHelper->cnsFileid));
            stgRequestHelper->subrequest->setStatus(SUBREQUEST_REPACK);
            result = true;
            break;
          
          case DISKCOPY_WAITTAPERECALL:
          {
            // reset the filesize to the nameserver one, as we don't have anything in the db
            stgRequestHelper->subrequest->castorFile()->setFileSize(stgCnsHelper->cnsFilestat.filesize);
            // trigger recall, the repack migration will be started at the end of it; answer client only if success
            castor::stager::Tape *tape = new Tape();
            tape->setVid(dynamic_cast<castor::stager::StageRepackRequest*>(stgRequestHelper->fileRequest)->repackVid());
            result = stgRequestHelper->stagerService->createRecallCandidate
              (stgRequestHelper->subrequest, stgRequestHelper->svcClass, tape);
            if (result) {
              // "Triggering Tape Recall"
              castor::dlf::Param params[] = {
                castor::dlf::Param("Type", castor::ObjectsIdStrings[stgRequestHelper->fileRequest->type()]),
                castor::dlf::Param("Filename", stgRequestHelper->subrequest->fileName()),
                castor::dlf::Param("Username", stgRequestHelper->username),
                castor::dlf::Param("Groupname", stgRequestHelper->groupname),
                castor::dlf::Param("SvcClass", stgRequestHelper->svcClass->name()),
                castor::dlf::Param("TPVID", tape->vid()),
                castor::dlf::Param("TapeStatus", castor::stager::TapeStatusCodesStrings[tape->status()]),
                castor::dlf::Param("FileSize", stgRequestHelper->subrequest->castorFile()->fileSize()),
                castor::dlf::Param(stgRequestHelper->subrequestUuid)};
              castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, 9, params, &(stgCnsHelper->cnsFileid));
            } else {
              // no tape copy found because of Tape0 file, log it
              // any other tape error will throw an exception and will be classified as LVL_ERROR 
              stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
            }
            if (tape != 0)
              delete tape;
            }
            break;
	    
          default:
            break;
        }
        return result;
      }
    
  }//end namespace daemon
 }//end namespace stager
}//end namespace castor
