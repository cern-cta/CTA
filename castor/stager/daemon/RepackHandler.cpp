/**********************************************************************************************/
/* StagerRepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#include "castor/stager/daemon/StagerRepackHandler.hpp"

namespace castor{
  namespace stager{
    namespace daemon{
      
      StagerRepackHandler::StagerRepackHandler(StagerRequestHelper* stgRequestHelper) throw (castor::exception::Exception) :
        StagerPrepareToGetHandler(stgRequestHelper)
      {
        this->typeRequest = OBJ_StageRepackRequest;	
      }
      
      bool StagerRepackHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
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
            result = true;
            break;
          
          case DISKCOPY_WAITTAPERECALL:
            // trigger recall, the repack migration will be started at the end of it; answer client only if success
            result = stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest, stgRequestHelper->svcClass);
            if(result) {
              stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, &(stgCnsHelper->cnsFileid));
            }
            else {
              // no tape copy found because of Tape0 file, log it
              // any other tape error will throw an exception and will be classified as LVL_ERROR 
              stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
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
