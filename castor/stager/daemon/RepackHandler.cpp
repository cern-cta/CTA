/**********************************************************************************************/
/* StagerRepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#include "castor/stager/dbService/StagerRepackHandler.hpp"

namespace castor{
  namespace stager{
    namespace dbService{
      
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
            result = true;
            break;
          
          case -1:
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
            break;
          
          case 0:   
            // DiskCopy STAGED: the repack migration has been already started, just answer the client
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_REPACK_MIGRATION, &(stgCnsHelper->cnsFileid));
            result = true;
            break;
          
          case 2:
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, &(stgCnsHelper->cnsFileid));
            
            // if success, answer client
            result = stgRequestHelper->stagerService->createRecallCandidate(
              stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);
            break;
          
          default:
            break;
        }
        return result;
      }
    
  }//end namespace dbService
 }//end namespace stager
}//end namespace castor
