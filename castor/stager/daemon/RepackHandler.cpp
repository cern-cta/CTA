/**********************************************************************************************/
/* RepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the OpenRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#include "castor/stager/daemon/RepackHandler.hpp"
#include "castor/stager/StageRepackRequest.hpp"
#include "castor/stager/Tape.hpp"

namespace castor{
  namespace stager{
    namespace daemon{
      
      bool RepackHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {
        bool result = false;
        switch(reqHelper->stagerService->processPrepareRequest(reqHelper->subrequest)) {
          case -2:
            reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(reqHelper->cnsFileid));
            reqHelper->subrequest->setStatus(SUBREQUEST_WAITSUBREQ);
            result = true;
            break;
          
          case -1:
            reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(reqHelper->cnsFileid));
            break;
          
          case DISKCOPY_STAGED:   
            // the repack migration has been already started, just log it and answer the client
            reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_REPACK_MIGRATION, &(reqHelper->cnsFileid));
            reqHelper->subrequest->setStatus(SUBREQUEST_REPACK);
            result = true;
            break;
          
          case DISKCOPY_WAITTAPERECALL:
          {
            // reset the filesize to the nameserver one, as we don't have anything in the db
            reqHelper->subrequest->castorFile()->setFileSize(reqHelper->cnsFilestat.filesize);
            // trigger recall, the repack migration will be started at the end of it; answer client only if success
            castor::stager::Tape *tape = new Tape();
            tape->setVid(dynamic_cast<castor::stager::StageRepackRequest*>(reqHelper->fileRequest)->repackVid());
            result = reqHelper->stagerService->createRecallCandidate
              (reqHelper->subrequest, reqHelper->svcClass, tape);
            if (result) {
              // "Triggering Tape Recall"
              castor::dlf::Param params[] = {
                castor::dlf::Param("Type", castor::ObjectsIdStrings[reqHelper->fileRequest->type()]),
                castor::dlf::Param("Filename", reqHelper->subrequest->fileName()),
                castor::dlf::Param("Username", reqHelper->username),
                castor::dlf::Param("Groupname", reqHelper->groupname),
                castor::dlf::Param("SvcClass", reqHelper->svcClass->name()),
                castor::dlf::Param("TPVID", tape->vid()),
                castor::dlf::Param("TapeStatus", castor::stager::TapeStatusCodesStrings[tape->status()]),
                castor::dlf::Param("FileSize", reqHelper->subrequest->castorFile()->fileSize()),
                castor::dlf::Param(reqHelper->subrequestUuid)};
              castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, 9, params, &(reqHelper->cnsFileid));
            } else {
              // no tape copy found because of Tape0 file, log it
              // any other tape error will throw an exception and will be classified as LVL_ERROR 
              reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(reqHelper->cnsFileid));
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
