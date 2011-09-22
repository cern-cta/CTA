/**********************************************************************************************/
/* RepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the OpenRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#include "castor/stager/daemon/RepackHandler.hpp"
#include "castor/stager/StageRepackRequest.hpp"
#include "castor/stager/BulkRequestResult.hpp"
#include "castor/stager/FileResult.hpp"
#include "castor/rh/FileResponse.hpp"
#include "castor/replier/RequestReplier.hpp"

namespace castor{
  namespace stager{
    namespace daemon{
      
      void RepackHandler::handle() throw(castor::exception::Exception) {
        castor::stager::BulkRequestResult *result = 0;
        try{
          // handle the repack subrequest
          castor::stager::BulkRequestResult *result =
            reqHelper->stagerService->handleRepackSubRequest(reqHelper->subrequest->id());
          // Log and prepare client response
          std::vector<castor::stager::FileResult>::const_iterator it =
            result->subResults().begin();
          // "Repack initiated" message
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                                  castor::stager::daemon::STAGER_BLKSTGSVC_REPACK,
                                  it->fileId(), it->nsHost());
          // get a pointer to the request replier
          castor::replier::RequestReplier *rr = castor::replier::RequestReplier::getInstance();
          // reply to client
          castor::rh::FileResponse res;
          res.setReqAssociated(result->reqId());
          res.setFileId(it->fileId());
          if (it->errorCode() != 0) {
            res.setErrorCode(it->errorCode());
            res.setErrorMessage(it->errorMessage());
          }
          rr->sendResponse(&result->client(), &res, true);
        } catch(castor::exception::Exception& e){
          if (0 != result) {
            std::vector<castor::stager::FileResult>::const_iterator it =
              result->subResults().begin();
            // "Unexpected exception caught"
            castor::dlf::Param params[] =
              {castor::dlf::Param("Function", "BulkStageReqSvcThread::process"),
               castor::dlf::Param("Message", e.getMessage().str()),
               castor::dlf::Param("Code", e.code())};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_REPACK,
                                    it->fileId(), it->nsHost(), 3, params);
          } else {
            // "Unexpected exception caught"
            castor::dlf::Param params[] =
              {castor::dlf::Param("Function", "BulkStageReqSvcThread::process")};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT,
                                    0, "", 1, params);
          }
          throw(e);
        }
      }
    
  }//end namespace daemon
 }//end namespace stager
}//end namespace castor
