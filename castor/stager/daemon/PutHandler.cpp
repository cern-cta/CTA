/****************************************************************************/
/* PutHandler: Constructor and implementation of Put request handler */
/**************************************************************************/


#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/PutHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace daemon{
      
      /*********************************/
      /* handler for the Put request  */
      /*******************************/
      void PutHandler::handle() throw (castor::exception::Exception)
      {
        OpenRequestHandler::handle();
        handlePut();
      }
      
      void PutHandler::handlePut() throw (castor::exception::Exception)
      {  
        castor::stager::DiskCopyForRecall* dc = 0;
        try{
          reqHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PUT, &(reqHelper->cnsFileid));
          
          
          /* use the stagerService to recreate castor file */
          dc = reqHelper->stagerService->recreateCastorFile(reqHelper->castorFile, reqHelper->subrequest);
          
          if(dc) {
            // check for request in wait due to concurrent puts
            if(dc->status() == DISKCOPY_WAITFS_SCHEDULING) {
              reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(reqHelper->cnsFileid));
              // we don't need to do anything, the request will be restarted
              // however we have to commit the transaction
              reqHelper->dbSvc->commit();
            }
            else {
              // schedule the put
              reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_CASTORFILE_RECREATION, &(reqHelper->cnsFileid));
              if(!dc->mountPoint().empty()) {
                reqHelper->subrequest->setRequestedFileSystems(dc->diskServer() + ":" + dc->mountPoint());
              }
              
              if(!reqHelper->subrequest->xsize()) {
                // use the defaultFileSize
                reqHelper->subrequest->setXsize(reqHelper->svcClass->defaultFileSize());
              }
              reqHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
              reqHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);	      
              reqHelper->dbSvc->updateRep(reqHelper->baseAddr, reqHelper->subrequest, true);
            }
            delete dc;
          }
          else {
            reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_RECREATION_IMPOSSIBLE, &(reqHelper->cnsFileid));
          }
        }
        catch(castor::exception::Exception& e) {
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_ERROR, STAGER_PUT, 2 ,params, &(reqHelper->cnsFileid));
          if(dc) delete dc;
          throw(e);
        }
      }/* end PutHandler::handle()*/
      
      
      
    }// end daemon namespace
  }// end stager namespace
}//end castor namespace 

