/*************************************************************************************/
/* GetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "Cns_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/Tape.hpp"

#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/GetHandler.hpp"


namespace castor{
  namespace stager{
    namespace daemon{

      /********************************************/
      /* for Get, Update                          */
      /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
      bool GetHandler::switchDiskCopiesForJob() throw(castor::exception::Exception)
      {
        int result = reqHelper->stagerService->getDiskCopiesForJob(reqHelper->subrequest, sources);

        switch(result) {
          case -3:
            reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_PREPARETOGET_DISK2DISKCOPY, &(reqHelper->cnsFileid));
            break;

          case -2:
            reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(reqHelper->cnsFileid));
            break;

          case -1:
            reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(reqHelper->cnsFileid));
            break;

          case DISKCOPY_STAGED:   // schedule job
          case DISKCOPY_WAITDISK2DISKCOPY:
            {
              reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_SCHEDULINGJOB, &(reqHelper->cnsFileid));

              if(result == DISKCOPY_WAITDISK2DISKCOPY) {
                // we are also performing internal replication, log this
                reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_GET_REPLICATION, &(reqHelper->cnsFileid));
              }

              // Fill the requested filesystems for the request being processed
              std::string rfs = "";
              std::list<castor::stager::DiskCopyForRecall*>::iterator it;
              for(it = sources.begin(); it != sources.end(); it++) {
                if(!rfs.empty()) rfs += "|";
                rfs += (*it)->diskServer() + ":" + (*it)->mountPoint();
                delete (*it);
              }
              sources.clear();
              reqHelper->subrequest->setRequestedFileSystems(rfs);
              reqHelper->subrequest->setXsize(0);   // no size requirements for a Get

              reqHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
              reqHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
              reqHelper->dbSvc->updateRep(reqHelper->baseAddr, reqHelper->subrequest, true);
            }
            break;

          case DISKCOPY_WAITFS:   // case of update in a prepareToPut: we actually need to behave as a Put and schedule
            {
              castor::stager::DiskCopyForRecall* dc =
                reqHelper->stagerService->recreateCastorFile(reqHelper->castorFile, reqHelper->subrequest);
              if(dc) {
                // cf. also the Put handler where we may have waited on WAITFS_SCHEDULING. This case won't happen here
                if(!dc->mountPoint().empty()) {
                  reqHelper->subrequest->setRequestedFileSystems(dc->diskServer() + ":" + dc->mountPoint());
                }
                reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_CASTORFILE_RECREATION, &(reqHelper->cnsFileid));

                reqHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
                reqHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
                try {
                  reqHelper->dbSvc->updateRep(reqHelper->baseAddr, reqHelper->subrequest, true);
                }
                catch (castor::exception::Exception& e) {
                  // should never happen, we forward any exception and we delete the object to avoid a memory leak
                  delete dc;
                  throw(e);
                }
                delete dc;
              }
              else {
                reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_RECREATION_IMPOSSIBLE, &(reqHelper->cnsFileid));
              }
            }
            break;

          case DISKCOPY_WAITTAPERECALL:   // create a tape copy and corresponding segment objects on stager catalogue
            {
              // reset the filesize to the nameserver one, as we don't have anything in the db
              reqHelper->subrequest->castorFile()->setFileSize(reqHelper->cnsFilestat.filesize);
              // first check the special case of 0 bytes files
              if (0 == reqHelper->cnsFilestat.filesize) {
                reqHelper->stagerService->createEmptyFile(reqHelper->subrequest, true);
                // Note that all the process of updating the subrequest is done in PL/SQL,
                // allowing to have a single round trip to the DB
                break;
              }
              // Create recall candidates
              castor::stager::Tape *tape = 0;
              result = reqHelper->stagerService->createRecallCandidate(reqHelper->subrequest, reqHelper->svcClass, tape);
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

        }
        return false;
      }


      /****************************************************************************************/
      /* handler for the get subrequest method */
      /****************************************************************************************/
      void GetHandler::handle() throw (castor::exception::Exception)
      {
        OpenRequestHandler::handle();
        handleGet();
      }
      
      void GetHandler::handleGet() throw (castor::exception::Exception)
      {
        try{
          reqHelper->logToDlf(DLF_LVL_DEBUG, STAGER_GET, &(reqHelper->cnsFileid));

          switchDiskCopiesForJob();
        }
        catch(castor::exception::Exception& e){
          if(e.getMessage().str().empty()) {
            e.getMessage() << sstrerror(e.code());
          }
          castor::dlf::Param params[]={
            castor::dlf::Param("ErrorCode", e.code()),
            castor::dlf::Param("ErrorMessage", e.getMessage().str())
          };
          castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_ERROR, STAGER_GET, 2, params, &(reqHelper->cnsFileid));
          throw(e);
        }
      }

    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
