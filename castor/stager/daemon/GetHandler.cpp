/*************************************************************************************/
/* GetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/


#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"
#include "castor/stager/daemon/GetHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "osdep.h"


#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
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

      GetHandler::GetHandler(RequestHelper* stgRequestHelper, CnsHelper* stgCnsHelper) throw (castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StageGetRequest;
      }


      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void GetHandler::handlerSettings() throw(castor::exception::Exception)
      {
        this->maxReplicaNb = stgRequestHelper->svcClass->maxReplicaNb();

        this->default_protocol = "rfio";
      }




      /********************************************/
      /* for Get, Update                          */
      /*     switch(getDiskCopyForJob):           */
      /*        case 0,1: (staged) jobmanagerd    */
      /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
      bool GetHandler::switchDiskCopiesForJob() throw(castor::exception::Exception)
      {
        int result = stgRequestHelper->stagerService->getDiskCopiesForJob(stgRequestHelper->subrequest, sources);

        switch(result) {
          case -3:
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_PREPARETOGET_DISK2DISKCOPY, &(stgCnsHelper->cnsFileid));
            // we notify the jobmanager daemon as we have to wait on diskcopy replication
            m_notifyJobManager = true;
            break;

          case -2:
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(stgCnsHelper->cnsFileid));
            break;

          case -1:
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
            break;

          case DISKCOPY_STAGED:   // schedule job
          case DISKCOPY_WAITDISK2DISKCOPY:
            {
              stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_SCHEDULINGJOB, &(stgCnsHelper->cnsFileid));

              if(result == DISKCOPY_WAITDISK2DISKCOPY) {
                // there's room for internal replication, check if it's to be done
                processReplica();
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
              stgRequestHelper->subrequest->setRequestedFileSystems(rfs);
              stgRequestHelper->subrequest->setXsize(0);   // no size requirements for a Get

              stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
              stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
              stgRequestHelper->dbSvc->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);

              // and we notify the jobmanager daemon
              m_notifyJobManager = true;
            }
            break;

          case DISKCOPY_WAITFS:   // case of update in a prepareToPut: we actually need to behave as a Put and schedule
            {
              castor::stager::DiskCopyForRecall* dc =
                stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile, stgRequestHelper->subrequest);
              if(dc) {
                // cf. also the Put handler where we may have waited on WAITFS_SCHEDULING. This case won't happen here
                if(!dc->mountPoint().empty()) {
                  stgRequestHelper->subrequest->setRequestedFileSystems(dc->diskServer() + ":" + dc->mountPoint());
                }
                stgRequestHelper->subrequest->setXsize(this->xsize);
                stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_CASTORFILE_RECREATION, &(stgCnsHelper->cnsFileid));

                stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
                stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
                try {
                  stgRequestHelper->dbSvc->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
                }
                catch (castor::exception::Exception e) {
                  // should never happen, we forward any exception and we delete the object to avoid a memory leak
                  delete dc;
                  throw(e);
                }
                // and we notify the jobmanager daemon
                m_notifyJobManager = true;
                delete dc;
              }
              else {
                stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_RECREATION_IMPOSSIBLE, &(stgCnsHelper->cnsFileid));
              }
            }
            break;

          case DISKCOPY_WAITTAPERECALL:   // create a tape copy and corresponding segment objects on stager catalogue
            {
              // reset the filesize to the nameserver one, as we don't have anything in the db
              stgRequestHelper->subrequest->castorFile()->setFileSize(stgCnsHelper->cnsFilestat.filesize);
              // first check the special case of 0 bytes files
              if (0 == stgCnsHelper->cnsFilestat.filesize) {
                stgRequestHelper->stagerService->createEmptyFile(stgRequestHelper->subrequest, true);
                // Note that all the process of updating the subrequest is done in PL/SQL,
                // allowing to have a single round trip to the DB
                // and we notify the jobmanager daemon
                m_notifyJobManager = true;
                break;
              }
              // Create recall candidates
              castor::stager::Tape *tape = 0;
              result = stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest, stgRequestHelper->svcClass, tape);
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

        }
        return false;
      }


      /****************************************************************************************/
      /* handler for the get subrequest method */
      /****************************************************************************************/
      void GetHandler::handle() throw (castor::exception::Exception)
      {
        try{
          handlerSettings();

          stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_GET, &(stgCnsHelper->cnsFileid));

          jobOriented();
          switchDiskCopiesForJob();

        }catch(castor::exception::Exception e){
          if(e.getMessage().str().empty()) {
            e.getMessage() << sstrerror(e.code());
          }
          castor::dlf::Param params[]={
            castor::dlf::Param("Error Code", e.code()),
            castor::dlf::Param("Error Message", e.getMessage().str())
          };
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_GET, 2, params, &(stgCnsHelper->cnsFileid));
          throw(e);
        }
      }


      /***********************************************************************/
      /* decides whether we need to replicate                                */
      /***********************************************************************/
      void GetHandler::processReplica() throw(castor::exception::Exception)
      {
        bool replicate = true;

        if( (sources.size() == 1 && sources.front()->status() == DISKCOPY_STAGEOUT)
            || stgRequestHelper->fileRequest->type() == OBJ_StageUpdateRequest
            || (maxReplicaNb > 0 && maxReplicaNb <= sources.size()) ) {
          // A Get on a STAGEOUT DiskCopy is not allowed to replicate,
          // nor is allowed an Update request. Otherwise, maxReplicaNb > 0 decides
          replicate = false;
        }
        // XXX Note that maxReplicaNb == 0 allows infinite replication (to be reviewed maybe)

        if(replicate) {
          // We need to replicate, create a diskCopyReplica request
          stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_GET_REPLICATION, &(stgCnsHelper->cnsFileid));
          stgRequestHelper->stagerService->createDiskCopyReplicaRequest
	    (stgRequestHelper->subrequest, sources.front(),
	     stgRequestHelper->svcClass, stgRequestHelper->svcClass, true);
        }
      }


      GetHandler::~GetHandler()throw(){
      }

    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
