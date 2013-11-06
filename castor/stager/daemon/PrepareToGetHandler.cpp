/*****************************************************************************
 *                      PrepareToGetHandler.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * Implementation of the prepareToGet subrequest's handler
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/daemon/PrepareToGetHandler.hpp"

namespace castor{
  namespace stager{
    namespace daemon{
      
      /**
       * constructor
       */
      PrepareToGetHandler::PrepareToGetHandler(RequestHelper* reqHelper)
        throw(castor::exception::Exception) :
        OpenRequestHandler(reqHelper), m_handlePGetStatement(0) {
      };

      /**
       * handler for the prepareToGet requests
       */
      bool PrepareToGetHandler::handle() throw (castor::exception::Exception) {
        // call parent's handler method
        OpenRequestHandler::handle();
        /* get the castorFile entity and populate its links in the db */
        reqHelper->getCastorFile();
        // call PL/SQL handlePrepareToGet method
        ReplyHelper* stgReplyHelper = NULL;
        castor::stager::DiskCopyInfo *diskCopy = NULL;
        try {
          castor::stager::SubRequestStatusCodes srStatus =
            reqHelper->stagerService->handlePrepareToGet(reqHelper->castorFile->id(),
                                                         reqHelper->subrequest->id(),
                                                         reqHelper->cnsFileid,
                                                         reqHelper->cnsFilestat.filesize,
                                                         reqHelper->m_stagerOpenTimeInUsec);
          reqHelper->subrequest->setStatus(srStatus);
          if (srStatus == SUBREQUEST_WAITTAPERECALL) { 
            // reset the filesize to the nameserver one, as we don't have anything in the db
            reqHelper->subrequest->castorFile()->setFileSize(reqHelper->cnsFilestat.filesize);
          }
          // answer client if needed
          if (srStatus != SUBREQUEST_FAILED) {
            if (reqHelper->subrequest->answered() == 0) {
              stgReplyHelper = new ReplyHelper();
              if(reqHelper->subrequest->protocol() == "xroot") {
                diskCopy = reqHelper->stagerService->
                  getBestDiskCopyToRead(reqHelper->subrequest->castorFile(),
                                        reqHelper->svcClass);
              }
              if (diskCopy) {
                stgReplyHelper->setAndSendIoResponse(reqHelper,&(reqHelper->cnsFileid), 0, "", diskCopy);
              } else {              
                stgReplyHelper->setAndSendIoResponse(reqHelper,&(reqHelper->cnsFileid), 0, "");
              }
              stgReplyHelper->endReplyToClient(reqHelper);
              delete stgReplyHelper;
              if (diskCopy) delete diskCopy;
            } else {
              // no reply needs to be sent to the client, archive the subrequest
              reqHelper->stagerService->archiveSubReq(reqHelper->subrequest->id(), SUBREQUEST_FINISHED);
            }
          }
        } catch(castor::exception::Exception& e) {
          if (stgReplyHelper != NULL) delete stgReplyHelper;
          if (diskCopy != NULL) delete diskCopy;
          throw(e);
        } 
        return true;
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
