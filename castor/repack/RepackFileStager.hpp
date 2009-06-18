
/******************************************************************************
 *                      RepackFileStager.hpp
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
 * @(#)$RCSfile: RepackFileStager.hpp,v $ $Revision: 1.21 $ $Release$ $Date: 2009/06/18 15:30:29 $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef _REPACKFILESTAGER_HPP_
#define _REPACKFILESTAGER_HPP_


#include "castor/server/IThread.hpp"
#include "RepackServer.hpp"
#include "RepackSubRequest.hpp"
#include "RepackSegment.hpp"
#include "castor/stager/StageRepackRequest.hpp"
#include "stager_client_api.h"


namespace castor {

  namespace repack {


    /**
     * class RepackFileStager
     */
    class RepackFileStager : public castor::server::IThread
    {
    public :

      /**
       * Constructor.
       * @param srv The refernce to the constructing RepackServer Instance.
       */
      RepackFileStager(RepackServer* srv);

      /**
       * Destructor
       */
      ~RepackFileStager() throw();

      ///empty initialization
      virtual void init() {};

      /**
       * Inherited from IThread : not implemented
       */
      virtual void stop() throw();

      /**
       * Checks the DB for new RepackSubRequests in TOBESTAGED/RESTART and
       * performs the procedure.
       * (execute startRepack/restartRepack method, described below).
       */
      virtual void run(void* param) throw();


    private:

      /**
       * Stages the files. It retrieves the files for the tape and
       * sends a StageRepackRequest to the stager. If the submit
       * was successfull the state of the RepackSubRequest is changed
       * to SUBREQUEST_STAGING.
       * @param sreq The RepackSubRequest to stage in files
       * @throws castor::exeption::Exception in case of an error
       */
      std::vector<RepackSegment*>  stageFiles(RepackSubRequest* sreq) throw (castor::exception::Exception);


      /** Sends the request to the stager.
       * The  RepackSubRequest cuuid is updated as soon as the
       * RequestHandler from the Stager answers.
       * @param rsreq The RepackSubRequest to update the cuuid
       * @param req The Request to send
       * @param reqId the returned request id (cuuid) from stager
       * @param opts The Stager options struct
       * @returns The Number of files for which the stager request failed.
       * @throws castor::exeption::Internal in case of an error
       */

      std::vector<RepackSegment*>  sendStagerRepackRequest( RepackSubRequest* rsreq,
                                   castor::stager::StageRepackRequest* req,
                                   std::string *reqId,
                                   struct stage_options* opts
                                   )
        throw (castor::exception::Exception);

      /**
       * Excutes stage_files and updates the RepackSubRequest. In case
       * stage_files gives an error the state is set to FAILED.
       */
      void startRepack(RepackSubRequest* sreq, castor::repack::IRepackSvc* oraSvc ) throw (castor::exception::Exception);


      /**
       * A pointer to the server instance, which keeps information
       * about Nameserver, stager etc.
       */
      RepackServer* ptr_server;
    };



  } //END NAMESPACE REPACK
} //END NAMESPACE CASTOR



#endif //_REPACKFILESTAGER_HPP_
