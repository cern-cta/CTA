/******************************************************************************
 *                      RepackServerReqSvcThread.hpp
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
 * @(#)$RCSfile: RepackWorker.hpp,v $ $Revision: 1.23 $ $Release$ $Date: 2007/03/20 08:11:24 $ $Author: gtaur $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/

#ifndef REPACKWORKER_HPP
#define REPACKWORKER_HPP 1

#include "castor/repack/RepackCommonHeader.hpp" /* the Common Header */
#include <sys/types.h>
#include <time.h>
#include <common.h>     /* for conversion of numbers to char */
#include <vector>
#include <map>



/* ============= */
/* Local headers */
/* ============= */
#include "vmgr_api.h"
#include "castor/IObject.hpp"
#include "castor/server/IThread.hpp"
#include "castor/MessageAck.hpp"
#include "castor/BaseObject.hpp"
#include "castor/io/ServerSocket.hpp"
#include "FileListHelper.hpp"
#include "DatabaseHelper.hpp"
#include "RepackAck.hpp"


namespace castor {

  namespace repack {

    class DatabaseHelper;
    class FileListHelper;
    class RepackServer;
    /**
     * A thread to process Put/Get request and submit them to the stager.
     */
    class RepackWorker : public castor::server::IThread {

    public:

      /**
       * Initializes the db access.
       */

      RepackWorker(RepackServer* pserver);


      /**
       * Standard destructor
       */
      ~RepackWorker() throw();



      virtual void run(void* param);
      virtual void stop() ;

    private:

      /**
       * Retrieves the Information of a tape and returns its status, otherwise -1
       * It also checks, whether the tape is in a valid repack state (FULL).
       * @param vid The Volumeid of the tape
       * @return FULL or -1
       * @throws castor::exception::Exception in case of an error
       */
      int getTapeStatus(std::string tapename) throw (castor::exception::Exception);

      /** executes :
       * 1. checkTapeForRepack(..)
       * 2. getTapeStatus(..)
       * @returns true, if both checks are ok, otherwise false.
       * @throws castor::exception::Exception in case of an error
       */
      bool checkTapeForRepack(std::string) throw (castor::exception::Exception);

      /**
       * Retrieves information about the tapes in a pool and counts the tape
       * in the pool.
       * @param pool the name of the tape pool
       * @throws castor::exception::Exception if the pool does not exist
       */
      int getPoolInfo(castor::repack::RepackRequest* rreq) throw (castor::exception::Exception);

      /**
       *.Validates the RepackRequest and stores it in the DB. The given tapes (RepackSubRequets)
       * status are set to SUBREQUEST_READYFORSTAGING
       * @param RepackRequest The request with the tapes to repack
       * @throws castor::exception::Exception if an error occurs
       */
      void handleRepack(RepackRequest* rreq) throw (castor::exception::Exception);

      /**
       *.Removes a RepackSubRequest in the given RepackRequest. Note that
       * it is not checked if the repack process is finished or not.
       * @param RepackRequest The request with the tapes to remove
       * @throws castor::exception::Exception if an error occurs
       */
      void removeRequest(RepackRequest* rreq) throw (castor::exception::Exception);



      /**
       *.Restarts a RepackSubRequest in the given RepackRequest. Note that
       * it is not checked if the repack process is finished or not.
       * @param RepackRequest The request with the tapes to remove
       * @throws castor::exception::Exception if an error occurs
       */
      void restart(RepackRequest* rreq) throw (castor::exception::Exception);

      /**
       *.Archive a RepackSubRequest in the given RepackRequest, if finished.
       * @param RepackRequest The request with the tapes to be archived
       * @throws castor::exception::Exception if an error occurs
       */
      void archiveSubRequests(RepackRequest* rreq) throw (castor::exception::Exception);

     /**
       * Archive  all finishedRepackSubRequest in the given RepackRequest.
       * @param RepackRequest 
       * @throws castor::exception::Exception if an error occurs
       */
      void archiveAllSubRequests(RepackRequest* rreq) throw (castor::exception::Exception);

      /** Gets the status of one RepackSubRequest from the DB. The given tape vid
       * in the RepackSubRequest (only one is allowed, has to ensured by the
       * repack cliet is searched in the Repack DB and if it is found, returned.
       * The existing RepackRequest is replaced and therefore also deleted.
       * A new one is created and returned.
       * @param RepackRequest The request with the tape to look for
       * @return The RepackRequest corresponding to the given vid.
       * @throws castor::exception::Exception if the tape was not found
       */
      void  getStatus(RepackRequest* rreq) throw (castor::exception::Exception);


      /** Gets the status of all RepackSubRequests from the DB. The given tape vid
       * is searched in the Repack DB and if it is found, returned.
       * The existing RepackSubRequest in the given RepackRequest is replaced
       * and therefore also deleted.
       * @param RepackRequest The request with the tape to look for
       * @throws castor::exception::Exception if the tape was not found
       */
      void getStatusAll(RepackRequest* rreq) throw (castor::exception::Exception);

      /**
       * the DatabaseHelper, which helps to store the Request in the DB.
       */
      DatabaseHelper* m_databasehelper;

      /**
       * The RepackServer instance pointer.
       */
      RepackServer* ptr_server;

    };

  } // end of namespace repack

} // end of namespace castor


#endif // REPACKWORKER_HPP
