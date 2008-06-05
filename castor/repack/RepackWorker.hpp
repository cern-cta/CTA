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
 * @(#)$RCSfile: RepackWorker.hpp,v $ $Revision: 1.27 $ $Release$ $Date: 2008/06/05 16:25:00 $ $Author: gtaur $
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
#include "castor/repack/IRepackSvc.hpp"
#include "castor/repack/RepackAck.hpp"


namespace castor {

  namespace repack {
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

      virtual void init() {};

      virtual void run(void* param);
      virtual void stop() ;

    private:

      bool  checkTapeVmgrStatus(std::string) throw ();

      bool  getPoolVmgrInfo(castor::repack::RepackRequest* rreq) throw ();

      RepackAck* handleRepack(RepackRequest* rreq) throw ();

      RepackAck* removeRequest(RepackRequest* rreq) throw ();

      RepackAck* restartRequest(RepackRequest* rreq) throw ();

      RepackAck* archiveSubRequests(RepackRequest* rreq) throw ();

      RepackAck* archiveAllSubRequests(RepackRequest* rreq) throw ();

      RepackAck*  getNsStatus(RepackRequest* rreq) throw ();


      RepackAck* getStatusAll(RepackRequest* rreq) throw ();

      RepackAck* queryForErrors(RepackRequest* rreq) throw ();
      
      RepackAck* getStatus(RepackRequest* rreq) throw ();


      /**
       * the Database Service, which helps to store the Request in the DB.
       */

      castor::repack::IRepackSvc* m_dbSvc;

      /**
       * The RepackServer instance pointer.
       */
      RepackServer* ptr_server;

    };

  } // end of namespace repack

} // end of namespace castor


#endif // REPACKWORKER_HPP
