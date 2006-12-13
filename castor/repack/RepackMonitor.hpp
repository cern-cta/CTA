/******************************************************************************
 *                      castor/RepackMonitor.hpp
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
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/
#ifndef REPACKMONITOR_HPP
#define REPACKMONITOR_HPP 1

#include "RepackCommonHeader.hpp"
#include "castor/rh/FileQryResponse.hpp"
#include "DatabaseHelper.hpp"
#include "FileListHelper.hpp"
#include "castor/server/IThread.hpp"
#include "stager_client_api.h"


namespace castor {
  namespace repack {

  /** forward declaration */
  class RepackServer;
    

  class RepackMonitor : public castor::server::IThread {

    public:
      /**
       * The Constructor 
       */
      RepackMonitor(RepackServer* svr);
      
      /**
       * The Destructor
       */
      ~RepackMonitor();
      
      /**
       * runs an update of file statistics if all available RepackSubRequests.
       */
      void run(void *param) throw();

      /**
       * Not implemented
       */
      void stop() throw();


      /**
       * Updates the file statistics from a RepackSubRequest by querying the
       * assigned stager (in the RepackRequest). It takes the ServiceClass 
       * information of the RepackRequest into account)
       * @throws castor::exception::Exception in case of an error.
       */
      void updateTape(RepackSubRequest*)   throw (castor::exception::Internal);
      

      /** Retrieves the stats from a request (by the given cuuid in the 
       *  RepackSubRequest). Beware that the returned objects in the vector
       *  have to be deleted by the caller.
       *  @param sreq The RepackSubRequest to check.
       *  @param fr A vector with the allocated FileResponses.
       *  @throws castor::exception::Exception in case of an error.
       */
      void RepackMonitor::getStats(RepackSubRequest* sreq,
                                   std::vector<castor::rh::FileQryResponse*>* fr)
                                             throw (castor::exception::Exception);

    private:
      DatabaseHelper* m_dbhelper;
      RepackServer* ptr_server;
    };

  }
}

#endif // REPACKMONITOR_HPP

