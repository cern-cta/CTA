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
      RepackMonitor(RepackServer* svr);
      ~RepackMonitor();
      
      void run(void *param) throw();
      void stop() throw();


      RepackSubRequest* getFinishedTapes() throw (castor::exception::Internal);

      void updateTape(RepackSubRequest*)   throw (castor::exception::Internal);
      

      /** Retrieves the stats from a request (by the given cuuid in the 
       *  RepackSubRequest).
       *  @param sreq The RepackSubRequest to check
       *  @param responses The allocated stager API file response struct
       *  @param nbresps The Number of reponses
       *  @returns -1 in case of an error (Message has been written to DLF) 
       */
      int RepackMonitor::getStats(RepackSubRequest* sreq, 
                            struct stage_filequery_resp **responses,
                            int* nbresps)   throw (castor::exception::Internal);

    private:
      DatabaseHelper* m_dbhelper;
      RepackServer* ptr_server;
    };

  }
}

#endif // REPACKMONITOR_HPP

