/******************************************************************************
 *                      Collector.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: Collector.cpp,v $ $Author: sponcec3 $
 *
 * The Collector thread of the RmMaster daemon.
 * It collects the data from the different nodes and updates a shared
 * memory representation of it
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/rmmaster/Collector.hpp"

// XXX to be removed !
#include "h/stager_client_api_common.h"

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/IStagerSvc.hpp"

namespace castor {

  namespace rmmaster {

    //------------------------------------------------------------------------------
    // constructor
    //------------------------------------------------------------------------------
    Collector::Collector(castor::stager::IStagerSvc* svc,
                         castor::rmmaster::ClusterStatus* clusterStatus) :
      m_stgService(svc), m_clusterStatus(clusterStatus) {
      stage_trace(3, "Thread Collector created!");
    }

    //------------------------------------------------------------------------------
    // destructor
    //------------------------------------------------------------------------------
    Collector::~Collector() throw() {
      stage_trace(3, "Thread Spring Cleaning killed!");
      m_stgService = 0;
    }

    //------------------------------------------------------------------------------
    // runs the thread starts by threadassign()
    //------------------------------------------------------------------------------
    void Collector::run(void* par) throw() {
      // "Thread Collector started. Updating shared memory data."
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9, 0, 0);
      try{
        if (m_stgService != 0) {
          m_stgService->getClusterStatus(m_clusterStatus);
        }
      }
      catch(castor::exception::Exception e) {
        castor::dlf::Param params[] =
          {castor::dlf::Param("code", sstrerror(e.code())),
           castor::dlf::Param("message", e.getMessage().str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 9, 2, params);
      }
      catch (...) {
        castor::dlf::Param params2[] =
          {castor::dlf::Param("message", "general exception caught")};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 9, 1, params2);
      }
    }


    //------------------------------------------------------------------------------
    // Stops the Thread
    //------------------------------------------------------------------------------
    void Collector::stop() throw() {
      stage_trace(3,"Thread Collector stopped!");
    }

  }  // END Namespace rmmaster

}  // END Namespace castor
