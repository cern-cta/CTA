/******************************************************************************
 *                      VdqmMain.cpp
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
 * @author castor dev team
 *****************************************************************************/
 
#include "castor/dlf/Dlf.hpp"
#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/vdqm/DriveSchedulerThread.hpp"
#include "castor/vdqm/RequestHandlerThread.hpp"
#include "castor/vdqm/RTCPJobSubmitterThread.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/VdqmServer.hpp"

#include <stdio.h>
#include <string>


//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {
  try {
    castor::vdqm::VdqmServer       server;
    Cuuid_t                        cuuid                       = cuuid;
    castor::server::BaseThreadPool *requestHandlerThreadPool   = NULL;
    castor::server::BaseThreadPool *driveSchedulerThreadPool   = NULL;
    castor::server::BaseThreadPool *rtcpJobSubmitterThreadPool = NULL;

    Cuuid_create(&cuuid);

    server.logStart(cuuid, argc, argv);
    server.parseCommandLine(cuuid, argc, argv);
    server.initDatabaseService(cuuid);


    //------------------------
    // Create the thread pools
    //------------------------

    {
      int vdqmPort = 0;

      try {
        vdqmPort = server.getListenPort();
      } catch(castor::exception::InvalidConfigEntry &ex) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("invalidValue", ex.getEntryValue())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
          castor::vdqm::VDQM_INVALID_PORT_ENTRY, 1, params);

        std::cerr << std::endl << "Error: " << ex.getMessage().str()
          << std::endl;

        return 1;
      }

      server.addThreadPool(
        new castor::server::TCPListenerThreadPool("RequestHandlerThreadPool",
          new castor::vdqm::RequestHandlerThread(), vdqmPort));

      castor::dlf::Param params[] = {
        castor::dlf::Param("vdqmPort", vdqmPort)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        castor::vdqm::VDQM_SET_VDQMPORT, 1, params);
    }

    {
      int timeout = 0;

      try {
        timeout = server.getSchedulerTimeout();
      } catch(castor::exception::InvalidConfigEntry &ex) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("invalidValue", ex.getEntryValue())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
          castor::vdqm::VDQM_INVALID_SCHEDULERTIMEOUT_ENTRY, 1, params);

        std::cerr << std::endl << "Error: " << ex.getMessage().str()
          << std::endl;

        return 1;
      }

      server.addThreadPool(
        new castor::server::SignalThreadPool("DriveSchedulerThreadPool",
          new castor::vdqm::DriveSchedulerThread(), timeout));

      castor::dlf::Param params[] = {
        castor::dlf::Param("timeout", timeout)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        castor::vdqm::VDQM_SET_SCHEDULERTIMEOUT, 1, params);
    }

    {
      int timeout = 0;

      try {
        timeout = server.getRTCPJobSubmitterTimeout();
      } catch(castor::exception::InvalidConfigEntry &ex) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("invalidValue", ex.getEntryValue())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
          castor::vdqm::VDQM_INVALID_RTCPJOBSUBMITTERTIMEOUT_ENTRY, 1, params);

        std::cerr << std::endl << "Error: " << ex.getMessage().str()
          << std::endl;

        return 1;
      }

      server.addThreadPool(
        new castor::server::SignalThreadPool("JobSubmitterThreadPool",
          new castor::vdqm::RTCPJobSubmitterThread(), timeout));

      castor::dlf::Param params[] = {
        castor::dlf::Param("timeout", timeout)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        castor::vdqm::VDQM_SET_RTCPJOBSUBMITTERTIMEOUT, 1, params);
    }

    // Add a dedicated UDP thread pool for getting wakeup notifications
    {
      int notifyPort = 0;

      try {
        notifyPort = server.getNotifyPort();
      } catch(castor::exception::InvalidConfigEntry &ex) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("invalidValue", ex.getEntryValue())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
          castor::vdqm::VDQM_INVALID_NOTIFYPORT_ENTRY, 1, params);

        std::cerr << std::endl << "Error: " << ex.getMessage().str()
          << std::endl;

        return 1;
      }

      server.addNotifierThreadPool(notifyPort);

      castor::dlf::Param params[] = {
        castor::dlf::Param("notifyPort", notifyPort)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        castor::vdqm::VDQM_SET_NOTIFYPORT, 1, params);
    }

    //----------------------------------------------
    // Set the number of threads in each thread pool
    //----------------------------------------------

    requestHandlerThreadPool = server.getThreadPool('R');
    if(requestHandlerThreadPool == NULL) {
      std::cerr << "Failed to get RequestHandlerThreadPool" << std::endl;
      return 1;
    }

    {
      int nbThreads = server.getRequestHandlerThreadNumber();

      requestHandlerThreadPool->setNbThreads(nbThreads);

      castor::dlf::Param params[] = {
        castor::dlf::Param("nbThreads", nbThreads)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        castor::vdqm::VDQM_SET_REQUEST_HANDLER_THREAD_NB, 1, params);
    }

    driveSchedulerThreadPool = server.getThreadPool('D');
    if(driveSchedulerThreadPool == NULL) {
      std::cerr << "Failed to get DriveSchedulerThreadPool" << std::endl;
      return 1;
    }

    {
      int nbThreads = server.getSchedulerThreadNumber();

      driveSchedulerThreadPool->setNbThreads(nbThreads);

      castor::dlf::Param params[] = {
        castor::dlf::Param("nbThreads", nbThreads)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        castor::vdqm::VDQM_SET_SCHEDULER_THREAD_NB, 1, params);
    }

    rtcpJobSubmitterThreadPool = server.getThreadPool('J');
    if(rtcpJobSubmitterThreadPool == NULL) {
      std::cerr << "Failed to get JobSubmitterThreadPool" << std::endl;
      return 1;
    }

    {
      int nbThreads = server.getRTCPJobSubmitterThreadNumber();

      rtcpJobSubmitterThreadPool->setNbThreads(nbThreads);

      castor::dlf::Param params[] = {
        castor::dlf::Param("nbThreads", nbThreads)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        castor::vdqm::VDQM_SET_RTCP_JOB_SUBMITTER_THREAD_NB, 1, params);
    }


    try {

      //-----------------
      // Start the server
      //-----------------

      server.start();
      
    } catch (castor::exception::Exception &e) {
      std::cerr << "Failed to start VDQM server : "
                << sstrerror(e.code()) << std::endl
                << e.getMessage().str() << std::endl;

      return 1;
    } catch (...) {
      std::cerr << "Failed to start VDQM server : Caught general exception!"
        << std::endl;
    
      return 1;
    }

  } catch(castor::exception::Exception &e) {
    std::cerr << "Failed to start VDQM server : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;

    return 1;
  }
  
  return 0;
}
