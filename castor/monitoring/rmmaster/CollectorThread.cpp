/******************************************************************************
 *                      CollectorThread.cpp
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
 * @(#)$RCSfile$ $Author$
 *
 * The Collector thread of the RmMaster daemon.
 * It collects the data from the different nodes and updates a shared
 * memory representation of it
 *
 * @author castor-dev team
 *****************************************************************************/

#include "castor/monitoring/rmmaster/CollectorThread.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/monitoring/FileSystemStatus.hpp"
#include "castor/monitoring/DiskServerMetricsReport.hpp"
#include "castor/monitoring/FileSystemMetricsReport.hpp"
#include "castor/monitoring/DiskServerStateReport.hpp"
#include "castor/monitoring/FileSystemStateReport.hpp"
#include "castor/monitoring/admin/DiskServerAdminReport.hpp"
#include "castor/monitoring/admin/FileSystemAdminReport.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/MessageAck.hpp"
#include "castor/Constants.hpp"
#include <time.h>
#include <errno.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::monitoring::rmmaster::CollectorThread::CollectorThread
(castor::monitoring::ClusterStatus* clusterStatus) {
  m_updater = new castor::monitoring::rmmaster::ora::StatusUpdateHelper(clusterStatus); 
  // "Collector thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 10);
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::CollectorThread::run(void* par) throw() {
  try { // no exception should go out
    // "Thread Collector started"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9);
    // Get the server socket
    castor::io::ServerSocket* sock = (castor::io::ServerSocket*) par;
    // prepare the ackowledgement
    castor::MessageAck ack;
    ack.setStatus(true);
    // get the incoming object
    castor::IObject* obj = 0;
    try {
      obj = sock->readObject();
    } catch (castor::exception::Exception e) {
      // "Unable to read Object from socket" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 12, 1, params);
      ack.setStatus(false);
      ack.setErrorCode(EINVAL);
      std::ostringstream stst;
      stst << "Unable to read Request object from socket."
           << std::endl << e.getMessage().str();
      ack.setErrorMessage(stst.str());
    }
    // handle it
    if (ack.status()) {
      try {
        if (OBJ_DiskServerStateReport == obj->type()) {
          castor::monitoring::DiskServerStateReport* dss =
            dynamic_cast<castor::monitoring::DiskServerStateReport*>(obj);
          m_updater->handleStateUpdate(dss);
        } else if (OBJ_DiskServerMetricsReport == obj->type()) {
          castor::monitoring::DiskServerMetricsReport* dss =
            dynamic_cast<castor::monitoring::DiskServerMetricsReport*>(obj);
          m_updater->handleMetricsUpdate(dss);
        } else if (OBJ_DiskServerAdminReport == obj->type()) {
          castor::monitoring::admin::DiskServerAdminReport* dss =
            dynamic_cast<castor::monitoring::admin::DiskServerAdminReport*>(obj);
          m_updater->handleDiskServerAdminUpdate(dss);
        } else if (OBJ_FileSystemAdminReport == obj->type()) {
          castor::monitoring::admin::FileSystemAdminReport* dss =
            dynamic_cast<castor::monitoring::admin::FileSystemAdminReport*>(obj);
          m_updater->handleFileSystemAdminUpdate(dss);
        } else {
          // "Received unknown object from socket"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Type", obj->type())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 1, params);
          ack.setStatus(false);
          ack.setErrorCode(EINVAL);
          std::ostringstream stst;
          stst << "Received unknown object from socket."
               << std::endl << "Type was " << obj->type();
          ack.setErrorMessage(stst.str());
        }
      } catch (castor::exception::Exception e) {
        // "Caught exception in CollectorThread" message
        castor::dlf::Param params[] =
          {castor::dlf::Param("Message", e.getMessage().str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 12, 1, params);
        ack.setStatus(false);
        ack.setErrorCode(e.code());
        std::ostringstream stst;
        stst << e.getMessage().str();
        ack.setErrorMessage(stst.str());
      }
    }
    // free memory
    if (0 != obj) delete obj;
    // "Sending acknowledgement to client" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 15);
    try {
      sock->sendObject(ack);
    } catch (castor::exception::Exception e) {
      // "Unable to send Ack to client" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Standard Message", sstrerror(e.code())),
         castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 16, 2, params);
    }
    // closing and deleting socket
    delete sock;
  } catch (...) {
    // "General exception caught"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17);
  }
}
