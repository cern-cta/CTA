/******************************************************************************
 *                      StateThread.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * The StateThread of the RmNode daemon collects and send to
 * the rmmaster the state of the node on which it runs.
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include files
#include "castor/monitoring/rmnode/StateThread.hpp"
#include "castor/monitoring/rmnode/RmNodeConfig.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"
#include "castor/monitoring/DiskServerStateReport.hpp"
#include "castor/monitoring/FileSystemStateReport.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/MessageAck.hpp"
#include "castor/IObject.hpp"
#include "castor/System.hpp"
#include "getconfent.h"
#include <unistd.h>
#include <sys/sysinfo.h>
#include <sys/vfs.h>
#include "errno.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::monitoring::rmnode::StateThread::StateThread
(std::string rmMasterHost, int rmMasterPort) :
  m_rmMasterHost(rmMasterHost), m_rmMasterPort(rmMasterPort) {
  // "State thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 6, 0, 0);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::monitoring::rmnode::StateThread::~StateThread() throw() {
  // "State thread destructed"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 12, 0, 0);
}

//------------------------------------------------------------------------------
// runs the thread starts by threadassign()
//------------------------------------------------------------------------------
void castor::monitoring::rmnode::StateThread::run(void* par)
  throw(castor::exception::Exception) {
  // "State thread started"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 8, 0, 0);
  castor::monitoring::DiskServerStateReport* state = 0;
  try {
    // collect
    state = collectDiskServerState();
    // send state to rmMaster
    castor::io::ClientSocket s(m_rmMasterPort, m_rmMasterHost);
    s.connect();
    s.sendObject(*state);
    // "State sent to rmMaster"
    castor::dlf::Param params[] =
      {castor::dlf::Param("content", state)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 16, 1, params);
    // cleanup memory
    delete state;
    state = 0;
    // check the acknowledgment
    castor::IObject* obj = s.readObject();
    castor::MessageAck* ack =
      dynamic_cast<castor::MessageAck*>(obj);
    if (0 == ack) {
      castor::exception::InvalidArgument e; // XXX To be changed
      e.getMessage() << "No Acknowledgement from the Server";
      delete ack;
      throw e;
    }
  }
  catch(castor::exception::Exception e) {
    // cleanup
    if (0 != state) {
      delete state;
      state = 0;
    }
    // "Error caught in StateThread::run"
    castor::dlf::Param params[] =
      {castor::dlf::Param("code", sstrerror(e.code())),
       castor::dlf::Param("message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 14, 2, params);
  }
  catch (...) {
    // "Error caught in StateThread::run"
    castor::dlf::Param params2[] =
      {castor::dlf::Param("message", "general exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 14, 1, params2);
  }
}

//------------------------------------------------------------------------------
// collectDiskServerState
//------------------------------------------------------------------------------
castor::monitoring::DiskServerStateReport*
castor::monitoring::rmnode::StateThread::collectDiskServerState()
  throw(castor::exception::Exception) {
  castor::monitoring::DiskServerStateReport* state =
    new castor::monitoring::DiskServerStateReport();
  // fill the machine name
  state->setName(castor::System::getHostName());
  // use sysinfo to get total RAM, Memory and Swap
  struct sysinfo si;
  if (0 == sysinfo(&si)) {
    // RAM
    u_signed64 ram = si.totalram;
    ram *= si.mem_unit;
    state->setRam(ram);
    // Memory
    u_signed64 memory = si.totalhigh;
    memory *= si.mem_unit;
    state->setMemory(memory);
    // Swap
    u_signed64 swap = si.totalswap;
    swap *= si.mem_unit;
    state->setSwap(swap);
  } else {
    delete state;
    castor::exception::Exception e(errno);
    e.getMessage()
      << "StateThread::collectDiskServerState : sysinfo call failed";
    throw e;
  }
  // fill the status, always the same
  state->setStatus(castor::stager::DISKSERVER_PRODUCTION);
  state->setAdminStatus(castor::monitoring::ADMIN_NONE);
  // get current list of filesystems
  char** fs;
  int nbFs;
  if (getconfent_multi_fromfile
      (RMNODECONFIGFILE, "RMNODE", "MOUNTPOINT", 0, &fs, &nbFs) < 0) {
    castor::exception::Exception e(errno);
    delete state;
    e.getMessage()
      << "StateThread::collectDiskServerState : getconfent_multi_fromfile failed";
    throw e;
  }
  // fill state for each FileSystems
  try {
    for (int i = 0; i < nbFs; i++) {
      state->addFileSystemStatesReports(collectFileSystemState(fs[i]));
    }
  } catch (castor::exception::Exception e) {
    delete state;
    throw e;
  }
  return state;
}

//------------------------------------------------------------------------------
// collectFileSystemState
//------------------------------------------------------------------------------
castor::monitoring::FileSystemStateReport*
castor::monitoring::rmnode::StateThread::collectFileSystemState
(std::string mountPoint)
  throw(castor::exception::Exception) {
  castor::monitoring::FileSystemStateReport* state =
    new castor::monitoring::FileSystemStateReport();
  // set mountpoint
  state->setMountPoint(mountPoint);
  // set space
  struct statfs sf;
  if (statfs(mountPoint.c_str(),&sf) == 0) {
    state->setSpace(((u_signed64) sf.f_blocks) * ((u_signed64) sf.f_bsize));
  } else {
    delete state;
    castor::exception::Exception e(errno);
    e.getMessage()
      << "StateThread::collectFileSystemState : statfs call failed";
    throw e;
  }
  // set status (always the same)
  state->setStatus(castor::stager::FILESYSTEM_PRODUCTION);
  state->setAdminStatus(castor::monitoring::ADMIN_NONE);
  return state;
}
