/******************************************************************************
 *                      BasePlugin.cpp
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
 * Base class for a stagerjob plugin
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "castor/IClient.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/job/stagerjob/BasePlugin.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"

#define SELECT_TIMEOUT 60

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::BasePlugin::BasePlugin
(std::string protocol) throw() :
  m_selectTimeOut(SELECT_TIMEOUT) {
  castor::job::stagerjob::registerPlugin(protocol, this);
}

//------------------------------------------------------------------------------
// getPortRange
//------------------------------------------------------------------------------
std::pair<int, int>
castor::job::stagerjob::BasePlugin::getPortRange
(castor::job::stagerjob::InputArguments&) throw() {
  return std::pair<int, int>(1024, 65536);
}

//------------------------------------------------------------------------------
// waitForChild
//------------------------------------------------------------------------------
bool castor::job::stagerjob::BasePlugin::waitForChild
(castor::job::stagerjob::InputArguments &args) throw() {
  bool childFailed = false;
  // Wait for all child to exit
  while (1) {
    int term_status = -1;
    pid_t childPid = waitpid(-1, &term_status, WNOHANG);
    if (childPid < 0) {
      break;
    }
    if (childPid > 0) {
      if (WIFEXITED(term_status)) {
        int status = WEXITSTATUS(term_status);
        castor::dlf::Param params[] =
          {castor::dlf::Param("PID", childPid),
           castor::dlf::Param("Status", status),
           castor::dlf::Param(args.subRequestUuid)};
        childFailed = (status != 0);
        if (childFailed) {
          castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                                  CHILDEXITED, 3, params, &args.fileId);
        } else {
          castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_DEBUG,
                                  CHILDEXITED, 3, params, &args.fileId);
        }
      } else if (WIFSIGNALED(term_status)) {
        castor::dlf::Param params[] =
          {castor::dlf::Param("PID", childPid),
           castor::dlf::Param("Signal", WTERMSIG(term_status)),
           castor::dlf::Param(args.subRequestUuid)};
        castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                                CHILDSIGNALED, 3, params, &args.fileId);
        childFailed = true;
      } else {
        castor::dlf::Param params[] =
          {castor::dlf::Param("PID", childPid),
           castor::dlf::Param(args.subRequestUuid)};
        castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                                CHILDSTOPPED, 2, params, &args.fileId);
        childFailed = true;
      }
    }
    sleep(1);
  }
  return childFailed;
}
