/******************************************************************************
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
 * helper class for accessing VDQM from C++
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/Services.hpp"
#include "castor/tape/tapegateway/daemon/VdqmTapeGatewayHelper.hpp"
#include "h/serrno.h"
#include "h/vdqm_api.h"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <cstring>

//------------------------------------------------------------------------------
// connectToVdqm
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmTapeGatewayHelper::connectToVdqm() {
  serrno=0;
  m_connection=NULL;
  int rc = vdqm_Connect(&m_connection);
  if (rc<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::connectToVdqm"
      << " connect to vdqm failed";
    throw ex;
  }
  return;
}

//------------------------------------------------------------------------------
// createRequest
//------------------------------------------------------------------------------
int castor::tape::tapegateway::VdqmTapeGatewayHelper::createRequest(
  const std::string &vid, const char *dgn, const int mode, const int port,
  const int priority) {
  // Due to historical compoenents, we first send the priority of the future
  // request (set on the VID only) and only then we create the request
  serrno = 0;
  int ret = vdqm_SendVolPriority((char*)vid.c_str(),0,priority,0);
  if (ret < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::createRequest "
      << "vdqm_SendVolPriority failed";
    throw ex;
  }
  // send vol request
  int reqId = 0;
  serrno = 0;
  ret = vdqm_CreateRequest(m_connection, &reqId, (char*)vid.c_str(), (char*)dgn,
    NULL, NULL, mode, port);
  if (ret < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::createRequest "
      << "vdqm_CreateRequest failed";
    throw ex;
  }
  return reqId;
}

//------------------------------------------------------------------------------
// confirmRequestToVdqm
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmTapeGatewayHelper::confirmRequestToVdqm()
   {
  // after saving the transaction id in the db I send the confirmation to vdqm
  serrno=0;
  int ret = vdqm_QueueRequest(m_connection);

  if (ret<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::submitTapeToVdqm"
      <<" vdqm_QueueRequest failed";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkVdqmForRequest
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmTapeGatewayHelper::checkVdqmForRequest
(const int mountTransactionId)
   {
  serrno=0;
  int rc = vdqm_PingServer(NULL,NULL,mountTransactionId); // dgn is not given
  if (rc < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::checkVdqmForRequest"
      << " vdqm_PingServer failed";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// disconnectFromVdqm
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmTapeGatewayHelper::disconnectFromVdqm()
   {
  serrno=0;
  int rc = vdqm_Disconnect(&m_connection);
  if (rc<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::disconnectFromVdqm"
      << " connect to vdqm failed";
    throw ex;
  }
  return;
}
