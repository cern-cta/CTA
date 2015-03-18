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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/io/ServerSocket.hpp"
#include "castor/server/NotifierThread.hpp"
#include "castor/vdqm/ProtocolFacade.hpp"
#include "castor/vdqm/RequestHandlerThread.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "serrno.h"
#include "vdqm_constants.h"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::vdqm::RequestHandlerThread::RequestHandlerThread()
  throw() {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::vdqm::RequestHandlerThread::~RequestHandlerThread()
  throw() {
}

//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::vdqm::RequestHandlerThread::init()
  throw() {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::vdqm::RequestHandlerThread::run(void *param)
  throw() {

  Cuuid_t cuuid = nullCuuid;

  // Gives a Cuuid to the request
  Cuuid_create(&cuuid);

  if(param == NULL) {
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      VDQM_REQUEST_HANDLER_SOCKET_IS_NULL);
    return;
  }

  castor::io::ServerSocket *socket = (castor::io::ServerSocket*)param;

  try {

    handleRequest(cuuid, *socket);

    // Maybe the scheduler has some work to do
    castor::server::NotifierThread::getInstance()->doNotify('D');

  } catch(castor::exception::Exception &e) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("Standard Message", sstrerror(e.code())),
      castor::dlf::Param("Precise Message", e.getMessage().str())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, VDQM_HANDLE_REQUEST_EXCEPT, 2,
      params);
  }

  // De-allocate the socket
  delete socket;
} 


//-----------------------------------------------------------------------------
// handleRequest
//-----------------------------------------------------------------------------
void castor::vdqm::RequestHandlerThread::handleRequest(Cuuid_t &cuuid,
  castor::io::ServerSocket &sock)
   {

  unsigned short port; // Client port
  unsigned long  ip;   // Client IP

  try {

    // Get client IP info
    sock.getPeerIp(port, ip);

  } catch(castor::exception::Exception &e) {

    castor::exception::Exception ie;

    ie.getMessage() << "Failed to get peer port and IP: "
      << e.getMessage().str();

    throw ie;
  }

  // "New Request Arrival" message
  castor::dlf::Param params[] = {
    castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
    castor::dlf::Param("Port", port)
  };
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, VDQM_NEW_REQUEST_ARRIVAL,
    2, params);

  try {

    // The ProtocolFacade manages the analysis of the remaining socket message
    ProtocolFacade protocolFacade(sock, cuuid);

    protocolFacade.handleProtocolVersion();

  } catch(castor::exception::Exception &e) {
    castor::exception::Exception ie;

    ie.getMessage() << "Caught ProtocolFacade exception: "
      << e.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::vdqm::RequestHandlerThread::stop()
  throw() {
}
