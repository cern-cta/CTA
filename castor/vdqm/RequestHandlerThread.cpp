/******************************************************************************
 *                castor/vdqm/RequestHandlerThread.cpp
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

#include "castor/io/ServerSocket.hpp"
#include "castor/vdqm/ProtocolFacade.hpp"
#include "castor/vdqm/RequestHandlerThread.hpp"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::vdqm::RequestHandlerThread::RequestHandlerThread()
throw () {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::vdqm::RequestHandlerThread::~RequestHandlerThread()
throw () {
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
  castor::io::ServerSocket *sock = (castor::io::ServerSocket*)param;

  // placeholder for the request uuid if any
  Cuuid_t cuuid = nullCuuid;

  // Retrieve info on the client
  unsigned short port;
  unsigned long ip;

  // gives a Cuuid to the request
  Cuuid_create(&cuuid);

  try {
    sock->getPeerIp(port, ip);

    // "New Request Arrival" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port", port)
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 1, 2, params);
  } catch(castor::exception::Exception e) {
    // "Exception caught : ignored" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("Standard Message", sstrerror(e.code())),
      castor::dlf::Param("Precise Message", e.getMessage().str())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 5, 2, params);
  }

  // The ProtocolFacade manages the analysis of the remaining socket message
  ProtocolFacade protocolFacade = ProtocolFacade(sock, &cuuid);
  protocolFacade.handleProtocolVersion();

  delete sock;
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::vdqm::RequestHandlerThread::stop()
throw() {
}
