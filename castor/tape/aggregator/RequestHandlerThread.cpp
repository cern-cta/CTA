/******************************************************************************
 *                castor/tape/aggregator/RequestHandlerThread.cpp
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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/RequestHandlerThread.hpp"
#include "castor/tape/aggregator/RCPJobSubmitter.hpp"
#include "castor/tape/aggregator/SocketHelper.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::RequestHandlerThread::RequestHandlerThread()
  throw () : m_jobQueue(1) {

  m_rtcopyMagicOld0Handlers[VDQM_CLIENTINFO] =
    &RequestHandlerThread::handleJobSubmission;

  m_magicToHandlers[RTCOPY_MAGIC_OLD0] = &m_rtcopyMagicOld0Handlers;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::RequestHandlerThread::~RequestHandlerThread()
  throw () {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::init()
  throw() {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::run(void *param)
  throw() {
  Cuuid_t cuuid = nullCuuid;

  // Gives a Cuuid to the request
  Cuuid_create(&cuuid);

  if(param == NULL) {
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_REQUEST_HANDLER_SOCKET_IS_NULL);
    return;
  }

  castor::io::ServerSocket *socket = (castor::io::ServerSocket*)param;

  try {

    dispatchRequest(cuuid, *socket);

  } catch(castor::exception::Exception &e) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("Standard Message", sstrerror(e.code())),
      castor::dlf::Param("Precise Message", e.getMessage().str())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_HANDLE_REQUEST_EXCEPT, 2, params);
  }

  // Close and de-allocate the socket
  socket->close();
  delete socket;
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::stop()
  throw() {
}


//-----------------------------------------------------------------------------
// dispatchRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::dispatchRequest(
  Cuuid_t &cuuid, castor::io::ServerSocket &socket)
  throw(castor::exception::Exception) {

  uint32_t magic   = 0;
  uint32_t reqtype = 0;


  try {
    magic = SocketHelper::readUint32(socket);
  } catch (castor::exception::Exception &e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_READ_MAGIC, 1, params);

    return;
  }

  MagicToHandlersMap::iterator handlerMapItor = m_magicToHandlers.find(magic);

  if(handlerMapItor == m_magicToHandlers.end()) {
    // Unknown magic number
    castor::dlf::Param params[] = {
      castor::dlf::Param("Magic Number", magic)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, AGGREGATOR_UNKNOWN_MAGIC, 1,
      params);

    return;
  }

  try {
    reqtype = SocketHelper::readUint32(socket);
  } catch (castor::exception::Exception &e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_READ_REQUEST_TYPE, 1, params);

    return;
  }

  HandlerMap *handlers = handlerMapItor->second;

  HandlerMap::iterator handlerItor = handlers->find(reqtype);

  if(handlerItor == handlers->end()) {
    // Unknown request type
    castor::dlf::Param params[] = {
      castor::dlf::Param("Magic Number", magic),
      castor::dlf::Param("Request Type", reqtype)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_UNKNOWN_REQUEST_TYPE, 2, params);

    return;
  }

  Handler handler = handlerItor->second;

  // Invoke the request's associated handler
  (this->*handler)(cuuid, socket);
}


//-----------------------------------------------------------------------------
// handleJobSubmission
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::handleJobSubmission(
  Cuuid_t &cuuid, castor::io::ServerSocket &socket) throw() {
}
