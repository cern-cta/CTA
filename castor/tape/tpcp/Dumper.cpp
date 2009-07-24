/******************************************************************************
 *                 castor/tape/tpcp/Dumper.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/Constants.hpp"
#include "castor/tape/tapegateway/DumpNotification.hpp"
#include "castor/tape/tpcp/Dumper.hpp"

#include <errno.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Dumper::Dumper(ParsedCommandLine &cmdLine,
  FilenameList &filenames, const vmgr_tape_info &vmgrTapeInfo,
  const char *const dgn, const int volReqId,
  castor::io::ServerSocket &callbackSock) throw() :
  ActionHandler(cmdLine, filenames, vmgrTapeInfo, dgn, volReqId,
    callbackSock) {

  // Register the Aggregator message handler member functions
  registerMsgHandler(OBJ_DumpNotification,
    &Dumper::handleDumpNotification, this);
  registerMsgHandler(OBJ_EndNotification,
    &Dumper::handleEndNotification, this);
  registerMsgHandler(OBJ_EndNotificationErrorReport,
    &Dumper::handleEndNotificationErrorReport, this);
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Dumper::~Dumper() {
  // Do nothing
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::tpcp::Dumper::run() throw(castor::exception::Exception) {

  // Spin in the dispatch message loop until there is no more work
  while(ActionHandler::dispatchMessage()) {
    // Do nothing
  }
}


//------------------------------------------------------------------------------
// handleDumpNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Dumper::handleDumpNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::DumpNotification *msg = NULL;

  castMessage(obj, msg, sock);

  std::ostream &os = std::cout;

  os << msg->message();

  return true;
}


//------------------------------------------------------------------------------
// handleEndNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Dumper::handleEndNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotification(obj, sock);
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Dumper::handleEndNotificationErrorReport(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotificationErrorReport(obj,sock);
}
