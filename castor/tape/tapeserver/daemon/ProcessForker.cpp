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

#include "castor/exception/Exception.hpp"
#include "castor/messages/ForkCleaner.pb.h"
#include "castor/messages/ForkDataTransfer.pb.h"
#include "castor/messages/ForkLabel.pb.h"
#include "castor/messages/ForkSucceeded.pb.h"
#include "castor/messages/Status.pb.h"
#include "castor/messages/StopProcessForker.pb.h"
#include "castor/tape/tapeserver/daemon/ProcessForker.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerMsgType.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerUtils.hpp"
#include "castor/utils/SmartArrayPtr.hpp"
#include "h/serrno.h"

#include <errno.h>
#include <memory>
#include <poll.h>
#include <sstream>
#include <sys/types.h>
#include <sys/wait.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForker::ProcessForker(log::Logger &log,
  const int socketFd) throw(): m_log(log), m_socketFd(socketFd) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForker::~ProcessForker() throw() {
  if(-1 == close(m_socketFd)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    log::Param params[] = {log::Param("socketFd", m_socketFd),
      log::Param("message", message)};
    m_log(LOG_ERR, "Failed to close file-descriptor of ProcessForkerSocket",
      params);
  }
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForker::execute() {
  try {
    while(handleEvents()) {
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle events: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::handleEvents() {
  if(thereIsAPendingMsg()) {
    return handleMsg();
  } else {
    return true; // The main event loop should continue
  }
}

//------------------------------------------------------------------------------
// thereIsAPendingMsg
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::thereIsAPendingMsg() {

  // Call poll() in orer to see if there is any data to be read
  struct pollfd fds;
  fds.fd = m_socketFd;
  fds.events = POLLIN;
  fds.revents = 0;
  const int timeout = 100; // Timeout in milliseconds
  const int pollRc = poll(&fds, 1, timeout);

  // Return true of false depending on the result from poll()
  switch(pollRc) {
  case 0: // Timeout
    return false;
  case -1: // Error
    {
      char message[100];
      sstrerror_r(errno, message, sizeof(message));
      log::Param params[] = {log::Param("message", message)};
      m_log(LOG_ERR,
        "Error detected when checking for a pending ProcessForker message",
        params);
      return false;
    }
  case 1: // There is a possibility of a pending message
    return fds.revents & POLLIN ? true : false;
  default: // Unexpected return value
    {
      std::ostringstream message;
      message << "poll returned an unexpected value"
        ": expected=0 or 1 actual=" << pollRc;
      log::Param params[] = {log::Param("message", message.str())};
      m_log(LOG_ERR,
        "Error detected when checking for a pending ProcessForker message",
        params);
      return false;
    }
  }
}

//------------------------------------------------------------------------------
// handleMsg
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::handleMsg() {
  ProcessForkerFrame frame;
  try {
    frame = ProcessForkerUtils::readFrame(m_socketFd);
  } catch(castor::exception::Exception &ne) {
    log::Param params[] = {log::Param("message", ne.getMessage().str())};
    m_log(LOG_ERR, "Failed to handle message", params);
    sleep(1); // Sleep a moment to avoid going into a tight error loop
    return true; // The main event loop should continue
    /*
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle message: " << ne.getMessage().str();
    throw ex;
    */
  }

  log::Param params[] = {
    log::Param("type", ProcessForkerMsgType::toString(frame.type)),
    log::Param("len", frame.payload.length())};
  m_log(LOG_INFO, "ProcessForker handling a ProcessForker message", params);

  MsgHandlerResult result;
  try {
    result = dispatchMsgHandler(frame);
  } catch(castor::exception::Exception &ex) {
    log::Param("message", ex.getMessage().str());
    m_log(LOG_ERR, "ProcessForker::dispatchMsgHandler() threw an exception",
      params);
    messages::Status msg;
    msg.set_status(ex.code());
    msg.set_message(ex.getMessage().str());
    ProcessForkerUtils::writeFrame(m_socketFd, msg);
    return true; // The main event loop should continue
  } catch(std::exception &se) {
    log::Param("message", se.what());
    m_log(LOG_ERR, "ProcessForker::dispatchMsgHandler() threw an exception",
      params);
    messages::Status msg;
    msg.set_status(SEINTERNAL);
    msg.set_message(se.what());
    ProcessForkerUtils::writeFrame(m_socketFd, msg);
    return true; // The main event loop should continue
  } catch(...) {
    m_log(LOG_ERR,
      "ProcessForker::dispatchMsgHandler() threw an unknown exception");
    messages::Status msg;
    msg.set_status(SEINTERNAL);
    msg.set_message("Caught and unknown and unexpected exception");
    ProcessForkerUtils::writeFrame(m_socketFd, msg);
    return true; // The main event loop should continue
  }

  ProcessForkerUtils::writeFrame(m_socketFd, result.reply);
  {
    log::Param params[] = {
      log::Param("payloadType",
        ProcessForkerMsgType::toString(result.reply.type)),
      log::Param("payloadLen", result.reply.payload.length())};
    m_log(LOG_DEBUG, "ProcessForker wrote reply", params);
  }
  return result.continueMainEventLoop;
}

//------------------------------------------------------------------------------
// dispatchMsgHandler
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForker::MsgHandlerResult
  castor::tape::tapeserver::daemon::ProcessForker::dispatchMsgHandler(
  const ProcessForkerFrame &frame) {
  switch(frame.type) {
  case ProcessForkerMsgType::MSG_FORKCLEANER:
    return handleForkCleanerMsg(frame);
  case ProcessForkerMsgType::MSG_FORKDATATRANSFER:
    return handleForkDataTransferMsg(frame);
  case ProcessForkerMsgType::MSG_FORKLABEL:
    return handleForkLabelMsg(frame);
  case ProcessForkerMsgType::MSG_STOPPROCESSFORKER:
    return handleStopProcessForkerMsg(frame);
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to dispatch message handler"
        ": Unknown message type: type=" << frame.type;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleForkCleanerMsg
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForker::MsgHandlerResult 
  castor::tape::tapeserver::daemon::ProcessForker::handleForkCleanerMsg(
  const ProcessForkerFrame &frame) {

  // Parse the incoming request
  messages::ForkCleaner rqst;
  ProcessForkerUtils::parsePayload(frame, rqst);

  // Log the contents of the incomming request
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", rqst.unitname()));
  params.push_back(log::Param("vid", rqst.vid()));
  m_log(LOG_INFO, "ProcessForker handling ForkCleaner message", params);

  // Fork a label session
  const pid_t forkRc = fork();

  // If fork failed
  if(0 > forkRc) {
    // Log an error message and return
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    params.push_back(log::Param("message", message));
    m_log(LOG_ERR,
      "ProcessForker failed to fork cleaner session for tape drive",
      params);

    // Create and return the result of handling the incomming request
    messages::Status reply;
    reply.set_status(SEINTERNAL);
    reply.set_message("Failed to fork cleaner session for tape drive");
    MsgHandlerResult result;
    result.continueMainEventLoop = true;
    ProcessForkerUtils::serializePayload(result.reply, reply);
    return result;

  // Else if this is the parent process
  } else if(0 < forkRc) {
    log::Param params[] = {log::Param("pid", forkRc)};
    m_log(LOG_INFO, "ProcessForker forked cleaner session", params);

    // TO BE DONE
    waitpid(forkRc, NULL, 0);

    // Create and return the result of handling the incomming request
    messages::ForkSucceeded reply;
    reply.set_pid(forkRc);
    MsgHandlerResult result;
    result.continueMainEventLoop = true;
    ProcessForkerUtils::serializePayload(result.reply, reply);
    return result;

  // Else this is the child process
  } else {
    // TO BE DONE
    exit(0);
  }
}

//------------------------------------------------------------------------------
// handleDataTransferMsg
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForker::MsgHandlerResult 
  castor::tape::tapeserver::daemon::ProcessForker::handleForkDataTransferMsg(
  const ProcessForkerFrame &frame) {

  // Parse the incoming request
  messages::ForkDataTransfer rqst;
  ProcessForkerUtils::parsePayload(frame, rqst);

  // Log the contents of the incomming request
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", rqst.unitname()));
  m_log(LOG_INFO, "ProcessForker handling ForkDataTransfer message", params);

  // Fork a data-transfer session
  const pid_t forkRc = fork();

  // If fork failed
  if(0 > forkRc) {
    // Log an error message and return
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    params.push_back(log::Param("message", message));
    m_log(LOG_ERR,
      "ProcessForker failed to fork data-transfer session for tape drive",
      params);

    // Create and return the result of handling the incomming request
    messages::Status reply;
    reply.set_status(SEINTERNAL);
    reply.set_message("Failed to fork data-transfer session for tape drive");
    MsgHandlerResult result;
    result.continueMainEventLoop = true;
    ProcessForkerUtils::serializePayload(result.reply, reply);
    return result;

  // Else if this is the parent process
  } else if(0 < forkRc) {
    log::Param params[] = {log::Param("pid", forkRc)};
    m_log(LOG_INFO, "ProcessForker forked data-transfer session", params);

    // TO BE DONE
    waitpid(forkRc, NULL, 0);

    // Create and return the result of handling the incomming request
    messages::ForkSucceeded reply;
    reply.set_pid(forkRc);
    MsgHandlerResult result;
    result.continueMainEventLoop = true;
    ProcessForkerUtils::serializePayload(result.reply, reply);
    return result;

  // Else this is the child process
  } else {
    // TO BE DONE
    exit(0);
  }
}

//------------------------------------------------------------------------------
// handleForkLabelMsg
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForker::MsgHandlerResult 
  castor::tape::tapeserver::daemon::ProcessForker::handleForkLabelMsg(
  const ProcessForkerFrame &frame) {
  // Parse the incoming request
  messages::ForkLabel rqst;
  ProcessForkerUtils::parsePayload(frame, rqst);

  // Log the contents of the incomming request
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", rqst.unitname()));
  params.push_back(log::Param("vid", rqst.vid()));
  m_log(LOG_INFO, "ProcessForker handling ForkLabel message", params);

  // Fork a label session
  const pid_t forkRc = fork();

  // If fork failed
  if(0 > forkRc) {
    // Log an error message and return
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    params.push_back(log::Param("message", message));
    m_log(LOG_ERR, "ProcessForker failed to fork label session for tape drive",
      params);

    // Create and return the result of handling the incomming request
    messages::Status reply;
    reply.set_status(SEINTERNAL);
    reply.set_message("Failed to fork label session for tape drive");
    MsgHandlerResult result;
    result.continueMainEventLoop = true;
    ProcessForkerUtils::serializePayload(result.reply, reply);
    return result;

  // Else if this is the parent process
  } else if(0 < forkRc) {
    log::Param params[] = {log::Param("pid", forkRc)};
    m_log(LOG_INFO, "ProcessForker forked label session", params);

    // TO BE DONE
    waitpid(forkRc, NULL, 0);

    // Create and return the result of handling the incomming request
    messages::ForkSucceeded reply;
    reply.set_pid(forkRc);
    MsgHandlerResult result;
    result.continueMainEventLoop = true;
    ProcessForkerUtils::serializePayload(result.reply, reply);
    return result;

  // Else this is the child process
  } else {
    // TO BE DONE
    exit(0);
  }
}

//------------------------------------------------------------------------------
// handleStopProcessForkerMsg
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForker::MsgHandlerResult 
  castor::tape::tapeserver::daemon::ProcessForker::
  handleStopProcessForkerMsg(const ProcessForkerFrame &frame) {

  // Parse the incoming request
  messages::StopProcessForker rqst;
  ProcessForkerUtils::parsePayload(frame, rqst);

  // Log the fact that the ProcessForker will not gracefully stop
  log::Param params[] = {log::Param("reason", rqst.reason())};
  m_log(LOG_INFO, "Gracefully stopping ProcessForker", params);

  // Create and return the result of handling the incomming request
  messages::Status reply;
  reply.set_status(0);
  reply.set_message("");
  MsgHandlerResult result;
  result.continueMainEventLoop = false;
  ProcessForkerUtils::serializePayload(result.reply, reply);
  return result;
}
