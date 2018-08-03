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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "common/log/log.hpp"
#include "ZMQReactor.hpp"
#include "ZMQPollEventHandler.hpp"
#include "common/utils/utils.hpp"

#include "common/log/SyslogLogger.hpp"
#include <algorithm>

namespace{
  bool operator==(const zmq_pollitem_t& a,const zmq_pollitem_t& b){
       if( (a.fd==b.fd && a.fd!= -1 && b.fd != -1) || 
            (a.socket==b.socket && a.socket!=NULL && b.socket != NULL) ){
      return true;
    }
    return false;
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::reactor::ZMQReactor::ZMQReactor(cta::log::Logger &l):m_log(l) {
}
//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::reactor::ZMQReactor::~ZMQReactor() {
  clear();
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void cta::mediachanger::reactor::ZMQReactor::clear() {
  for(HandlerMap::iterator it=m_handlers.begin();it!=m_handlers.end();++it){
    delete it->second;
  }
  m_handlers.clear();
}


//------------------------------------------------------------------------------
// registerHandler
//------------------------------------------------------------------------------

  void cta::mediachanger::reactor::ZMQReactor::registerHandler(
  ZMQPollEventHandler *const handler) {
  zmq_pollitem_t item;
  handler->fillPollFd(item);

  std::ostringstream socketInHex;
  socketInHex << std::hex << item.socket;
  std::list<log::Param> params = {log::Param("fd", item.fd),
    log::Param("socket", socketInHex.str())};
  //log::write(LOG_DEBUG, "ZMQReactor registering a new handler", params);
  m_log(LOG_DEBUG, "ZMQReactor registering a new handler", params);

  checkDoubleRegistration(item);
  m_handlers.push_back(std::make_pair(item,handler));
}

//------------------------------------------------------------------------------
// checkDoubleRegistration
//------------------------------------------------------------------------------
void cta::mediachanger::reactor::ZMQReactor::checkDoubleRegistration(
  const zmq_pollitem_t &item) const {
  for(HandlerMap::const_iterator it=m_handlers.begin(); it!=m_handlers.end();
    ++it) {
    const std::pair<zmq_pollitem_t, ZMQPollEventHandler*> &maplet = *it;
    if(item == maplet.first) {
      cta::exception::Exception ex;
      ex.getMessage() << "ZMQReactor detected a double registration: fd=" <<
        item.fd << " socket=" << std::hex << item.socket;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------
void cta::mediachanger::reactor::ZMQReactor::handleEvents(const int timeout) {
  //it should not bring any copy, thanks to NRVO
  std::vector<zmq_pollitem_t> pollFds=buildPollFds();

  // Please note that we are relying on the fact that the file descriptors of
  // the vector are stored contiguously
  const int pollRc = zmq_poll(&pollFds[0], pollFds.size(), timeout);  
  if(0 <= pollRc){
    for(std::vector<zmq_pollitem_t>::const_iterator it=pollFds.begin();
      it!=pollFds.end(); ++it) {
      const zmq_pollitem_t &pollFd = *it;
      if(0 != pollFd.revents) {
        handleEvent(pollFd);
      }
    }
  } else if(0 > pollRc) {
    const std::string message = utils::errnoToString(errno);
    std::list<log::Param> params = {log::Param("message", message)};
    m_log(LOG_ERR, "Failed to handle I/O event: zmq_poll() failed", params);
  }
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
void cta::mediachanger::reactor::ZMQReactor::handleEvent(
  const zmq_pollitem_t &pollFd) {
  // Find and dispatch the appropriate handler if there is a pending event
  if(pollFd.revents & ZMQ_POLLIN) {
    HandlerMap::iterator handlerItor = findHandler(pollFd);
    if(handlerItor != m_handlers.end()) {
      ZMQPollEventHandler *const handler = handlerItor->second;
      const bool removeAndDeleteHandler = handler->handleEvent(pollFd);
      if(removeAndDeleteHandler) { 
        m_handlers.erase(handlerItor);
        delete(handler);
      }
    }else {
      std::list<log::Param> params;
      params.push_back(log::Param("fd",pollFd.fd));
      params.push_back(log::Param("socket",pollFd.socket));
      m_log(LOG_ERR, "Event on some poll, but no handler to match it", params);
    }
  }
}
  
//------------------------------------------------------------------------------
// findHandler
//------------------------------------------------------------------------------

cta::mediachanger::reactor::ZMQReactor::HandlerMap::iterator
  cta::mediachanger::reactor::ZMQReactor::findHandler(const zmq_pollitem_t& pollfd) {
  for(HandlerMap::iterator it=m_handlers.begin();it!=m_handlers.end();++it){
    // Use overloaded == to compare zmq_pollitem_t references
    if(pollfd==it->first){
      return it;
    } 
  }
  return m_handlers.end();
}

//------------------------------------------------------------------------------
// buildPollFds
//------------------------------------------------------------------------------
std::vector<zmq_pollitem_t> cta::mediachanger::reactor::ZMQReactor::buildPollFds()
  const {
  std::vector<zmq_pollitem_t> pollFds;
  pollFds.reserve(m_handlers.size());
  for(HandlerMap::const_iterator it=m_handlers.begin();it!=m_handlers.end();
    ++it) {
    ZMQPollEventHandler *const handler = it->second;
    zmq_pollitem_t pollFd;
    handler->fillPollFd(pollFd);
    pollFds.push_back(pollFd);
  }
  return pollFds;
}
