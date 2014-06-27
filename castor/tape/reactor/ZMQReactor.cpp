/******************************************************************************
 *         castor/tape/reactor/ZMQReactor.hpp
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
 * @author castor-dev
 *****************************************************************************/

#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/reactor/ZMQPollEventHandler.hpp"
#include "zmq/ZmqWrapper.hpp"
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

namespace castor {
namespace tape {
namespace reactor {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------    
  ZMQReactor::ZMQReactor(log::Logger& log,zmq::Context& ctx):
  m_context(ctx),m_log(log){
    
  }

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
  ZMQReactor::~ZMQReactor() throw() {
    clear();
  }

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------  
  void ZMQReactor::clear(){
    for(HandlerMap::iterator it=m_handlers.begin();it!=m_handlers.end();++it){
      delete it->second;
    }
    m_handlers.clear();
  }

//------------------------------------------------------------------------------
// registerHandler
//------------------------------------------------------------------------------  
  void ZMQReactor::registerHandler(ZMQPollEventHandler *const handler){
    zmq_pollitem_t item;
    handler->fillPollFd(item);
    item.events = ZMQ_POLLIN;
    //TODO, handle double registration 
    m_handlers.push_back(std::make_pair(item,handler));
  }

//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------  
  void ZMQReactor::handleEvents(const int timeout) {
    //it should not bring any copy, thanks to NRVO
    std::vector<zmq_pollitem_t> pollFds=buildPollFds();
    
    m_log(LOG_DEBUG, "Before zmq_poll");
    const int pollRc = zmq_poll(&pollFds[0], pollFds.size(), timeout);  
    m_log(LOG_DEBUG, "After zmq_poll");
    if(0 <= pollRc){
      for(std::vector<zmq_pollitem_t>::const_iterator it=pollFds.begin();
        it!=pollFds.end(); ++it) {
        const zmq_pollitem_t &pollFd = *it;
        if(0 != pollFd.revents) {
          handleEvent(pollFd);
        }
      }
    } else if(0 > pollRc) {
      char message[100];
      sstrerror_r(errno, message, sizeof(message));
      log::Param params[] = {log::Param("message", message)};
      m_log(LOG_ERR, "Failed to handle I/O event: zmq_poll() failed", params);
    }
  }

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
  void ZMQReactor::handleEvent(const zmq_pollitem_t &pollFd) {
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
  ZMQReactor::HandlerMap::iterator
  ZMQReactor::findHandler(const zmq_pollitem_t& pollfd) {
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
  std::vector<zmq_pollitem_t>  ZMQReactor::buildPollFds() const{
    std::vector<zmq_pollitem_t> vec;
    vec.reserve(m_handlers.size());
    for(HandlerMap::const_iterator it=m_handlers.begin();it!=m_handlers.end();++it){
      //ZMQPollEventHandler *const handler = it->second;
      //zmq_pollitem_t pollFd;
      //handler->fillPollFd(pollFd);
      //pollFd.events = ZMQ_POLLIN;
      vec.push_back(it->first);
    }
            std::list<log::Param> params;
        params.push_back(log::Param("nbFds",vec.size()));
        m_log(LOG_INFO, "STEVE buildPollFds()", params);
    return vec;
  }

} // namespace reactor
} // namespace tape
} // namespace castor
