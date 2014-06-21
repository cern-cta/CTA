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
#include "zmq/zmqcastor.hpp"

#include <algorithm>

namespace{
  bool operator==(const zmq::pollitem_t& a,const zmq::pollitem_t& b){
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
  ZMQReactor::ZMQReactor(log::Logger& log,zmq::context_t& ctx):
  m_context(ctx),m_log(log){
    
  }
//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
  ZMQReactor::~ZMQReactor(){
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
    zmq::pollitem_t item;
    handler->fillPollFd(item);
    item.events = ZMQ_POLLIN;
    //TODO, handle double registration 
    m_handlers.push_back(std::make_pair(item,handler));
  }
//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------  
  void ZMQReactor::handleEvents(const int timeout){
    //it should not bring any copy, thanks to NRVO
    std::vector<zmq::pollitem_t> pollFD=buildPollFds();
    
    const int pollrc = zmq::poll(&pollFD[0], pollFD.size(), timeout);  
    if(pollrc !=0){
      dispatchEventHandlers(pollFD);
    }
  }
//------------------------------------------------------------------------------
// dispatchEventHandlers
//------------------------------------------------------------------------------  
  void ZMQReactor::dispatchEventHandlers(const std::vector<zmq::pollitem_t>& pollFD){  
    for(std::vector<zmq::pollitem_t>::const_iterator it=pollFD.begin();
            it!=pollFD.end();
            ++it) {
      
      // Find and dispatch the appropriate handler if there is a pending event
      if(it->revents & ZMQ_POLLIN) {
        ZMQPollEventHandler *handler = findHandler(*it);
        if(handler) {
          const bool removeAndDeleteHandler = handler->handleEvent(*it);
          if(removeAndDeleteHandler) { 
            removeHandler(handler);
            delete(handler);
          }
        }else {
          std::list<log::Param> params;
          params.push_back(log::Param("fd",it->fd));
          params.push_back(log::Param("socket",it->socket));
          m_log(LOG_ERR, "Event on some poll, but no handler to match it", params);
        }
      }
    }
  }
  
  ZMQPollEventHandler *  
  ZMQReactor::findHandler(const zmq::pollitem_t& pollfd) const{
    for(HandlerMap::const_iterator it=m_handlers.begin();it!=m_handlers.end();++it){
      if(pollfd==it->first){
        return it->second;
      } 
    }
    return NULL;
  }
//------------------------------------------------------------------------------
// removeHandler
//------------------------------------------------------------------------------
  void ZMQReactor::removeHandler(ZMQPollEventHandler *const handler){
    zmq::pollitem_t pollitem;
    for(HandlerMap::iterator it=m_handlers.begin();it!=m_handlers.end();++it){
      if(it->second==handler){
        pollitem=it->first;
        m_handlers.erase(it);
        break;
      }
    }
  }
//------------------------------------------------------------------------------
// buildPollFds
//------------------------------------------------------------------------------  
  std::vector<zmq::pollitem_t>  ZMQReactor::buildPollFds() const{
    std::vector<zmq::pollitem_t> vec;
    vec.reserve(m_handlers.size());
    for(HandlerMap::const_iterator it=m_handlers.begin();it!=m_handlers.end();++it){
      vec.push_back(it->first);
    }
    return vec;
  }

} // namespace reactor
} // namespace tape
} // namespace castor
