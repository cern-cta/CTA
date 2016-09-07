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

#include "common/exception/BadAlloc.hpp"
#include "castor/tape/reactor/PollReactorImpl.hpp"
#include "common/SmartArrayPtr.hpp"
#include "castor/utils/utils.hpp"

#include <unistd.h>
#include <poll.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::reactor::PollReactorImpl::PollReactorImpl(log::Logger &log)
  throw(): m_log(log) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::reactor::PollReactorImpl::~PollReactorImpl() throw() {
  clear();
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void castor::tape::reactor::PollReactorImpl::clear() throw() {
  // Delete all event handlers
  for(HandlerMap::const_iterator itor = m_handlers.begin();
    itor !=  m_handlers.end(); itor++) {
    delete itor->second;
  }

  // Remove all event handlers
  m_handlers.clear();
}

//------------------------------------------------------------------------------
// registerHandler
//------------------------------------------------------------------------------
void castor::tape::reactor::PollReactorImpl::registerHandler(
  PollEventHandler *const handler)  {
  std::pair<HandlerMap::iterator, bool> insertResult =
    m_handlers.insert(HandlerMap::value_type(handler->getFd(), handler));
  if(!insertResult.second) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to register event handler for file descriptor "
      << handler->getFd() << " with reactor"
      ": File descriptor already has a registered event handler";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// removeHandler
//------------------------------------------------------------------------------
void castor::tape::reactor::PollReactorImpl::removeHandler(
  PollEventHandler *const handler)  {
  const HandlerMap::size_type nbElements = m_handlers.erase(handler->getFd());
  if(0 == nbElements) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to remove event handler for file descriptor " <<
      handler->getFd() << " from reactor: Handler not found";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------
void castor::tape::reactor::PollReactorImpl::handleEvents(const int timeout) {
  nfds_t nfds = 0;
  cta::SmartArrayPtr<struct pollfd> fds(buildPollFds(nfds));

  const int pollrc = poll(fds.get(), nfds, timeout);

  switch(pollrc) {
  case 0:
    // Timeout - do nothing
    break;
  case -1:
    {
      const int pollErrno = errno;

      std::list<log::Param> params = {
        log::Param("errno", pollErrno),
        log::Param("message", utils::errnoToString(pollErrno))};
      m_log(LOG_ERR, "Failed to handle a pending vdqm request: poll() failed",
        params);
    }
    break;
  default:
    dispatchEventHandlers(fds.get(), nfds);
  }
}

//------------------------------------------------------------------------------
// buildPollFdsArray
//------------------------------------------------------------------------------
struct pollfd *castor::tape::reactor::PollReactorImpl::buildPollFds(
  nfds_t &nfds) {
  nfds = m_handlers.size();

  cta::SmartArrayPtr<struct pollfd> fds;
  try {
    fds.reset(new struct pollfd[nfds]);
  } catch(std::bad_alloc &ba) {
    cta::exception::BadAlloc ex;
    ex.getMessage() <<
      "Failed to allocate memory for the file-descriptors of poll()"
      ": " << ba.what();
  }

  int i=0;
  for(HandlerMap::const_iterator itor = m_handlers.begin();
    itor != m_handlers.end(); itor++) {
    itor->second->fillPollFd(fds[i]);
    i++;
  }

  return fds.release();
}

//------------------------------------------------------------------------------
// dispatchEventHandlers
//------------------------------------------------------------------------------
void castor::tape::reactor::PollReactorImpl::dispatchEventHandlers(
  const struct pollfd *const fds, const nfds_t nfds) {
  // For each poll() file descriptor
  for(nfds_t i=0; i<nfds; i++) {
    // Find and dispatch the appropriate handler if there is a pending event
    if(0 != fds[i].revents) {
      PollEventHandler *handler = findHandler(fds[i].fd);
      const bool removeAndDeleteHandler = handler->handleEvent(fds[i]);
      if(removeAndDeleteHandler) {
        removeHandler(handler);
        delete(handler);
      }
    }
  }
}

//------------------------------------------------------------------------------
// findHandler
//------------------------------------------------------------------------------
castor::tape::reactor::PollEventHandler
  *castor::tape::reactor::PollReactorImpl::findHandler(const int fd)  {
  HandlerMap::iterator itor = m_handlers.find(fd);
  if(itor == m_handlers.end()) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to find event handler for file descriptor " << fd;
    throw ex;
  }

  return itor->second;
}
