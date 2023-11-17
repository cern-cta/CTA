/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/threading/SocketPair.hpp"
#include "common/exception/Errnum.hpp"
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <poll.h>
#include <memory>
#include <list>
#include <algorithm>

namespace cta::server {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SocketPair::SocketPair() {
  int fd[2];
  cta::exception::Errnum::throwOnMinusOne(
    ::socketpair(AF_LOCAL, SOCK_SEQPACKET, 0, fd),
    "In SocketPair::SocketPair(): failed to socketpair(): ");
  m_parentFd = fd[0];
  m_childFd = fd[1];
  if (m_parentFd < 0 || m_childFd < 0) {
    std::stringstream err;
    err << "In SocketPair::SocketPair(): unexpected file descriptor: "
        << "fd[0]=" << fd[0] << " fd[1]=" << fd[1];
    throw cta::exception::Exception(err.str());
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SocketPair::~SocketPair() {
  if (m_parentFd != -1)
    ::close(m_parentFd);
  if (m_childFd != -1)
    ::close(m_childFd);
}

//------------------------------------------------------------------------------
// SocketPair::close
//------------------------------------------------------------------------------
void SocketPair::close(Side sideToClose) {
  if (m_currentSide != Side::both)
    throw CloseAlreadyCalled("In SocketPair::close(): one side was already closed");
  switch(sideToClose) {
  case Side::child:
    ::close(m_childFd);
    m_childFd = -1;
    m_currentSide = Side::parent;
    break;
  case Side::parent:
    ::close(m_parentFd);
    m_parentFd = -1;
    m_currentSide = Side::child;
    break;
  default:
    throw cta::exception::Exception("In SocketPair::close(): invalid side");
  }
}

//------------------------------------------------------------------------------
// SocketPair::close
//------------------------------------------------------------------------------
bool SocketPair::pollFlag() {
  return m_pollFlag;
}

//------------------------------------------------------------------------------
// SocketPair::poll
//------------------------------------------------------------------------------
void SocketPair::poll(pollMap& socketPairs, time_t timeout, Side sourceToPoll) {
  std::unique_ptr<struct ::pollfd[]> fds(new ::pollfd[socketPairs.size()]);
  struct ::pollfd *fdsp=fds.get();
  std::list<std::string> keys;
  for (const auto & sp: socketPairs) {
    keys.push_back(sp.first);
    fdsp->fd = sp.second->getFdForAccess(sourceToPoll);
    fdsp->revents = 0;
    fdsp->events = POLLIN;
    fdsp++;
  }
  timeout = std::max(timeout, (time_t)0);
  int rc=::poll(fds.get(), socketPairs.size(), timeout * 1000);
  if (rc > 0) {
    // We have readable fds, copy the results in the provided map
    fdsp=fds.get();
    for (const auto & key: keys) {
      socketPairs.at(key)->m_pollFlag = (fdsp++)->revents & POLLIN;
    }
  } else if (!rc) {
    throw Timeout("In SocketPair::poll(): timeout");
  } else {
    throw cta::exception::Errnum("In SocketPair::poll(): failed to poll(): ");
  }
}

//------------------------------------------------------------------------------
// SocketPair::getFdForAccess
//------------------------------------------------------------------------------
int SocketPair::getFdForAccess(Side sourceOrDestination) {
  // Validate state. sourceOrDestination must be Side::current, Side::parent, or Side::child.
  // If it is Side::Current, m_currentSide must be Side::parent or Side::child.
  Side sideToValidate = sourceOrDestination == Side::current ? m_currentSide : sourceOrDestination;
  ::set<Side> validSides = {Side::parent, Side::child};
  if(!validSides.count(sideToValidate)) {
    throw cta::exception::Exception("In SocketPair::getFdForAccess(): invalid state")
  }
  
  // Assign access FD based on sourceOrDestination + m_currentSide
  // If sourceOrDestination == Side::current, return the fd matching m_currentSide
  // (m_currentSide == Side::child returns child fd)
  // Otherwise, return the fd matching opposite of the side specified
  // (sourceOrDestination == Side::parent returns child fd)
  int accessFd = (
    (sourceOrDestination == Side::parent) ||
    (sourceOrDestination == Side::current && m_currentSide == Side::child)
  ) ? m_childFd : m_parentFd;
 
  if (accessFd == -1)
    throw cta::exception::Exception("In SocketPair::getFdForAccess(): file descriptor is closed");
  return accessFd;
}

//------------------------------------------------------------------------------
// SocketPair::receive
//------------------------------------------------------------------------------
std::string SocketPair::receive(Side source) {
  int fd=getFdForAccess(source);
  // First, get the message size (using peek option)
  ssize_t sizePeek = recv(fd, nullptr, 0, MSG_DONTWAIT | MSG_PEEK | MSG_TRUNC);
  if (!sizePeek) {
    throw PeerDisconnected("In SocketPair::receive(): connection reset by peer.");
  } else if (sizePeek < 0) {
    if (errno == EAGAIN) {
      throw NothingToReceive("In SocketPair::receive(): nothing to receive.");
    } else  {
      throw cta::exception::Errnum("In SocketPair::receive(): failed to recv(): ");
    }
  }
  std::unique_ptr<char[]> buff(new char[sizePeek]);
  struct ::msghdr hdr;
  struct ::iovec iov;
  hdr.msg_name = nullptr;
  hdr.msg_namelen = 0;
  hdr.msg_iov = &iov;
  hdr.msg_iovlen = 1;
  hdr.msg_iov->iov_base = (void*)buff.get();
  hdr.msg_iov->iov_len = sizePeek;
  hdr.msg_control = nullptr;
  hdr.msg_controllen = 0;
  hdr.msg_flags = 0;
  ssize_t size=recvmsg(fd, &hdr, MSG_DONTWAIT);
  if (size > 0) {
    if (hdr.msg_flags & MSG_TRUNC) {
      throw Overflow("In SocketPair::receive(): message was truncated.");
    }
    std::string ret;
    ret.append(buff.get(), size);
    return ret;
  } else if (!size) {
    throw PeerDisconnected("In SocketPair::receive(): connection reset by peer.");
  } else {
    if (errno == EAGAIN) {
      throw NothingToReceive("In SocketPair::receive(): nothing to receive.");
    } else  {
      throw cta::exception::Errnum("In SocketPair::receive(): failed to recvmsg(): ");
    }
  }
}

//------------------------------------------------------------------------------
// SocketPair::send
//------------------------------------------------------------------------------
void SocketPair::send(const std::string& msg, Side destination) {
  int fd=getFdForAccess(destination);
  cta::exception::Errnum::throwOnMinusOne(::send(fd, msg.data(), msg.size(), 0),
    "In SocketPair::send(): failed to send(): ");
}

} // namespace cta::server
