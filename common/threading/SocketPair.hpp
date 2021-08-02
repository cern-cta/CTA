/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string> 
#include <stdint.h>
#include <map>
#include "common/exception/Exception.hpp"

namespace cta { namespace server {

/**
 * A class implementing a datagram communication between a parent process and
 * its child. The communication is achieved of a unix domain socket of type
 * datagram. It will hence allow transmission of bound binary packets.
 * Higher level class will have the duty to implement semantics on top of that.
 */
class SocketPair {
public:
  /// Constructor: opens the socket pair.
  SocketPair();
  /// Destructor: closes the remaining socketpairs 
  ~SocketPair();
  /// Enum allowing description of sides (parent, child)
  enum class Side: uint8_t {
    parent,
    child,
    current,
    both
  };
  CTA_GENERATE_EXCEPTION_CLASS(CloseAlreadyCalled);
  /// Close one side (after forking)
  void close(Side sideToClose);
  /// Send a buffer (optional side parameter allows use without closing,
  /// useful for testing).
  void send(const std::string& msg, Side destination = Side::current);
  CTA_GENERATE_EXCEPTION_CLASS(NothingToReceive);
  CTA_GENERATE_EXCEPTION_CLASS(PeerDisconnected);
  /// Receive a buffer immediately (optional side parameter allows use without
  /// closing, useful for testing).
  std::string receive(Side source = Side::current);
  /// A typedef used to store socketpairs to be passed to ppoll.
  typedef std::map<std::string, SocketPair *> pollMap;
  CTA_GENERATE_EXCEPTION_CLASS(Timeout);
  CTA_GENERATE_EXCEPTION_CLASS(Overflow);
  /// Poll the socketpairs listed in the map for reading (optional side 
  /// parameter allows use without closing, useful for testing).
  static void poll(pollMap & socketPairs, time_t timeout, 
    Side sourceToPoll = Side::current);
  /// Flag holding the result of a poll for a given socketpair.
  bool pollFlag();
  /// An helper function getting the right file descriptor for
  /// a given source or destination. With checks.
  int getFdForAccess(Side sourceOrDestination);
private:
  int m_parentFd = -1;               ///< The file descriptor for the 
  int m_childFd = -1;
  Side m_currentSide = Side::both;
  bool m_pollFlag = false;
};

}} // namespace cta::server
