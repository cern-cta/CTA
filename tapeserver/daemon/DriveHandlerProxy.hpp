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

#include "TapedProxy.hpp"
#include "common/threading/SocketPair.hpp"

namespace cta  { namespace tape { namespace daemon {

/**
 * A class sending the data transfer process reports to the daemon via a
 * socketPair. It implements the TapedProxy interface.
 */
class DriveHandlerProxy: public TapedProxy {
public:
  /**
   * Constructor. The constructor will close the server side of the
   * pair.
   * @param sopcketPair Reference to the socket pair.
   */
  DriveHandlerProxy(server::SocketPair & sopcketPair);
  virtual ~DriveHandlerProxy();
  void reportState(const cta::tape::session::SessionState state, const cta::tape::session::SessionType type, const std::string& vid) override;
  void reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) override;
  void addLogParams(const std::string& unitName, const std::list<cta::log::Param>&   params) override;
  void deleteLogParams(const std::string& unitName, const std::list<std::string>& paramNames) override;
  void labelError(const std::string& unitName, const std::string& message) override;
private:
  server::SocketPair & m_socketPair;
};

}}} // namespace cta::tape::daemon
