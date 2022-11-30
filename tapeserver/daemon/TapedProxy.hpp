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

#pragma once

#include <stdint.h>

#include <string>
#include <memory>
#include <list>

#include "common/exception/Exception.hpp"
#include "common/log/Param.hpp"
#include "tapeserver/session/SessionState.hpp"
#include "tapeserver/session/SessionType.hpp"

namespace cta {

namespace server {
class SocketPair;
}

namespace tape {
namespace daemon {

/**
 * Abstract class defining the interface to a proxy object representing the
 * possible notifications sent back to main tape daemon (taped)
 */
class TapedProxy {
public:
  /**
   * Destructor.
   */
  virtual ~TapedProxy()  = 0;

  /**
   * Notifies taped of a state change. Taped will validate the transition and
   * kill the process if it is an unexpected transition.
   *
   * @param state the new state.
   * @param type the type of the session (archive, retrieve, verify,
   * @param vid the vid of the tape involved
   */
  virtual void reportState(const cta::tape::session::SessionState state,
    const cta::tape::session::SessionType type,
    const std::string & vid) = 0;

  /**
   * Report a heartbeat to taped. The data counters might or might not have changed
   * as the sending of the heartbeat itself is an information.
   *
   * @param totalTapeBytesMoved cumulated data transfered to/from tape during the session.
   * @param totalDiskBytesMoved cumulated data transfered to/from disk during the session.
   */
  virtual void reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) = 0;

  /**
   * Sends a new set of parameters, to be logged by the mother process when the
   * transfer session is over.
   * @param params: a vector of log parameters
   */
  virtual void addLogParams(const std::string &unitName,
    const std::list<cta::log::Param> & params) = 0;

  /**
   * Sends a list of parameters to remove from the end of session logging.
   */
  virtual void deleteLogParams(const std::string &unitName,
    const std::list<std::string> & paramNames) = 0;

  /**
   * Returns the socket pair used for communication between DriveHandler and DataTransferSession
   */
  virtual const std::unique_ptr<server::SocketPair>& socketPair() = 0;
};  // class TapeserverProxy

}}}  // namespace cta::tape::daemon

