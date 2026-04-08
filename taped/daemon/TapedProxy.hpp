/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/log/Param.hpp"
#include "tapeserver/session/SessionState.hpp"
#include "tapeserver/session/SessionType.hpp"

#include <functional>
#include <list>
#include <stdint.h>
#include <string>

namespace cta::tape::daemon {

/**
 * Abstract class defining the interface to a proxy object representing the
 * possible notifications sent back to main tape daemon (taped), as well as the notifications received from it
 */
class TapedProxy {
public:
  /**
   * Destructor.
   */
  virtual ~TapedProxy() = default;

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
                           const std::string& vid) = 0;

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
  virtual void addLogParams(const std::vector<cta::log::Param>& params) = 0;

  /**
   * Sends a list of parameters to remove from the end of session logging.
   */
  virtual void deleteLogParams(const std::vector<std::string>& paramNames) = 0;

  /**
   * Sends a list of parameters to remove from the end of session logging.
   */
  virtual void resetLogParams() = 0;

  /**
   * Notifies the tapeserverd daemon that a label session has encountered the
   * specified error.
   *
   * @param unitName The unit name of the tape drive.
   * @param message The error message.
   */
  virtual void labelError(const std::string& unitName, const std::string& message) = 0;

  /**
   * Add a callback function to handle the request to refresh the logger.
   *
   * @param handler to be run to refresh the logger
   */
  virtual void setRefreshLoggerHandler(std::function<void()> handler) = 0;

};  // class TapeserverProxy

}  // namespace cta::tape::daemon
