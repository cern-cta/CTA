/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
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

#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/session/SessionState.hpp"
#include "tapeserver/session/SessionType.hpp"

namespace cta {
namespace tape {
namespace daemon {

using SessionVid = std::string;
using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

class DriveHandlerStateReporter {
public:

  DriveHandlerStateReporter(const std::string& driveName, SubprocessHandler::ProcessingStatus *processingStatus,
    SessionVid *sessionVid, TimePoint * lastDataMovementTime, cta::log::LogContext* lc);

  ~DriveHandlerStateReporter() = default;

  using SessionState = session::SessionState;
  using SessionType = session::SessionType;

  TimePoint processState(const serializers::WatchdogMessage& message,
    SessionState *sessionState, SessionState *previousState,
    SessionType *sessionType, SessionType *previousType);

private:
  std::string m_driveName;
  SubprocessHandler::ProcessingStatus *m_processingStatus;
  SessionVid *m_sessionVid;
  TimePoint *m_lastDataMovementTime;
  cta::log::LogContext *m_lc;

  /** Helper function to handle Scheduling state report */
  SessionVid processScheduling(const serializers::WatchdogMessage& message, const SessionState& sessionState,
    const SessionType& sessionType);

  /** Helper function to handle Checking state report */
  SessionVid processChecking(const serializers::WatchdogMessage& message, const SessionState& sessionState,
    const SessionType& sessionType);

  /** Helper function to handle Mounting state report */
  SessionVid processMounting(const serializers::WatchdogMessage& message, const SessionState& sessionState,
    const SessionType& sessionType);

  /** Helper function to handle Running state report */
  SessionVid processRunning(const serializers::WatchdogMessage& message, const SessionState& sessionState,
    const SessionType& sessionType);

  /** Helper function to handle Unmounting state report */
  SessionVid processUnmounting(const serializers::WatchdogMessage& message, const SessionState& sessionState,
    const SessionType& sessionType);

  /** Helper function to handle DrainingToDisk state report */
  SessionVid processDrainingToDisk(const serializers::WatchdogMessage& message, const SessionState& sessionState,
    const SessionType& sessionType);

  /** Helper function to handle ShuttingDown state report */
  SessionVid processShuttingDown(const serializers::WatchdogMessage& message, const SessionState& sessionState,
    const SessionType& sessionType);

  /** Helper function to handle Fatal state report */
  void processFatal(const serializers::WatchdogMessage& message);
};

} // namespace daemon
} // namespace tape
} // namespace cta