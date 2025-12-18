/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/session/SessionState.hpp"
#include "tapeserver/session/SessionType.hpp"

namespace cta::tape::daemon {

using SessionVid = std::string;
using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

class DriveHandlerStateReporter {
public:
  DriveHandlerStateReporter(const std::string& driveName,
                            SubprocessHandler::ProcessingStatus* processingStatus,
                            SessionVid* sessionVid,
                            TimePoint* lastDataMovementTime,
                            cta::log::LogContext* lc);

  ~DriveHandlerStateReporter() = default;

  using SessionState = session::SessionState;
  using SessionType = session::SessionType;

  TimePoint processState(const serializers::WatchdogMessage& message,
                         SessionState* sessionState,
                         SessionState* previousState,
                         SessionType* sessionType,
                         SessionType* previousType);

private:
  std::string m_driveName;
  SubprocessHandler::ProcessingStatus* m_processingStatus;
  SessionVid* m_sessionVid;
  TimePoint* m_lastDataMovementTime;
  cta::log::LogContext* m_lc;

  /** Helper function to handle Scheduling state report */
  SessionVid processScheduling(const serializers::WatchdogMessage& message,
                               const SessionState& sessionState,
                               const SessionType& sessionType);

  /** Helper function to handle Checking state report */
  SessionVid processChecking(const serializers::WatchdogMessage& message,
                             const SessionState& sessionState,
                             const SessionType& sessionType);

  /** Helper function to handle Mounting state report */
  SessionVid processMounting(const serializers::WatchdogMessage& message,
                             const SessionState& sessionState,
                             const SessionType& sessionType);

  /** Helper function to handle Running state report */
  SessionVid processRunning(const serializers::WatchdogMessage& message,
                            const SessionState& sessionState,
                            const SessionType& sessionType);

  /** Helper function to handle Unmounting state report */
  SessionVid processUnmounting(const serializers::WatchdogMessage& message,
                               const SessionState& sessionState,
                               const SessionType& sessionType);

  /** Helper function to handle DrainingToDisk state report */
  SessionVid processDrainingToDisk(const serializers::WatchdogMessage& message,
                                   const SessionState& sessionState,
                                   const SessionType& sessionType);

  /** Helper function to handle ShuttingDown state report */
  SessionVid processShuttingDown(const serializers::WatchdogMessage& message,
                                 const SessionState& sessionState,
                                 const SessionType& sessionType);

  /** Helper function to handle Fatal state report */
  void processFatal(const serializers::WatchdogMessage& message);
};

}  // namespace cta::tape::daemon
