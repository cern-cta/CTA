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

#include "tapeserver/daemon/DriveHandlerStateReporter.hpp"

namespace cta {
namespace tape {
namespace daemon {

DriveHandlerStateReporter::DriveHandlerStateReporter(const std::string& driveName,
  SubprocessHandler::ProcessingStatus *processingStatus, SessionVid *sessionVid, TimePoint * lastDataMovementTime,
  cta::log::LogContext* lc)
  : m_driveName(driveName),
    m_processingStatus(processingStatus),
    m_sessionVid(sessionVid),
    m_lastDataMovementTime(lastDataMovementTime),
    m_lc(lc) {}

TimePoint DriveHandlerStateReporter::processState(const serializers::WatchdogMessage& message,
  SessionState *sessionState, SessionState *previousState,
  SessionType *sessionType, SessionType *previousType) {
  // Log a session state change
  if (*sessionState != static_cast<SessionState>(message.sessionstate())) {
    *previousState = *sessionState;
    *previousType = *sessionType;
    log::ScopedParamContainer scoped(*m_lc);
    scoped.add("PreviousState", session::toString(*previousState))
          .add("PreviousType", session::toString(*previousType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_lc->log(log::INFO, "In processEvent(): changing session state");
  }

  switch (static_cast<SessionState>(message.sessionstate())) {
    case SessionState::Checking:
      *m_sessionVid = processChecking(message, *sessionState, *sessionType);
      break;
    case SessionState::Scheduling:
      *m_sessionVid = processScheduling(message, *sessionState, *sessionType);
      break;
    case SessionState::Mounting:
      *m_sessionVid = processMounting(message, *sessionState, *sessionType);
      break;
    case SessionState::Running:
      *m_sessionVid = processRunning(message, *sessionState, *sessionType);
      break;
    case SessionState::Unmounting:
      *m_sessionVid = processUnmounting(message, *sessionState, *sessionType);
      break;
    case SessionState::DrainingToDisk:
      *m_sessionVid = processDrainingToDisk(message, *sessionState, *sessionType);
      break;
    case SessionState::ShuttingDown:
      *m_sessionVid = processShuttingDown(message, *sessionState, *sessionType);
      break;
    case SessionState::Fatal:
      processFatal(message);
      break;
    default: {
      exception::Exception ex;
      ex.getMessage() << "In processEvent(): unexpected session state:"
                      << session::toString(static_cast<SessionState>(message.sessionstate()));
      throw ex;
    }
  }

  // Update state
  *sessionState = static_cast<SessionState>(message.sessionstate());
  *sessionType = static_cast<SessionType>(message.sessiontype());

  return std::chrono::steady_clock::now();
}

SessionVid DriveHandlerStateReporter::processScheduling(const serializers::WatchdogMessage& message,
  const SessionState& sessionState, const SessionType& sessionType) {
  // We are either going to schedule
  // Check the transition is expected. This is non-fatal as the drive session has the last word anyway.
  log::ScopedParamContainer params(*m_lc);
  params.add("tapeDrive", m_driveName);
  std::set<SessionState> expectedStates = {SessionState::StartingUp, SessionState::Scheduling};
  if (!expectedStates.count(sessionState) ||
      sessionType != SessionType::Undetermined ||
      static_cast<SessionType>(message.sessiontype()) != SessionType::Undetermined) {
    params.add("PreviousState", session::toString(sessionState))
          .add("PreviousType", session::toString(sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_lc->log(log::DEBUG, "WARNING: In processScheduling(): unexpected previous state/type.");
  }

  return std::string("");
}

SessionVid DriveHandlerStateReporter::processChecking(const serializers::WatchdogMessage& message,
  const SessionState& sessionState, const SessionType& sessionType) {
  // We expect to come from startup/undefined and to get into checking/cleanup
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(*m_lc);
  params.add("tapeDrive", m_driveName);
  if (sessionState != SessionState::StartingUp || sessionType != SessionType::Undetermined ||
      static_cast<SessionType>(message.sessiontype()) != SessionType::Cleanup) {
    params.add("PreviousState", session::toString(sessionState))
          .add("PreviousType", session::toString(sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_lc->log(log::DEBUG, "WARNING: In processChecking(): unexpected previous state/type.");
  }

  return std::string("");
}

SessionVid DriveHandlerStateReporter::processMounting(const serializers::WatchdogMessage& message,
  const SessionState& sessionState, const SessionType& sessionType) {
  // The only transition expected is from scheduling. Several sessions types are possible
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(*m_lc);
  params.add("tapeDrive", m_driveName);
  std::set<SessionType> expectedNewTypes = {SessionType::Archive, SessionType::Retrieve, SessionType::Label};
  if (sessionState != SessionState::Scheduling ||
      sessionType != SessionType::Undetermined ||
      !expectedNewTypes.count(static_cast<SessionType>(message.sessiontype()))) {
    params.add("PreviousState", session::toString(sessionState))
          .add("PreviousType", session::toString(sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_lc->log(log::DEBUG, "WARNING: In processMounting(): unexpected previous state/type.");
  }

  return message.vid();
}

SessionVid DriveHandlerStateReporter::processRunning(const serializers::WatchdogMessage& message,
  const SessionState& sessionState, const SessionType& sessionType) {
  // This status can be reported repeatedly (or we can transition from the previous one: Mounting).
  // We expect the type not to change (and to be in the right range)
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(*m_lc);
  params.add("tapeDrive", m_driveName);
  std::set<SessionState> expectedStates = {SessionState::Mounting, SessionState::Running};
  std::set<SessionType> expectedTypes = {SessionType::Archive, SessionType::Retrieve, SessionType::Label};
  if (!expectedStates.count(sessionState) ||
      !expectedTypes.count(sessionType) ||
      (sessionType != static_cast<SessionType>(message.sessiontype()))) {
    params.add("PreviousState", session::toString(sessionState))
          .add("PreviousType", session::toString(sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_lc->log(log::DEBUG, "WARNING: In processMounting(): unexpected previous state/type.");
  }

  // On state change reset the data movement counter
  if (sessionState != static_cast<SessionState>(message.sessionstate())) {
    *m_lastDataMovementTime = std::chrono::steady_clock::now();
  }

  return message.vid();
}

SessionVid DriveHandlerStateReporter::processUnmounting(const serializers::WatchdogMessage& message,
  const SessionState& sessionState, const SessionType& sessionType) {
  // This status can come from either running (any running compatible session type)
  // of checking in the case of the cleanup session.
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(*m_lc);
  params.add("tapeDrive", m_driveName);
  std::set<std::tuple<SessionState, SessionType>> expectedStateTypes =
    {
      std::make_tuple(SessionState::Running, SessionType::Archive),
      std::make_tuple(SessionState::Running, SessionType::Retrieve),
      std::make_tuple(SessionState::Running, SessionType::Label),
      std::make_tuple(SessionState::Checking, SessionType::Cleanup)
    };
  // (all types of sessions can unmount).
  if (!expectedStateTypes.count(std::make_tuple(sessionState, sessionType))) {
    params.add("PreviousState", session::toString(sessionState))
          .add("PreviousType", session::toString(sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_lc->log(log::DEBUG, "WARNING: In processUnmounting(): unexpected previous state/type.");
  }

  return message.vid();
}

SessionVid DriveHandlerStateReporter::processDrainingToDisk(const serializers::WatchdogMessage& message,
  const SessionState& sessionState, const SessionType& sessionType) {
  // This status transition is expected from unmounting, and only for retrieve sessions.
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(*m_lc);
  params.add("tapeDrive", m_driveName);
  if (SessionState::Unmounting != sessionState ||
      SessionType::Retrieve != sessionType) {
    params.add("PreviousState", session::toString(sessionState))
          .add("PreviousType", session::toString(sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_lc->log(log::DEBUG, "WARNING: In processDrainingToDisk(): unexpected previous state/type.");
  }

  return std::string("");
}

SessionVid DriveHandlerStateReporter::processShuttingDown(const serializers::WatchdogMessage& message,
  const SessionState& sessionState, const SessionType& sessionType) {
  // This status transition is expected from unmounting, and only for retrieve sessions.
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(*m_lc);
  params.add("tapeDrive", m_driveName);
  std::set<SessionState> expectedStates = {SessionState::Unmounting, SessionState::DrainingToDisk};
  if (!expectedStates.count(sessionState)) {
    params.add("PreviousState", session::toString(sessionState))
          .add("PreviousType", session::toString(sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_lc->log(log::DEBUG, "WARNING: In processShuttingDown(): unexpected previous state/type.");
  }

  return std::string("");
}

void DriveHandlerStateReporter::processFatal(const serializers::WatchdogMessage& message) {
  // This status indicates that the session cannot be run and the server should
  // shut down (central storage unavailable).
  log::ScopedParamContainer params(*m_lc);
  params.add("tapeDrive", m_driveName);
  m_processingStatus->shutdownRequested = true;
  m_lc->log(log::CRIT, "In processFatal(): shutting down after fatal failure.");
}

} // namespace daemon
} // namespace tape
} // namespace cta
