/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once
#include <string>

namespace cta::tape::session {

/** Possible states for the tape session. */
enum class SessionState : uint32_t {
  PendingFork,     ///< The subprocess is not started yet (internal state).
  StartingUp,      ///< The subprocess has just been started.
  Checking,        ///< The cleaner session checks if a tape is still present in the drive after failure
  Scheduling,      ///< The subprocess is determining what to do next.
  Mounting,        ///< The subprocess is mounting the tape.
  Running,         ///< The subprocess is running the data transfer.
  Unmounting,      ///< The subprocess is unmounting the tape.
  DrainingToDisk,  ///< The subprocess is flushing the memory to disk (retrieves only)
  ShuttingDown,    ///< The subprocess completed all tasks and will exit
  Shutdown,  ///< The subprocess is finished after a shutdown was requested and will not be retarted (internal state)
  Killed,    ///< The subprocess was killed and a restart is expected.
  Fatal      ///< The subprocess reported a fatal error (like scheduling inaccessible).
};
/** Session state to string */
std::string toString(SessionState state);

}  // namespace cta::tape::session
