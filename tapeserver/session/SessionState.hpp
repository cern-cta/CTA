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
#include <string>

namespace cta::tape::session {

/** Possible states for the tape session. */
enum class SessionState: uint32_t  {
  PendingFork,///< The subprocess is not started yet (internal state).
  StartingUp, ///< The subprocess has just been started.
  Checking,   ///< The cleaner session checks if a tape is still present in the drive after failure
  Scheduling, ///< The subprocess is determining what to do next.
  Mounting,   ///< The subprocess is mounting the tape.
  Running,    ///< The subprocess is running the data transfer.
  Unmounting, ///< The subprocess is unmounting the tape.
  DrainingToDisk, ///< The subprocess is flushing the memory to disk (retrieves only)
  ShuttingDown, ///< The subprocess completed all tasks and will exit
  Shutdown,   ///< The subprocess is finished after a shutdown was requested and will not be retarted (internal state)
  Killed,     ///< The subprocess was killed and a restart is expected.
  Fatal       ///< The subprocess reported a fatal error (like scheduling inaccessible).
};
/** Session state to string */
std::string toString(SessionState state);

} // namespace cta::tape::session
