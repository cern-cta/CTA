/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <string>

namespace cta::tape::session {

/** Progress of one TapeSession, independently of its outcome and drive status. */
enum class TapeSessionState : uint32_t {
  Preparing,       ///< Preparing jobs, workers, and tape access outside the explicit tape operations.
  Mounting,        ///< Asking the media changer to mount the tape.
  Loading,         ///< Waiting for the mounted tape to become ready in the drive.
  Transferring,    ///< Processing tape transfer tasks.
  Finalizing,      ///< Cleaning up and completing outstanding reporting outside the explicit tape operations.
  Unloading,       ///< Asking the drive to unload the tape, including rewind.
  Unmounting,      ///< Asking the media changer to remove the tape from the drive.
  DrainingToDisk,  ///< Retrieval disk delivery remains active; the drive is still unavailable.
  Finished         ///< The session owner has joined all transfer and job-reporting workers.
};
/** TapeSession state to string. */
std::string toString(TapeSessionState state);

}  // namespace cta::tape::session
