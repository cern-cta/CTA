/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <string>

namespace cta::tape::session {

/// Progress of one TapeSession, independently of its outcome and drive status.
/// Callers must enter each phase at most once per session; phases may be skipped.
/// Repeating the current phase is harmless, but returning to a previous phase is invalid.
/// The tracker suppresses consecutive duplicates; it does not enforce transition order.
enum class TapeSessionState : uint32_t {
  Preparing,       ///< Initial preparation of jobs, workers, and tape access before mounting.
  Mounting,        ///< Asking the media changer to mount the tape.
  Loading,         ///< Waiting for media readiness and completing post-load setup before transfer.
  Transferring,    ///< Processing tape transfer tasks.
  Finalizing,      ///< Completing worker and reporting teardown after hardware work and disk delivery end.
  Unloading,       ///< Asking the drive to unload the tape, including rewind.
  Unmounting,      ///< Asking the media changer to remove the tape from the drive.
  DrainingToDisk,  ///< Retrieval disk delivery remains active; the drive is still unavailable.
  Finished         ///< The session owner has joined all workers and completed the final reporting attempt.
};
/// Return the display name of a tape-session phase.
std::string toString(TapeSessionState state);

}  // namespace cta::tape::session
