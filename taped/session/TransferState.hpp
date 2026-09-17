/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <string>

namespace cta::tape::session {

/** Progress of one DataTransferSession, independently of its outcome and drive status. */
enum class TransferState : uint32_t {
  Preparing,       ///< Preparing jobs, workers, and tape access for an acquired mount.
  Transferring,    ///< Processing tape transfer tasks.
  Finalizing,      ///< Cleaning up and completing outstanding reporting.
  DrainingToDisk,  ///< Tape work has ended while retrieve disk delivery remains active.
  Finished         ///< The session owner has joined all transfer and job-reporting workers.
};
/** Transfer state to string. */
std::string toString(TransferState state);

}  // namespace cta::tape::session
