/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once
#include <string>

namespace cta::tape::session {

/** Possible type for the tape session. */
enum class SessionType: uint32_t {
    Undetermined, ///< The initial direction for the session is undetermined.
    Archive,      ///< Direction is disk to tape.
    Retrieve,     ///< Direction is tape to disk.
    Label,        ///< (Re)label the tape.
    Cleanup       ///< Check the drive for the presence of a tape and eject it if necessary.
  };
/** Session state to string */
std::string toString(SessionType type);

} // namespace cta::tape::session
