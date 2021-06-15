/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <string>

namespace cta { namespace tape { namespace session {

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

}}} // namespace cta::tape::s
