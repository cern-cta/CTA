/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "SessionType.hpp"
#include <sstream>

namespace cta { namespace tape { namespace session {

std::string toString(SessionType type) {
  switch(type) {
  case SessionType::Archive:
    return "Archive";
  case SessionType::Retrieve:
    return "Retrieve";
  case SessionType::Label:
    return "Label";
  case SessionType::Undetermined:
    return "Undetermined";
  default:
    {
      std::stringstream st;
      st << "UnknownType (" << ((uint32_t) type) << ")";
      return st.str();
    }
  }
}

}}} // namespace cta::tape::session

