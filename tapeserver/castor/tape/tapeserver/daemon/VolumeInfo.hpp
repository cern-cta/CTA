/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
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

#include "common/dataStructures/MountType.hpp"

#include <string>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

  /**
   * Class holding the result of a Volume request
   */
    class VolumeInfo {
    public:
      VolumeInfo() {};
      /** The VID we will work on */
      std::string vid;
      /** The mount type: archive or retrieve */
      cta::common::dataStructures::MountType mountType;
      /** The number of files currently on tape*/
      uint32_t nbFiles;
      /** The mount Id */
      std::string mountId;
    };

} // namespace daemon
} // namespace tapesever
} // namespace tape
} // namespace castor
