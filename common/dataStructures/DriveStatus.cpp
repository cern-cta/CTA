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

#include "common/dataStructures/DriveStatus.hpp"

#include <sstream>

std::string cta::common::dataStructures::toString(cta::common::dataStructures::DriveStatus type) {
  switch(type) {
    case cta::common::dataStructures::DriveStatus::Down:
      return "Down";
    case cta::common::dataStructures::DriveStatus::Up:
      return "Free";
    case cta::common::dataStructures::DriveStatus::Probing:
      return "Probe";
    case cta::common::dataStructures::DriveStatus::Starting:
      return "Start";
    case cta::common::dataStructures::DriveStatus::Mounting:
      return "Mount";
    case cta::common::dataStructures::DriveStatus::Transferring:
      return "Transfer";
    case cta::common::dataStructures::DriveStatus::Unloading:
      return "Unload";
    case cta::common::dataStructures::DriveStatus::Unmounting:
      return "Unmount";
    case cta::common::dataStructures::DriveStatus::DrainingToDisk:
      return "DrainToDisk";
    case cta::common::dataStructures::DriveStatus::CleaningUp:
      return "CleanUp";
    case cta::common::dataStructures::DriveStatus::Shutdown:
      return "Shutdown";
    case cta::common::dataStructures::DriveStatus::Unknown:
      return "Unknown";
    default:
    {
      std::stringstream ret;
      ret << "WRONG STATE CODE (" << (uint32_t) type << ")";
      return ret.str();
    }
  }
}


