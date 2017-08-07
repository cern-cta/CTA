/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/dataStructures/DriveStatus.hpp"

#include <sstream>

std::string cta::common::dataStructures::toString(cta::common::dataStructures::DriveStatus type) {
  switch(type) {
    case cta::common::dataStructures::DriveStatus::Down:
      return "Down";
    case cta::common::dataStructures::DriveStatus::Up:
      return "Free";
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


