/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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


