/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/PhysicalLibrary.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PhysicalLibrary::PhysicalLibrary() = default;

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool PhysicalLibrary::operator==(const PhysicalLibrary& rhs) const {
  return name == rhs.name && manufacturer == rhs.manufacturer && model == rhs.model && type == rhs.type
         && guiUrl == rhs.guiUrl && webcamUrl == rhs.webcamUrl && location == rhs.location
         && nbPhysicalCartridgeSlots == rhs.nbPhysicalCartridgeSlots
         && nbAvailableCartridgeSlots == rhs.nbAvailableCartridgeSlots
         && nbPhysicalDriveSlots == rhs.nbPhysicalDriveSlots;
}

}  // namespace cta::common::dataStructures
