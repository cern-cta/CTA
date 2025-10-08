/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace cta::common::dataStructures {

enum DriveStatus {
  Down = 1,
  Up = 2,
  Probing = 3,
  Starting = 4,
  Mounting = 5,
  Transferring = 6,
  Unloading = 7,
  Unmounting = 8,
  DrainingToDisk = 9,
  CleaningUp = 10,
  Shutdown = 11,
  Unknown = 0
};

std::string toString(DriveStatus type);
} // namespace cta::common::dataStructures
