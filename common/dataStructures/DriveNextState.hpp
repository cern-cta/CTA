/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>
#include "MountType.hpp"

namespace cta::common::dataStructures {

/**
 * This struct holds minimal drive info. It is used to (re-)create the
 * drive's entry in the register is needed.
 */
struct DriveNextState {
  std::string driveName;
  MountType mountType;
  std::string vid;
  std::string tapepool;
};

} // namespace cta::common::dataStructures
