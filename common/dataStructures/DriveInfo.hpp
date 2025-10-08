/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace cta::common::dataStructures {

/**
 * This struct holds minimal drive info. It is used to (re-)create the
 * drive's entry in the register is needed.
 */
struct DriveInfo {
  std::string driveName;
  std::string host;
  std::string logicalLibrary;
};

} // namespace cta::common::dataStructures
