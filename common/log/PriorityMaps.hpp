/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <map>
#include <string>

namespace cta::log {

class PriorityMaps {
public:
  static const std::map<int, std::string> c_priorityToTextMap;
  static const std::map<std::string, int> c_configTextToPriorityMap;
  static std::string getPriorityText(const int priority);
};

}  // namespace cta::log
