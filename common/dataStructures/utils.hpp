/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/TapeFile.hpp"

#include <iostream>
#include <map>

namespace cta::common::dataStructures {

std::ostream& operator<<(std::ostream& os, const std::list<TapeFile>& map);
std::ostream& operator<<(std::ostream& os, const std::map<uint64_t, std::string>& map);
std::ostream& operator<<(std::ostream& os, const std::pair<std::string, std::string>& pair);
std::ostream& operator<<(std::ostream& os, const std::map<uint64_t, std::pair<std::string, std::string>>& map);
std::ostream& operator<<(std::ostream& os, const std::map<std::string, std::pair<uint32_t, TapeFile>>& map);
std::ostream& operator<<(std::ostream& os, const std::map<uint64_t, std::pair<std::string, std::string>>& map);

}  // namespace cta::common::dataStructures
