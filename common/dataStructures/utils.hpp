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

#pragma once

#include "common/dataStructures/TapeFile.hpp"

#include <iostream>
#include <map>

namespace cta::common::dataStructures {

std::ostream &operator<<(std::ostream &os, const std::list<TapeFile> &map);
std::ostream &operator<<(std::ostream &os, const std::map<uint64_t,std::string> &map);
std::ostream &operator<<(std::ostream &os, const std::pair<std::string,std::string> &pair);
std::ostream &operator<<(std::ostream &os, const std::map<uint64_t,std::pair<std::string,std::string>> &map);
std::ostream &operator<<(std::ostream &os, const std::map<std::string,std::pair<uint32_t,TapeFile>> &map);
std::ostream &operator<<(std::ostream &os, const std::map<uint64_t,std::pair<std::string,std::string>> &map);

} // namespace cta::common::dataStructures
