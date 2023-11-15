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

#include <list>
#include <map>
#include <optional>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct contains information about which drive was responsible for a
 * specific tape operation (read/write/label) and when did it happen
 */
struct TapeLog {

  TapeLog();

  bool operator==(const TapeLog &rhs) const;

  bool operator!=(const TapeLog &rhs) const;

  std::string drive;
  time_t time;

}; // struct TapeLog

std::ostream &operator<<(std::ostream &os, const TapeLog &obj);

std::ostream &operator<<(std::ostream &os, const std::optional<TapeLog> &obj);

} // namespace cta::common::dataStructures
