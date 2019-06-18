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

#pragma once

#include <ostream>

namespace cta {
namespace common {
namespace dataStructures {

/**
 * Structure describing the instructions to the drive from operators.
 * The values are reset to all false when the drive goes down (including
 * at startup).
 */
struct DesiredDriveState {
  bool up;        ///< Should the drive be up?
  bool forceDown; ///< Should going down preempt an existig mount?
  bool operator==(const DesiredDriveState &rhs) const {
    return up == rhs.up && forceDown == rhs.forceDown;
  }
  DesiredDriveState(): up(false), forceDown(false) {}
};

std::ostream &operator<<(std::ostream& os, const DesiredDriveState& obj);

}}} // namespace cta::common::dataStructures