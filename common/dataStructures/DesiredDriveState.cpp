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

#include "DesiredDriveState.hpp"

namespace cta {
namespace common {
namespace dataStructures {

DesiredDriveState::DesiredDriveState(const DesiredDriveState& ds) {
  if(this != &ds){
    up = ds.up;
    forceDown = ds.forceDown;
    reason = ds.reason;
    comment = ds.comment;
  }
}

DesiredDriveState& DesiredDriveState::operator=(const DesiredDriveState& ds) {
  if(this != &ds){
    up = ds.up;
    forceDown = ds.forceDown;
    reason = ds.reason;
    comment = ds.comment;
  }
  return *this;
}

}}}


std::ostream &cta::common::dataStructures::operator<<(std::ostream& os, const DesiredDriveState& obj) {
  std::string upStr(obj.up?"true":"false"),
          forceStr(obj.forceDown?"true":"false");
  return os << "(up="  << upStr  << " forceDown="  << forceStr << ")";
}
