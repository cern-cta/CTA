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

#include "LifecycleTimings.hpp"

namespace cta {
namespace common {
namespace dataStructures {

LifecycleTimings::LifecycleTimings() : creation_time(0), first_selected_time(0), completed_time(0) {}

LifecycleTimings::LifecycleTimings(const LifecycleTimings& orig) :
creation_time(orig.creation_time),
first_selected_time(orig.first_selected_time),
completed_time(orig.completed_time) {}

// Assignment operator
LifecycleTimings LifecycleTimings::operator=(const LifecycleTimings& orig) {
  creation_time = orig.creation_time;
  first_selected_time = orig.first_selected_time;
  completed_time = orig.completed_time;
  return *this;
}

time_t LifecycleTimings::getTimeForSelection() {
  if (first_selected_time != 0 && creation_time != 0) {
    return first_selected_time - creation_time;
  }
  return 0;
}

time_t LifecycleTimings::getTimeForCompletion() {
  if (completed_time != 0 && creation_time != 0) {
    return completed_time - creation_time;
  }
  return 0;
}

}  // namespace dataStructures
}  // namespace common
}  // namespace cta