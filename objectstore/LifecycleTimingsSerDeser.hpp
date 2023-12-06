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
namespace cta::objectstore {

class LifecycleTimingsSerDeser: public common::dataStructures::LifecycleTimings {
public:
  LifecycleTimingsSerDeser() : common::dataStructures::LifecycleTimings() {}
  explicit LifecycleTimingsSerDeser(const common::dataStructures::LifecycleTimings& lifecycleTimings) : common::dataStructures::LifecycleTimings(lifecycleTimings) {}

  void deserialize(const objectstore::serializers::LifecycleTimings& ostoreLifecycleTimings) {
    completed_time = ostoreLifecycleTimings.completed_time();
    creation_time = ostoreLifecycleTimings.creation_time();
    first_selected_time = ostoreLifecycleTimings.first_selected_time();
  }
      
  void serialize(objectstore::serializers::LifecycleTimings& lifecycleTimings) {
    lifecycleTimings.set_completed_time(completed_time);
    lifecycleTimings.set_creation_time(creation_time);
    lifecycleTimings.set_first_selected_time(first_selected_time);
  }
};

} // namespace cta::objectstore
