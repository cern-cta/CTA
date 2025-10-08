/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

  void serialize(objectstore::serializers::LifecycleTimings& lifecycleTimings) const {
    lifecycleTimings.set_completed_time(completed_time);
    lifecycleTimings.set_creation_time(creation_time);
    lifecycleTimings.set_first_selected_time(first_selected_time);
  }
};

} // namespace cta::objectstore
