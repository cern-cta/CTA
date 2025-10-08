/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <ctime>

namespace cta::common::dataStructures {

/**
 * This struct stores the lifecycle timings for a request
 */
struct LifecycleTimings {
  /**
   * Returns the elapsed time between the creation of the request
   * and the selection of the request for mounting
   */
  time_t getTimeForSelection() const;
  /**
   * Returns the elapsed time between the creation of the request and
   * its completion
   */
  time_t getTimeForCompletion() const;

  time_t creation_time = 0;
  time_t first_selected_time = 0;
  time_t completed_time = 0;
};

} // namespace cta::common::dataStructures
