/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "LifecycleTimings.hpp"

namespace cta::common::dataStructures {

time_t LifecycleTimings::getTimeForSelection() const {
  if (first_selected_time != 0 && creation_time != 0) {
    return first_selected_time - creation_time;
  } else {
    return 0;
  }
}

time_t LifecycleTimings::getTimeForCompletion() const {
  if (completed_time != 0 && creation_time != 0) {
    return completed_time - creation_time;
  } else {
    return 0;
  }
}

}  // namespace cta::common::dataStructures
