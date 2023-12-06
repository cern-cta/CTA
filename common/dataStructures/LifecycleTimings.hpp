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

#include <ctime>

namespace cta::common::dataStructures {

/**
 * This struct stores the lifecycle timings for a request
 */
struct LifecycleTimings {
  LifecycleTimings();
  LifecycleTimings(const LifecycleTimings& orig) = default;
  virtual ~LifecycleTimings() = default;
  LifecycleTimings& operator=(const LifecycleTimings& orig);
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

  time_t creation_time;
  time_t first_selected_time;
  time_t completed_time;
};

} // namespace cta::common::dataStructures
