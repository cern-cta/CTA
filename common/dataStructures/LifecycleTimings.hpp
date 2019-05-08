/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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

#include <ctime>

namespace cta{
namespace common{
namespace dataStructures{
    
/**
 * This class stores the informations about a request's lifecycle timings
 */
class LifecycleTimings {
public:
  LifecycleTimings();
  LifecycleTimings(const LifecycleTimings& orig);
  virtual ~LifecycleTimings();
  /**
   * Returns the elapsed time between the creation of the request
   * and the selection of the request for mounting
   */
  time_t getTimeForSelection();
  /**
   * Returns the elapsed time between the creation of the request and
   * its completion
   */
  time_t getTimeForCompletion();
  time_t creation_time;
  time_t first_selected_time;
  time_t completed_time;
};

}}}
