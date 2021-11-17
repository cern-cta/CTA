/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "LifecycleTimings.hpp"
namespace cta{
namespace common{
namespace dataStructures{

LifecycleTimings::LifecycleTimings() : creation_time(0), first_selected_time(0), completed_time(0) {}

LifecycleTimings::LifecycleTimings(const LifecycleTimings& orig): creation_time(orig.creation_time), first_selected_time(orig.first_selected_time), completed_time(orig.completed_time) {
}

LifecycleTimings::~LifecycleTimings() {
}

time_t LifecycleTimings::getTimeForSelection(){
  if(first_selected_time != 0 && creation_time != 0){
    return first_selected_time - creation_time;
  }
  return 0;
}

time_t LifecycleTimings::getTimeForCompletion(){
  if(completed_time != 0 && creation_time != 0){
    return completed_time - creation_time;
  }
  return 0;
}

}}}