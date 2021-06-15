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

#pragma once
namespace cta{
namespace objectstore{
  class LifecycleTimingsSerDeser: public cta::common::dataStructures::LifecycleTimings{
    public:
      LifecycleTimingsSerDeser() : cta::common::dataStructures::LifecycleTimings() {}
      LifecycleTimingsSerDeser(const cta::common::dataStructures::LifecycleTimings& lifecycleTimings) : cta::common::dataStructures::LifecycleTimings(lifecycleTimings) {}
      operator cta::common::dataStructures::LifecycleTimings() {
	return cta::common::dataStructures::LifecycleTimings(*this);
      } 

      void deserialize(const cta::objectstore::serializers::LifecycleTimings& ostoreLifecycleTimings){
	completed_time = ostoreLifecycleTimings.completed_time();
	creation_time = ostoreLifecycleTimings.creation_time();
	first_selected_time = ostoreLifecycleTimings.first_selected_time();
      }
      
      void serialize(cta::objectstore::serializers::LifecycleTimings &lifecycleTimings){
	lifecycleTimings.set_completed_time(completed_time);
	lifecycleTimings.set_creation_time(creation_time);
	lifecycleTimings.set_first_selected_time(first_selected_time);
      }
  };
}}
