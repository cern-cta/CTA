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

#include "common/dataStructures/MountPolicy.hpp"

#include <list>

namespace cta {
namespace catalogue {

/**
 * Structure containing a list of requester mount-polices and a list of
 * requester-group mount-policies.
 */
struct RequesterAndGroupMountPolicies {

  /**
   * List of requester activity mount-policies.
   */
  std::list<common::dataStructures::MountPolicy> requesterActivityMountPolicies;

  /**
   * List of requester mount-policies.
   */
  std::list<common::dataStructures::MountPolicy> requesterMountPolicies;

  /**
   * List of requester-group mount-policies.
   */
  std::list<common::dataStructures::MountPolicy> requesterGroupMountPolicies;

}; // struct RequesterAndGroupMountPolicies

} // namespace catalogue
} // namespace cta
