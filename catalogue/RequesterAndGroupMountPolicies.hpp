/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/dataStructures/MountPolicy.hpp"

#include <list>

namespace cta::catalogue {

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

} // namespace cta::catalogue
