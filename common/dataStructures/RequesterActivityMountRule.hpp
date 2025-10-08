/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta::common::dataStructures {

/**
 * Rule specifying which mount policy will be used for a specific data-transfer
 * requester.
 */
struct RequesterActivityMountRule {

  RequesterActivityMountRule() = default;

  bool operator==(const RequesterActivityMountRule &rhs) const;

  bool operator!=(const RequesterActivityMountRule &rhs) const;

  /**
   * The name of the disk instance to which the requester belongs.
   */
  std::string diskInstance;

  /**
   * The name of the requester which is only guaranteed to be unqiue within its
   * disk instance.
   */
  std::string name;

  /**
   * The activity regex that matched the requests that use this rule
   */
  std::string activityRegex;

  std::string mountPolicy;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

}; // struct RequesterMountRule

std::ostream &operator<<(std::ostream &os, const RequesterActivityMountRule &obj);

} // namespace cta::common::dataStructures
