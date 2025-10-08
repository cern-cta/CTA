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
 * Rule specifying which mount policy will be used for a specific group of
 * data-transfer requesters.
 */
struct RequesterGroupMountRule {

  RequesterGroupMountRule() = default;

  bool operator==(const RequesterGroupMountRule &rhs) const;

  bool operator!=(const RequesterGroupMountRule &rhs) const;

  /**
   * The name of the disk instance to which the requester group belongs.
   */
  std::string diskInstance;

  /**
   * The name of the requester group which is only guaranteed to be unique
   * within its disk instance.
   */
  std::string name;

  std::string mountPolicy;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

}; // struct RequesterGroupMountRule

std::ostream &operator<<(std::ostream &os, const RequesterGroupMountRule &obj);

} // namespace cta::common::dataStructures
