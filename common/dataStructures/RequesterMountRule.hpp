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

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * Rule specifying which mount policy will be used for a specific data-transfer
 * requester.
 */
struct RequesterMountRule {

  RequesterMountRule();

  bool operator==(const RequesterMountRule &rhs) const;

  bool operator!=(const RequesterMountRule &rhs) const;

  /**
   * The name of the disk instance to which the requester belongs.
   */
  std::string diskInstance;

  /**
   * The name of the requester which is only guaranteed to be unqiue within its
   * disk instance.
   */
  std::string name;

  std::string mountPolicy;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

}; // struct RequesterMountRule

std::ostream &operator<<(std::ostream &os, const RequesterMountRule &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
