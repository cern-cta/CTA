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


namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct holds the username and group name of a given user 
 */
struct OwnerIdentity {

  OwnerIdentity();
  
  OwnerIdentity(uint32_t uid, uint32_t gid);

  bool operator==(const OwnerIdentity &rhs) const;

  bool operator!=(const OwnerIdentity &rhs) const;

  uint32_t uid;
  uint32_t gid;

}; // struct OwnerIdentity

std::ostream &operator<<(std::ostream &os, const OwnerIdentity &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
