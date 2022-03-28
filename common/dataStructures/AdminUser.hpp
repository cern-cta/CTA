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
 * This is the administrative user which contains the username of the admin 
 */
struct AdminUser {

  AdminUser();

  bool operator==(const AdminUser &rhs) const;

  bool operator!=(const AdminUser &rhs) const;

  std::string name;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

}; // struct AdminUser

std::ostream &operator<<(std::ostream &os, const AdminUser &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
