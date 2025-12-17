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

#include "common/dataStructures/RetrieveJobToAdd.hpp"

#include <ostream>

namespace cta::common::dataStructures {
bool RetrieveJobToAdd::operator==(const RetrieveJobToAdd& rhs) const {
  return copyNb == rhs.copyNb && fSeq == rhs.fSeq && retrieveRequestAddress == rhs.retrieveRequestAddress
         && fileSize == rhs.fileSize && startTime == rhs.startTime && policy == rhs.policy && activity == rhs.activity
         && diskSystemName == rhs.diskSystemName;
}

bool RetrieveJobToAdd::operator!=(const RetrieveJobToAdd& rhs) const {
  return !operator==(rhs);
}

std::ostream& operator<<(std::ostream& os, const RetrieveJobToAdd& obj) {
  os << "(copyNb=" << static_cast<unsigned int>(obj.copyNb) << " fSeq=" << static_cast<unsigned int>(obj.fSeq)
     << " retrieveRequestAddress=" << obj.retrieveRequestAddress
     << " fileSize=" << static_cast<unsigned int>(obj.fileSize) << " policy=" << obj.policy
     << " startTime=" << static_cast<unsigned int>(obj.startTime);

  if (obj.activity.has_value()) {
    os << " activity=" << obj.activity.value();
  }

  if (obj.diskSystemName.has_value()) {
    os << " diskSystemName=" << obj.diskSystemName.value();
  }

  os << ")";

  return os;
}
}  // namespace cta::common::dataStructures
