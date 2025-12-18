/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
