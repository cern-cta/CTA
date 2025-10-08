/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/TapePool.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapePool::operator==(const TapePool &rhs) const {
  return name == rhs.name;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool TapePool::operator!=(const TapePool &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapePool &obj) {
  os << "(name=" << obj.name
     << " vo=" << obj.vo.name
     << " nbPartialTapes=" << obj.nbPartialTapes
     << " encryption=" << obj.encryption
     << " encryptionKeyName=" << obj.encryptionKeyName.value_or("")
     << " nbTapes=" << obj.nbTapes
     << " nbEmptyTapes=" << obj.nbEmptyTapes
     << " nbDisabledTapes=" << obj.nbDisabledTapes
     << " nbFullTapes=" << obj.nbFullTapes
     << " nbReadOnlyTapes=" << obj.nbReadOnlyTapes
     << " capacityBytes=" << obj.capacityBytes
     << " dataBytes=" << obj.dataBytes
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

} // namespace cta::catalogue
