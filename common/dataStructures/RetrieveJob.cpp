/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/RetrieveJob.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool RetrieveJob::operator==(const RetrieveJob& rhs) const {
  return request == rhs.request && fileSize == rhs.fileSize && tapeCopies == rhs.tapeCopies;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool RetrieveJob::operator!=(const RetrieveJob& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const RetrieveJob& obj) {
  os << "(request=" << obj.request << " fileSize=" << obj.fileSize << " tapeFiles=" << obj.tapeCopies << ")";
  return os;
}

}  // namespace cta::common::dataStructures
