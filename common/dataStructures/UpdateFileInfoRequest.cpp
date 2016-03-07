/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::UpdateFileInfoRequest::~UpdateFileInfoRequest() throw() {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::UpdateFileInfoRequest::operator==(const UpdateFileInfoRequest &rhs) const {
  return archiveFileID==rhs.archiveFileID
      && drData==rhs.drData
      && requester==rhs.requester
      && storageClass==rhs.storageClass;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::UpdateFileInfoRequest::operator!=(const UpdateFileInfoRequest &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::UpdateFileInfoRequest &obj) {
  os << "(archiveFileID=" << obj.archiveFileID
     << " drData=" << obj.drData
     << " requester=" << obj.requester
     << " storageClass=" << obj.storageClass << ")";
  return os;
}

