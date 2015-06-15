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

#include "common/exception/Exception.hpp"
#include "remotens/MockRemoteNS.hpp"

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockRemoteNS::~MockRemoteNS() throw() {
}

//------------------------------------------------------------------------------
// regularFileExists
//------------------------------------------------------------------------------
bool cta::MockRemoteNS::regularFileExists(const std::string &remoteFile)
  const {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// dirExists
//------------------------------------------------------------------------------
bool cta::MockRemoteNS::dirExists(const std::string &remoteFile) const {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// rename
//------------------------------------------------------------------------------
void cta::MockRemoteNS::rename(const std::string &remoteFile,
  const std::string &newRemoteFile) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}
