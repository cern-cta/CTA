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

#include <map>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/exception/Exception.hpp"
#include "common/RemotePath.hpp"
#include "common/Utils.hpp"
#include "remotens/EosNS.hpp"

#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::EosNS::EosNS(const std::string &XrootServerURL): m_fs(XrdCl::URL(XrootServerURL)) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::EosNS::~EosNS() throw() {
}

//------------------------------------------------------------------------------
// statFile
//------------------------------------------------------------------------------
std::unique_ptr<cta::RemoteFileStatus> cta::EosNS::statFile(const RemotePath &path) {
  XrdCl::StatInfo *stat_res = NULL;
  XrdCl::XRootDStatus s = m_fs.Stat(path.getAfterScheme(), stat_res, 0);
  if(!s.IsOK()) {
    delete stat_res;
    return std::unique_ptr<RemoteFileStatus>();
  }
  UserIdentity owner(0,0);
  cta::RemoteFileStatus *rfs = new cta::RemoteFileStatus(owner, stat_res->GetFlags(), stat_res->GetSize());
  delete stat_res;
  return std::unique_ptr<RemoteFileStatus>(rfs);
}

//------------------------------------------------------------------------------
// rename
//------------------------------------------------------------------------------
void cta::EosNS::rename(const RemotePath &remoteFile, const RemotePath &newRemoteFile) {
}

//------------------------------------------------------------------------------
// getFilename
//------------------------------------------------------------------------------
std::string cta::EosNS::getFilename(const RemotePath &remoteFile) const {
  const std::string afterScheme = remoteFile.getAfterScheme();
  const std::string afterSchemeTrimmed = Utils::trimSlashes(afterScheme);
  return Utils::getEnclosedName(afterSchemeTrimmed);
}

//------------------------------------------------------------------------------
// createEntry
//------------------------------------------------------------------------------
void cta::EosNS::createEntry(const RemotePath &path, const RemoteFileStatus &status) {
}

//------------------------------------------------------------------------------
// sendCommand
//------------------------------------------------------------------------------
int cta::EosNS::sendCommand(const std::string &cmd) const {
  std::string cmdPath = "root://localhost:1094//";
  return 0;
}