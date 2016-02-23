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

#include <iostream>
#include <map>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/exception/Exception.hpp"
#include "common/remoteFS/RemotePath.hpp"
#include "common/utils/utils.hpp"
#include "remotens/EosNS.hpp"

#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClFile.hh"

#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::EosNS::EosNS(const std::string &XrootServerURL): m_xrootServerURL(XrootServerURL) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::EosNS::~EosNS() throw() {
}

//------------------------------------------------------------------------------
// statFile
//------------------------------------------------------------------------------
std::unique_ptr<cta::RemoteFileStatus> cta::EosNS::statFile(const RemotePath &path) const {
  XrdCl::FileSystem fs{XrdCl::URL(m_xrootServerURL)};
  XrdCl::StatInfo *stat_res = NULL;
  XrdCl::XRootDStatus s = fs.Stat(path.getAfterScheme(), stat_res, 0);
  if(!s.IsOK()) {
    delete stat_res;
    return std::unique_ptr<RemoteFileStatus>();
  }
  UserIdentity owner(0,0);
  //here we cannot get the mode bits from EOS with stat_res->GetFlags() (xroot does not allow this to work properly)
  //therefore we hardcode a fully permissive mode for the prototype
  cta::RemoteFileStatus *rfs = new cta::RemoteFileStatus(owner, 0666, stat_res->GetSize());
  delete stat_res;
  return std::unique_ptr<RemoteFileStatus>(rfs);
}

//------------------------------------------------------------------------------
// rename
//------------------------------------------------------------------------------
void cta::EosNS::rename(const RemotePath &remoteFile, const RemotePath &newRemoteFile) {
  XrdCl::FileSystem fs{XrdCl::URL(m_xrootServerURL)};
  XrdCl::XRootDStatus s = fs.Mv(remoteFile.getAfterScheme(), newRemoteFile.getAfterScheme());
  if(!s.IsOK()) {
    throw cta::exception::Exception(std::string("Error renaming ")+remoteFile.getAfterScheme()+" to "+newRemoteFile.getAfterScheme()+" Reason: "+s.GetErrorMessage());
  }  
}

//------------------------------------------------------------------------------
// getFilename
//------------------------------------------------------------------------------
std::string cta::EosNS::getFilename(const RemotePath &remoteFile) const {
  const std::string afterScheme = remoteFile.getAfterScheme();
  const std::string afterSchemeTrimmed = utils::trimSlashes(afterScheme);
  return utils::getEnclosedName(afterSchemeTrimmed);
}

//------------------------------------------------------------------------------
// createEntry
//------------------------------------------------------------------------------
void cta::EosNS::createEntry(const RemotePath &path, const RemoteFileStatus &status) {
  XrdCl::File newFile;
  XrdCl::Access::Mode mode;
  if(status.mode & S_IRUSR) mode|= XrdCl::Access::Mode::UR;
  if(status.mode & S_IWUSR) mode|= XrdCl::Access::Mode::UW;
  if(status.mode & S_IXUSR) mode|= XrdCl::Access::Mode::UX;
  if(status.mode & S_IRGRP) mode|= XrdCl::Access::Mode::GR;
  if(status.mode & S_IWGRP) mode|= XrdCl::Access::Mode::GW;
  if(status.mode & S_IXGRP) mode|= XrdCl::Access::Mode::GX;
  if(status.mode & S_IROTH) mode|= XrdCl::Access::Mode::OR;
  if(status.mode & S_IWOTH) mode|= XrdCl::Access::Mode::OW;
  if(status.mode & S_IXOTH) mode|= XrdCl::Access::Mode::OX;
  XrdCl::OpenFlags::Flags flags;
  flags |= XrdCl::OpenFlags::Flags::Delete;
  std::string eosPath(std::string("root://")+m_xrootServerURL+path.getAfterScheme());
  XrdCl::XRootDStatus s = newFile.Open(eosPath, flags, mode);
  if(!s.IsOK()) {
    throw cta::exception::Exception(std::string("Error creating EOS file/dir ")+eosPath+" Reason: "+s.GetErrorMessage());
  }
  s = newFile.Close();
  if(!s.IsOK()) {
    throw cta::exception::Exception(std::string("Error closing EOS file/dir ")+eosPath+" Reason: "+s.GetErrorMessage());
  }
}
