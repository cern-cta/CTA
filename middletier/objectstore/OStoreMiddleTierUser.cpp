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
#include "middletier/objectstore/OStoreMiddleTierUser.hpp"
#include "objectstore/Backend.hpp"
#include "objectstore/RootEntry.hpp"
#include "nameserver/NameServer.hpp"

namespace cta {

OStoreMiddleTierUser::OStoreMiddleTierUser(objectstore::Backend& backend,
    NameServer & nameserver):
  m_backend(backend), m_nameserver(nameserver) {}

OStoreMiddleTierUser::~OStoreMiddleTierUser() throw() { }

void OStoreMiddleTierUser::createDir(
  const SecurityIdentity& requester,
  const std::string& dirPath) {
  m_nameserver.createDir(requester, dirPath, 0777);
}
void OStoreMiddleTierUser::deleteDir(
  const SecurityIdentity& requester,
  const std::string& dirPath) {
  m_nameserver.deleteDir(requester, dirPath);
}

DirIterator OStoreMiddleTierUser::getDirContents(
  const SecurityIdentity& requester,
  const std::string& dirPath) const {
  return m_nameserver.getDirContents(requester, dirPath);
}

DirEntry OStoreMiddleTierUser::stat(
  const SecurityIdentity& requester,
  const std::string path) const {
  return m_nameserver.statDirEntry(requester, path);
}

void OStoreMiddleTierUser::setDirStorageClass(
  const SecurityIdentity& requester,
  const std::string& dirPath,
  const std::string& storageClassName) {
  m_nameserver.setDirStorageClass(requester, dirPath, storageClassName);
}

void OStoreMiddleTierUser::clearDirStorageClass(
  const SecurityIdentity& requester,
  const std::string& dirPath) {
  m_nameserver.clearDirStorageClass(requester, dirPath);
}

std::string OStoreMiddleTierUser::getDirStorageClass(
  const SecurityIdentity& requester,
  const std::string& dirPath) const {
  return m_nameserver.getDirStorageClass(requester, dirPath);
}

void OStoreMiddleTierUser::archive(
  const SecurityIdentity& requester,
  const std::list<std::string>& srcUrls,
  const std::string& dstPath) {
  throw cta::exception::Exception("TODO");
}

std::map<cta::TapePool, std::list<cta::ArchiveToFileRequest> > OStoreMiddleTierUser::getArchiveToFileRequests(
  const SecurityIdentity& requester) const {
  throw cta::exception::Exception("TODO");
}

std::list<cta::ArchiveToFileRequest> OStoreMiddleTierUser::getArchiveToFileRequests(
  const SecurityIdentity& requester,
  const std::string& tapePoolName) const {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierUser::deleteArchiveToFileRequest(
  const SecurityIdentity& requester,
  const std::string& dstPath) {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierUser::retrieve(
  const SecurityIdentity& requester,
  const std::list<std::string>& srcPaths,
  const std::string& dstUrl) {
  throw cta::exception::Exception("TODO");
}

std::map<Tape, std::list<RetrievalJob> > OStoreMiddleTierUser::getRetrievalJobs(
  const SecurityIdentity& requester) const {
  throw cta::exception::Exception("TODO");
}

std::list<RetrievalJob> OStoreMiddleTierUser::getRetrievalJobs(
  const SecurityIdentity& requester,
  const std::string& vid) const {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierUser::deleteRetrievalJob(
  const SecurityIdentity& requester, 
  const std::string& dstUrl) {
  throw cta::exception::Exception("TODO");
}

  
}
