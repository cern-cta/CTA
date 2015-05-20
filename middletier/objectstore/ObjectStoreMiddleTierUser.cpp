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
#include "middletier/objectstore/ObjectStoreMiddleTierUser.hpp"
#include "objectstore/Backend.hpp"
#include "objectstore/RootEntry.hpp"

namespace cta {

OStoreMiddleTierUser::OStoreMiddleTierUser(objectstore::Backend& backend):
  m_backend(backend) {}

OStoreMiddleTierUser::~OStoreMiddleTierUser() throw() { }

void OStoreMiddleTierUser::createDir(
  const SecurityIdentity& requester,
  const std::string& dirPath) {
  throw cta::exception::Exception("TODO");

}
void OStoreMiddleTierUser::deleteDir(
  const SecurityIdentity& requester,
  const std::string& dirPath) {
  throw cta::exception::Exception("TODO");
}

DirIterator OStoreMiddleTierUser::getDirContents(
  const SecurityIdentity& requester,
  const std::string& dirPath) const {
  throw cta::exception::Exception("TODO");
}

DirEntry OStoreMiddleTierUser::stat(
  const SecurityIdentity& requester,
  const std::string path) const {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierUser::setDirStorageClass(
  const SecurityIdentity& requester,
  const std::string& dirPath,
  const std::string& storageClassName) {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierUser::clearDirStorageClass(
  const SecurityIdentity& requester,
  const std::string& dirPath) {
  throw cta::exception::Exception("TODO");
}

std::string OStoreMiddleTierUser::getDirStorageClass(
  const SecurityIdentity& requester,
  const std::string& dirPath) const {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierUser::archive(
  const SecurityIdentity& requester,
  const std::list<std::string>& srcUrls,
  const std::string& dstPath) {
  throw cta::exception::Exception("TODO");
}

std::map<TapePool, std::list<ArchivalJob> > OStoreMiddleTierUser::getArchivalJobs(
  const SecurityIdentity& requester) const {
  throw cta::exception::Exception("TODO");
}

std::list<ArchivalJob> OStoreMiddleTierUser::getArchivalJobs(
  const SecurityIdentity& requester,
  const std::string& tapePoolName) const {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierUser::deleteArchivalJob(
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
