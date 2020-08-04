/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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

#include "DeletedArchiveFileItor.hpp"
#include "DeletedArchiveFileItorImpl.hpp"


namespace cta {
namespace catalogue {


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DeletedArchiveFileItor::DeletedArchiveFileItor():
  m_impl(nullptr) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DeletedArchiveFileItor::DeletedArchiveFileItor(DeletedArchiveFileItorImpl *const impl):
  m_impl(impl) {
  if(nullptr == impl) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Pointer to implementation object is null");
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DeletedArchiveFileItor::DeletedArchiveFileItor(DeletedArchiveFileItor &&other):
  m_impl(other.m_impl) {
  other.m_impl = nullptr;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
DeletedArchiveFileItor::~DeletedArchiveFileItor() {
  delete m_impl;
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
DeletedArchiveFileItor &DeletedArchiveFileItor::operator=(DeletedArchiveFileItor &&rhs) {
  // Protect against self assignment
  if(this != &rhs) {
    // Avoid memory leak
    delete m_impl;

    m_impl = rhs.m_impl;
    rhs.m_impl = nullptr;
  }
  return *this;
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool DeletedArchiveFileItor::hasMore() const {
  if(nullptr == m_impl) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: "
      "This iterator is invalid");
  }
  return m_impl->hasMore();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
common::dataStructures::DeletedArchiveFile DeletedArchiveFileItor::next() {
  if(nullptr == m_impl) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: "
      "This iterator is invalid");
  }
  return m_impl->next();
}

}}
