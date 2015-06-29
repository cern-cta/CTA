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

#include "common/RemotePath.hpp"
#include "common/exception/Exception.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RemotePath::RemotePath() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RemotePath::RemotePath(const std::string &raw):
  m_raw(raw) {
  auto const pos = raw.find(':');

  if(std::string::npos == pos) {
    std::ostringstream msg;
    msg << "Failed to instantiate RemotePath object for URI " << raw <<
      " because the colon is missing as in scheme:hierarchical_part";
    throw exception::Exception(msg.str());
  }

  if(0 >= pos) {
    std::ostringstream msg;
    msg << "Failed to instantiate RemotePath object for URI " << raw <<
     " because the scheme before the colon is an empty string";
    throw exception::Exception(msg.str());
  }

  const auto indexOfLastChar = raw.length() - 1;
  if(pos == indexOfLastChar) {
    std::ostringstream msg;
    msg << "Failed to instantiate RemotePath object for URI " << raw <<
     " because the hierarchical part after the colon is an empty string";
    throw exception::Exception(msg.str());
  }

  m_scheme = raw.substr(0, pos);
  m_hier = raw.substr(pos + 1, indexOfLastChar - pos);
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::RemotePath::operator==(const RemotePath &rhs) const {
  return m_raw == rhs.m_raw;
}

//------------------------------------------------------------------------------
// getRaw
//------------------------------------------------------------------------------
const std::string &cta::RemotePath::getRaw() const throw() {
  return m_raw;
}

//------------------------------------------------------------------------------
// getScheme
//------------------------------------------------------------------------------
const std::string &cta::RemotePath::getScheme() const throw() {
  return m_scheme;
}

//------------------------------------------------------------------------------
// getHier
//------------------------------------------------------------------------------
const std::string &cta::RemotePath::getHier() const throw() {
  return m_hier;
}
