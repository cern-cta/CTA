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

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RemotePath::RemotePath() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RemotePath::RemotePath(const std::string &rawPath):
  m_rawPath(rawPath) {
  auto pos = rawPath.find(':');
  if(std::string::npos != pos) {
    m_protocol = rawPath.substr(0, pos);
  }
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::RemotePath::operator==(const RemotePath &rhs) const {
  return m_rawPath == rhs.m_rawPath;
}

//------------------------------------------------------------------------------
// getRawPath
//------------------------------------------------------------------------------
const std::string &cta::RemotePath::getRawPath() const throw() {
  return m_rawPath;
}

//------------------------------------------------------------------------------
// getProtocol
//------------------------------------------------------------------------------
const std::string &cta::RemotePath::getProtocol() const throw() {
  return m_protocol;
}
