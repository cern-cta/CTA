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

#include "cta/Mount.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Mount::Mount(const std::string &id, const std::string &vid):
  m_id(id),
  m_vid(vid) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::Mount::~Mount() throw() {
}

//------------------------------------------------------------------------------
// getId
//------------------------------------------------------------------------------
const std::string &cta::Mount::getId() const throw() {
  return m_id;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &cta::Mount::getVid() const throw() {
  return m_vid;
}
