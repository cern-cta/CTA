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

#include "common/VO.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::VO::VO() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::VO::~VO() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::VO::VO(
  const std::string &name,
  const CreationLog & creationLog):
  m_name(name), m_creationLog(creationLog) {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::VO::getName() const throw() {
  return m_name;
}
