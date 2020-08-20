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

#include "Position.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

Position::Position():m_wrap(0),m_lpos(0) {
}

Position::~Position() {
}

uint64_t Position::getLPos() const {
  return m_lpos;
}

uint64_t Position::getWrap() const {
  return m_wrap;
}

void Position::setLPos(const uint64_t lpos) {
  m_lpos = lpos;
}

void Position::setWrap(const uint64_t wrap) {
  m_wrap = wrap;
}


}}}}