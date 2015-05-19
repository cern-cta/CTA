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

#include "middletier/common/TapeFileLocation.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapeFileLocation::TapeFileLocation():
  m_fseq(0),
  m_blockId(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapeFileLocation::TapeFileLocation(const std::string &vid,
  const uint64_t fseq, const uint64_t blockId):
  m_vid(vid),
  m_fseq(0),
  m_blockId(0) {
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &cta::TapeFileLocation::getVid() const throw() {
  return m_vid;
}

//------------------------------------------------------------------------------
// getFseq
//------------------------------------------------------------------------------
uint64_t cta::TapeFileLocation::getFseq() const throw() {
  return m_fseq;
}

//------------------------------------------------------------------------------
// getBlockId
//------------------------------------------------------------------------------
uint64_t cta::TapeFileLocation::getBlockId() const throw() {
  return m_blockId;
}
