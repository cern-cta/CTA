/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "Position.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace rao {

Position::Position() : m_wrap(0), m_lpos(0) {}

Position::Position(const Position& other) {
  if (this != &other) {
    m_wrap = other.m_wrap;
    m_lpos = other.m_lpos;
  }
}

Position& Position::operator=(const Position& other) {
  if (this != &other) {
    m_wrap = other.m_wrap;
    m_lpos = other.m_lpos;
  }
  return *this;
}

Position::~Position() {}

uint64_t Position::getLPos() const {
  return m_lpos;
}

uint32_t Position::getWrap() const {
  return m_wrap;
}

void Position::setLPos(const uint64_t lpos) {
  m_lpos = lpos;
}

void Position::setWrap(const uint32_t wrap) {
  m_wrap = wrap;
}

}  // namespace rao
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor