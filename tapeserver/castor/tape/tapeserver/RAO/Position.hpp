/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <cstdint>

namespace castor { namespace tape { namespace tapeserver { namespace rao {

/**
 * Represents the physical position of a block on tape
 * (wrapNumber, longitudinal position)
 */
class Position {
public:
  Position();
  Position(const Position & other);
  Position &operator=(const Position &other);
  virtual ~Position();
  /**
   * Get the wrap number of this physical position
   * @return the wrap number of this physical position
   */
  uint32_t getWrap() const;
  /**
   * Get the longitudinal position of this physical position
   * @return the longitudinal position of this physical position
   */
  uint64_t getLPos() const;
  /**
   * Set the wrap number of this physical position
   * @param wrap this wrap number
   */
  void setWrap(const uint32_t wrap);
  /**
   * Set the longitudinal position of this physical position
   * @param lpos this longitudinal position
   */
  void setLPos(const uint64_t lpos);
private:
  uint32_t m_wrap = 0;
  uint64_t m_lpos = 0;
};

}}}}
