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

#pragma once

#include <cstdint>

namespace castor::tape::tapeserver::rao {

/**
 * Represents the physical position of a block on tape
 * (wrapNumber, longitudinal position)
 */
class Position {
public:
  Position() = default;
  virtual ~Position() = default;

  /**
   * Get the wrap number of this physical position
   * @return the wrap number of this physical position
   */
  uint32_t getWrap() const { return m_wrap; }
  /**
   * Get the longitudinal position of this physical position
   * @return the longitudinal position of this physical position
   */
  uint64_t getLPos() const { return m_lpos; }
  /**
   * Set the wrap number of this physical position
   * @param wrap this wrap number
   */
  void setWrap(const uint32_t wrap) { m_wrap = wrap; }
  /**
   * Set the longitudinal position of this physical position
   * @param lpos this longitudinal position
   */
  void setLPos(const uint64_t lpos) { m_lpos = lpos; }

private:
  uint32_t m_wrap = 0;
  uint64_t m_lpos = 0;
};

} // namespace castor::tape::tapeserver::rao
