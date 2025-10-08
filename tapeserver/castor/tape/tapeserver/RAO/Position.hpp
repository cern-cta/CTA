/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
