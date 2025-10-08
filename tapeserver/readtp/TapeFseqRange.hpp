/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>
#include <ostream>

namespace cta::tapeserver::readtp {

/**
 * A range of tape file sequence numbers specified by an inclusive lower
 * boundary and an inclusive upper boundary.
 */
class TapeFseqRange {

public:

  /**
   * Constructor.
   *
   * Constructs an empty range.
   */
  TapeFseqRange() noexcept;

  /**
   * Constructor.
   *
   * Constructs a range with the specified inclusive lower and upper
   * boundaries.  An upper boundary of 0 means infinity.
   *
   * Throws an InvalidArgument exception if either the lower boundary is 0
   * or if the lower boundary is greater than the upper boundary.
   *
   * @param lower The inclusive lower bound of the range.
   * @param upper The inclusive upper bound of the range.
   */
  TapeFseqRange(const uint32_t lower, const uint32_t upper)
    ;

  /**
   * Resets the range to be an empty range.
   */
  void reset() noexcept;

  /**
   * Resets the range to be a finite range with the specified inclusive lower
   * and upper boundaries.
   *
   * Throws an InvalidArgument exception if either the lower boundary is 0
   * or if the lower boundary is greater than the upper boundary.
   *
   * @param lower The inclusive lower bound of the range.
   * @param upper The inclusive upper bound of the range.
   */
  void reset(const uint32_t lower, const uint32_t upper)
    ;

  /**
   * Returns true if the range is empty.
   */
  bool isEmpty() const noexcept;

  /**
   * Returns the inclusive lower bound of the range or 0 if the range is empty.
   */
  uint32_t lower() const noexcept;

  /**
   * Returns the inclusive upper bound of the range.  If the range is finite,
   * then a value greater than 0 is returned.  If the range is either empty or
   * infinite then 0 is returned.  The method isEmpty() or a lower() return
   * value of 0 can be used to distinguish between the two cases when upper()
   * returns 0.
   */
  uint32_t upper() const noexcept;

  /**
   * Returns the size of the range.  An empty or infinite range returns 0.
   * The method isEmpty() or a lower() return value of 0 can be used to
   * distinguish between the two cases when size() returns 0.
   */
  uint32_t size() const noexcept;


private:

  /**
   * True if this range is empty, else false.
   */
  bool m_isEmpty;

  /**
   * The inclusive lower bound of the range.
   */
  uint32_t m_lower;

  /**
   * The inclusive upper bound of the range.  A value of 0 means infinity.
   */
  uint32_t m_upper;
};

} // namespace cta::tapeserver::readtp

/**
 * ostream << operator for cta::tapeserver::readtp::TapeFseqRange
 */
std::ostream &operator<<(std::ostream &os,
  const cta::tapeserver::readtp::TapeFseqRange &value);

