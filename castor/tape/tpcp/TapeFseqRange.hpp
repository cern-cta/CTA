/******************************************************************************
 *                 castor/tape/tpcp/TapeFseqRange.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TPCP_TAPEFSEQRANGE_HPP
#define CASTOR_TAPE_TPCP_TAPEFSEQRANGE_HPP 1

#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoValue.hpp"

#include <ostream>
#include <stdint.h>

namespace castor {
namespace tape   {
namespace tpcp   {

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
  TapeFseqRange() throw();

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
    throw(exception::InvalidArgument);

  /**
   * Resets the range to be an empty range.
   */
  void reset() throw();

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
    throw(exception::InvalidArgument);

  /**
   * Returns true if the range is empty.
   */
  bool isEmpty() const throw();

  /**
   * Returns the inclusive lower bound of the range or 0 if the range is empty.
   */
  uint32_t lower() const throw();

  /**
   * Returns the inclusive upper bound of the range.  If the range is finite,
   * then a value greater than 0 is returned.  If the range is either empty or
   * infinite then 0 is returned.  The method isEmpty() or a lower() return
   * value of 0 can be used to distinguish between the two cases when upper()
   * returns 0.
   */
  uint32_t upper() const throw();

  /**
   * Returns the size of the range.  An empty or infinite range returns 0.
   * The method isEmpty() or a lower() return value of 0 can be used to
   * distinguish between the two cases when size() returns 0.
   */
  uint32_t size() const throw();


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

} // namespace tpcp
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TPCP_TAPEFSEQRANGE_HPP
