/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "tapeserver/readtp/TapeFseqRangeSequence.hpp"
#include "tapeserver/readtp/TapeFseqRange.hpp"
#include "common/exception/Exception.hpp"

#include <getopt.h>
#include <ostream>

#include <list>

namespace cta {
namespace tapeserver {
namespace readtp {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFseqRangeSequence::TapeFseqRangeSequence() throw() {
  reset();
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFseqRangeSequence::TapeFseqRangeSequence(
  const TapeFseqRange &range) throw() {
  reset(range);
}


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void TapeFseqRangeSequence::reset() throw() {

  // Reset the range to be empty
  m_range.reset();

  m_next = 0; // Ignored
}


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void TapeFseqRangeSequence::reset(
  const TapeFseqRange &range) throw() {
  m_range = range;

  if(range.isEmpty()) {
    m_next = 0; // Ignored
  } else {
    m_next = m_range.lower();
  }
}


//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool TapeFseqRangeSequence::hasMore() const throw() {

  if(m_range.isEmpty()) {

    return false;

  } else {

    // Infinity is represented by range.upper() == 0
    return m_range.upper() == 0 || m_next <= m_range.upper();
  }
}


//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
uint32_t TapeFseqRangeSequence::next()
   {

  if(!hasMore()) {
    exception::Exception ex;

    ex.getMessage()
      << "Sequence::next() called after end of sequence";

    throw ex;
  }

  return m_next++;
}


//------------------------------------------------------------------------------
// range
//------------------------------------------------------------------------------
const TapeFseqRange
  &TapeFseqRangeSequence::range() const throw() {
  return m_range;
}


} // namespace readtp
} // namespace tapeserver
} // namespace cta