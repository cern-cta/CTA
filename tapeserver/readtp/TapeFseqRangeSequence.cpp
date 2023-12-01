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

#include "tapeserver/readtp/TapeFseqRangeSequence.hpp"
#include "tapeserver/readtp/TapeFseqRange.hpp"
#include "common/exception/Exception.hpp"

#include <getopt.h>
#include <ostream>

#include <list>

namespace cta::tapeserver::readtp {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFseqRangeSequence::TapeFseqRangeSequence() noexcept {
  reset();
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFseqRangeSequence::TapeFseqRangeSequence(
  const TapeFseqRange &range) noexcept {
  reset(range);
}


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void TapeFseqRangeSequence::reset() noexcept {

  // Reset the range to be empty
  m_range.reset();

  m_next = 0; // Ignored
}


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void TapeFseqRangeSequence::reset(
  const TapeFseqRange &range) noexcept {
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
bool TapeFseqRangeSequence::hasMore() const noexcept {

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
  &TapeFseqRangeSequence::range() const noexcept {
  return m_range;
}


} // namespace cta::tapeserver::readtp
