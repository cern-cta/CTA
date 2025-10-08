/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
