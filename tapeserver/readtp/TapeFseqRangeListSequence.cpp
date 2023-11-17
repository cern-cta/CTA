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

#include "tapeserver/readtp/TapeFseqRange.hpp"
#include "tapeserver/readtp/TapeFseqRangeListSequence.hpp"
#include "common/exception/InvalidArgument.hpp"
#include "common/exception/Exception.hpp"

#include <getopt.h>
#include <ostream>

#include <list>
#include <errno.h>

namespace cta::tapeserver::readtp {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFseqRangeListSequence::TapeFseqRangeListSequence()
   {
  reset(nullptr);
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFseqRangeListSequence::TapeFseqRangeListSequence(
  const TapeFseqRangeList *const list)  {
  reset(list);
}


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void TapeFseqRangeListSequence::reset(
  const TapeFseqRangeList *const list)  {
  m_list = list;

  if(m_list == nullptr) {
    m_isFinite  = true;
    m_totalSize = 0;
  } else {
    m_rangeItor  = list->begin();
    m_nbSequence = (*(list->begin()));

    // Determine the values of m_isFinite and m_totalSize
    m_isFinite  = true; // Initial guess
    m_totalSize = 0;    // Initial guess
    for(TapeFseqRangeList::const_iterator itor=list->begin();
      itor != list->end(); ++itor) {
      const TapeFseqRange &range = *itor;

      // If upper bound of range is infinity
      if(range.upper() == 0) {
        m_isFinite  = false;
        m_totalSize = 0;

        // No need to continue counting
        break;

      // Else the upper bound is finite
      } else {
        m_totalSize += range.size();
      }
    }
  }
}


//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool TapeFseqRangeListSequence::hasMore() const throw() {
  if(m_list != nullptr) {
    return m_nbSequence.hasMore();
  } else {
    return false;
  }
}


//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
uint32_t TapeFseqRangeListSequence::next()
   {

  if(!hasMore()) {
    exception::Exception ex;

    ex.getMessage()
      << "Invalid operation: Sequence::next() called after end of sequence";

    throw ex;
  }

  uint32_t tmp = m_nbSequence.next();

  // If the end of the current range sequence has been reached
  if(!m_nbSequence.hasMore()) {

    // Move on to the next if there is one
    m_rangeItor++;
    if(m_rangeItor != m_list->end()) {
      m_nbSequence = *m_rangeItor;
    }
  }

  return tmp;
}


//------------------------------------------------------------------------------
// isFinite
//------------------------------------------------------------------------------
bool TapeFseqRangeListSequence::isFinite() const throw() {
  return m_isFinite;
}


//------------------------------------------------------------------------------
// totalSize
//------------------------------------------------------------------------------
uint32_t TapeFseqRangeListSequence::totalSize()
  const throw() {
  return m_totalSize;
}

} // namespace cta::tapeserver::readtp

//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::TapeFseqRangeList
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const cta::tapeserver::readtp::TapeFseqRangeList &value) {

  os << '{';

  for(cta::tapeserver::readtp::TapeFseqRangeList::const_iterator itor =
    value.begin(); itor != value.end(); ++itor) {

    // Write a separating comma if not the first item in the list
    if(itor!=value.begin()) {
      os << ",";
    }

    os << *itor;
  }

  os << '}';

  return os;
}
