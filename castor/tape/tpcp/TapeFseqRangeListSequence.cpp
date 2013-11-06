/******************************************************************************
 *                 castor/tape/tpcp/TapeFseqRangeListSequence.cpp
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
 
#include "castor/tape/tpcp/TapeFseqRangeListSequence.hpp"

#include <errno.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TapeFseqRangeListSequence::TapeFseqRangeListSequence()
  throw(castor::exception::Exception) {
  reset(NULL);
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TapeFseqRangeListSequence::TapeFseqRangeListSequence(
  const TapeFseqRangeList *const list) throw(castor::exception::Exception) {
  reset(list);
}


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::tape::tpcp::TapeFseqRangeListSequence::reset(
  const TapeFseqRangeList *const list) throw(castor::exception::Exception) {
  m_list = list;

  if(m_list == NULL) {
    m_isFinite  = true;
    m_totalSize = 0;
  } else {
    m_rangeItor  = list->begin();
    m_nbSequence = (*(list->begin()));

    // Determine the values of m_isFinite and m_totalSize
    m_isFinite  = true; // Initial guess
    m_totalSize = 0;    // Initial guess
    for(TapeFseqRangeList::const_iterator itor=list->begin();
      itor != list->end(); itor++) {
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
bool castor::tape::tpcp::TapeFseqRangeListSequence::hasMore() const throw() {
  if(m_list != NULL) {
    return m_nbSequence.hasMore();
  } else {
    return false;
  }
}


//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
uint32_t castor::tape::tpcp::TapeFseqRangeListSequence::next()
  throw(castor::exception::Exception) {

  if(!hasMore()) {
    castor::exception::Exception ex(ECANCELED);

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
bool castor::tape::tpcp::TapeFseqRangeListSequence::isFinite() const throw() {
  return m_isFinite;
}


//------------------------------------------------------------------------------
// totalSize
//------------------------------------------------------------------------------
uint32_t castor::tape::tpcp::TapeFseqRangeListSequence::totalSize()
  const throw() {
  return m_totalSize;
}
