/******************************************************************************
 *                 castor/tape/tpcp/TapeFseqRangeSequence.cpp
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
 
#include "castor/tape/tpcp/TapeFseqRangeSequence.hpp"

#include <errno.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TapeFseqRangeSequence::TapeFseqRangeSequence() throw() {
  // Initialise to an empty sequence
  m_range.lower = 1;
  m_range.upper = 1;
  m_next        = 2;
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TapeFseqRangeSequence::TapeFseqRangeSequence(
  TapeFseqRange &range) throw() : m_range(range), m_next(range.lower) {
}


//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool castor::tape::tpcp::TapeFseqRangeSequence::hasMore() throw() {

  // Until end of tape is represented by range.upper = 0
  return m_range.upper == 0 || m_next <= m_range.upper;
}


//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
uint32_t castor::tape::tpcp::TapeFseqRangeSequence::next()
  throw(castor::exception::Exception) {

  if(!hasMore()) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage()
      << "Invalid operation: Sequence::next() called after end of sequence";

    throw ex;
  }

  return m_next++;
}
