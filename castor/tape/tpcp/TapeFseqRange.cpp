/******************************************************************************
 *                 castor/tape/tpcp/TapeFseqRange.cpp
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
 
#include "castor/tape/tpcp/TapeFseqRange.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TapeFseqRange::TapeFseqRange() throw() {

  reset();
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TapeFseqRange::TapeFseqRange(const uint32_t lower,
  const uint32_t upper) throw(exception::InvalidArgument) {

  reset(lower, upper);
}


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::tape::tpcp::TapeFseqRange::reset() throw() {
  m_isEmpty = true;
  m_lower   = 0; // Ignored
  m_upper   = 0; // Ignored
}


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::tape::tpcp::TapeFseqRange::reset(const uint32_t lower,
  const uint32_t upper) throw(exception::InvalidArgument) {

  if(lower == 0) {
    exception::InvalidArgument ex;

    ex.getMessage() << "Lower boundary must not be zero";
    throw(ex);
  }

  // If the upper boundary is not 0 meaning infinity and the lower boundary is
  // greater than the upper boundary
  if(upper != 0 && lower > upper) {
    exception::InvalidArgument ex;

    ex.getMessage() <<
      "Lower boundary must be less than or equal to the upper boundary"
      ": lower=" << lower << " upper=" << upper;

    throw(ex);
  }

  m_isEmpty = false;
  m_lower   = lower;
  m_upper   = upper;
}


//------------------------------------------------------------------------------
// isEmpty
//------------------------------------------------------------------------------
bool castor::tape::tpcp::TapeFseqRange::isEmpty() const throw() {
  return m_isEmpty;
}


//------------------------------------------------------------------------------
// lower
//------------------------------------------------------------------------------
uint32_t castor::tape::tpcp::TapeFseqRange::lower() const throw() {

  return m_isEmpty ? 0 : m_lower;
}


//------------------------------------------------------------------------------
// upper
//------------------------------------------------------------------------------
uint32_t castor::tape::tpcp::TapeFseqRange::upper() const throw() {

  return m_isEmpty ? 0 : m_upper;
}


//------------------------------------------------------------------------------
// size
//------------------------------------------------------------------------------
uint32_t castor::tape::tpcp::TapeFseqRange::size() const throw() {

  return m_isEmpty || m_upper == 0 ? 0 : m_upper - m_lower + 1;
}
