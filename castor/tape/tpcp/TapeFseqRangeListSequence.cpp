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
castor::tape::tpcp::TapeFseqRangeListSequence::TapeFseqRangeListSequence(
  TapeFseqRangeList &list) throw(castor::exception::Exception) :
  m_list(list), m_rangeItor(list.begin()), m_nbSequence(*(list.begin())) {
}


//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool castor::tape::tpcp::TapeFseqRangeListSequence::hasMore() throw() {

  return m_nbSequence.hasMore() || m_rangeItor != m_list.end();
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

  if(!m_nbSequence.hasMore()) {
    m_nbSequence = *(++m_rangeItor);
  }

  return m_nbSequence.next();
}
