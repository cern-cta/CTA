/******************************************************************************
 *                 castor/tape/tpcp/TapeFseqRangeSequence.hpp
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

#ifndef CASTOR_TAPE_TPCP_TAPEFSEQRANGESEQUENCE_HPP
#define CASTOR_TAPE_TPCP_TAPEFSEQRANGESEQUENCE_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/tpcp/TapeFseqRange.hpp"

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Generates a sequence of tape file sequence numbers from a range of tape file
 * sequence numbers.
 */
class TapeFseqRangeSequence {
public:

  /**
   * Constructor.
   *
   * @param range The range from which the sequence is generated.
   */
  TapeFseqRangeSequence(TapeFseqRange &range) throw();

  /**
   * Returns true if there is another tape file sequence number in the
   * sequence.
   */
  bool hasMore() throw();

  /**
   * Returns the next  tape file sequence number in the sequence, or throws an
   * exception if there isn't one.
   */
  uint32_t next() throw(castor::exception::Exception);


private:

  /**
   * The range from which the sequence is generated.
   */
  TapeFseqRange m_range;

  /**
   * The value to be returned by a call to next().
   */
  uint32_t m_next;

}; // class TapeFseqRangeSequence

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_TAPEFSEQRANGESEQUENCE_HPP
