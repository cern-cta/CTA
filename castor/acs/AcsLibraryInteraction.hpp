/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Mismatch.hpp"
#include "castor/exception/RequestFailed.hpp"
#include "castor/acs/Acs.hpp"
#include "castor/log/Logger.hpp"

#include <string>

extern "C" {
#include "acssys.h"
#include "acsapi.h"
}

namespace castor {
namespace acs {

/**
 * Abstract class implementing common code and data structures for mount
 * for recall, mount for migration and dismount requests 
 * that interact with ACS compatible tape libraries.
 */
class AcsLibraryInteraction  {
public:
  /**
   * Constructor.
   *
   * @param acs Wrapper around the ACSLS C-API.
   */
  AcsLibraryInteraction(Acs &acs, log::Logger &log) throw();

  /**
   * Pure-virtual destructor to guarantee this class is abstract.
   */
  virtual ~AcsLibraryInteraction() throw() = 0;

protected:

  /**
   * Requests responses from ACSLS in a loop until the RT_FINAL response is
   * received.
   *
   * @param requestSeqNumber The sequemce number that was sent in the initial
   * request to the ACSLS.
   * @param buf Output parameter.  Message buffer into which the RT_FINAL
   * response shall be written.
   * @param queryInterval Time in seconds to wait between queries to ACS for
   * responses.
   * @param timeout The time in seconds to spend trying to get the RT_FINAL
   * response.
   */
  void requestResponsesUntilFinal(const SEQ_NO requestSeqNumber,
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)],
    const int queryInterval, const int timeout) const
    ;

  /**
   * Sends a request for a response to the ACSLS.
   *
   * @param timeout The timeout.
   * @param requestSeqNumber The sequemce number that was sent in the initial
   * request to the ACSLS.
   * @param buf Output parameter.  The response message if there is one.
   * @return The type of the response message if there is one or RT_NONE if
   * there isn't one.
   */
  ACS_RESPONSE_TYPE requestResponse(const int timeout,
    const SEQ_NO requestSeqNumber,
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const
    ;

  /**
   * Throws castor::exception::Mismatch if the specified request and
   * response sequence-numbers do not match.
   *
   * @param requestSeqNumber Request sequence-number.
   * @param responseSeqNumber Response sequence-number.
   */
  void checkResponseSeqNumber(const SEQ_NO requestSeqNumber,
    const SEQ_NO responseSeqNumber) const ;

  /**
   * Wrapper around the ACSLS C-API.
   */
  Acs &m_acs;
  
  /**
   * Logger.
   */
  log::Logger &m_log;
  
}; // class AcsLibraryInteraction

} // namespace acs
} // namespace castor


