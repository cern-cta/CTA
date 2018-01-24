/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/exception/Mismatch.hpp"
#include "common/exception/RequestFailed.hpp"
#include "Acs.hpp"

#include <string>

extern "C" {
#include "acssys.h"
#include "acsapi.h"
}

namespace cta {
namespace acs {

/**
 * Abstract class implementing common code and data structures for mount 
 * for read-only access, mount for read/write access and dismount requests 
 * that interact with ACS compatible tape libraries.
 */
class AcsLibraryInteraction  {
public:
  /**
   * Constructor.
   *
   * @param acs Wrapper around the ACSLS C-API.
   */
  AcsLibraryInteraction(Acs &acs) throw();

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
   * Throws cta::exception::Mismatch if the specified request and
   * response sequence-numbers do not match.
   *
   * @param requestSeqNumber Request sequence-number.
   * @param responseSeqNumber Response sequence-number.
   */
  void checkResponseSeqNumber(const SEQ_NO requestSeqNumber,
    const SEQ_NO responseSeqNumber) const ;
  
  /**
   * Converts the specified volume status to a human readable representation.
   *
   * @param s The volume status.
   * @return  The string presentation of the volume status.
   */
  std::string volumeStatusAsString(const QU_VOL_STATUS &s) const throw();

  /**
   * Wrapper around the ACSLS C-API.
   */
  Acs &m_acs;
  
}; // class AcsLibraryInteraction

} // namespace acs
} // namespace cta


