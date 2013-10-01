/******************************************************************************
 *                 castor/tape/rmcd/AcsCmd.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_MEDIACHANGER_ACSCMD_HPP
#define CASTOR_TAPE_MEDIACHANGER_ACSCMD_HPP 1

#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Mismatch.hpp"
#include "castor/exception/RequestFailed.hpp"
#include "castor/tape/rmcd/Acs.hpp"
#include "castor/tape/rmcd/DebugBuf.hpp"

#include <istream>
#include <ostream>
#include <string>

extern "C" {
#include "acssys.h"
#include "acsapi.h"
}

namespace castor {
namespace tape {
namespace rmcd {

/**
 * Abstract class implementing common code and data structures for command-line
 * tools that interact with ACLS compatible tape libraries.
 */
class AcsCmd {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param acs Wrapper around the ACSLS C-API.
   */
  AcsCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, Acs &acs) throw();

  /**
   * Pure-virtual destructor to guarantee this class is abstract.
   */
  virtual ~AcsCmd() throw() = 0;

protected:

  /**
   * Standard input stream.
   */
  std::istream &m_in;

  /**
   * Standard output stream.
   */
  std::ostream &m_out;

  /**
   * Standard error stream.
   */
  std::ostream &m_err;

  /**
   * Wrapper around the ACSLS C-API.
   */
  Acs &m_acs;

  /**
   * Debug stream buffer that inserts a standard debug preamble before each
   * message-line written to it.
   */
  DebugBuf m_debugBuf;

  /**
   * Stream used to write debug messages.
   *
   * This stream will insert a standard debug preamble before each message-line
   * written to it.
   */
  std::ostream m_dbg;

  /**
   * Returns the string representation of the specfied boolean value.
   */
  std::string bool2Str(bool &value) const throw();

  /**
   * Returns the string representation of the specfied boolean value.
   */
  std::string bool2Str(BOOLEAN &value) const throw();

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
    const int queryInterval, const int timeout)
    throw (castor::exception::RequestFailed);

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
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
    throw(castor::exception::RequestFailed);

  /**
   * Throws castor::exception::Mismatch if the specified request and
   * response sequence-numbers do not match.
   *
   * @param requestSeqNumber Request sequence-number.
   * @param responseSeqNumber Response sequence-number.
   */
  void checkResponseSeqNumber(const SEQ_NO requestSeqNumber,
    const SEQ_NO responseSeqNumber) throw(castor::exception::Mismatch);

}; // class AcsCmd

} // namespace rmcd
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_MEDIACHANGER_ACSCMD_HPP
