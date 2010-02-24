/******************************************************************************
 *                 castor/tape/aggregator/RtcpdIOConnState.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_RTCPDIOCONNSTATE_HPP
#define CASTOR_TAPE_AGGREGATOR_RTCPDIOCONNSTATE_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"

namespace castor     {
namespace tape       {
namespace aggregator {

/**
 * Enumeration class representing the different states of an RTCPD disk/tape IO
 * control-connection.
 */
class RtcpdIOConnState {
public:

  /**
   * The Enumeration values.
   */
  enum Value {
    IDLE,
    SENDING_FILE_REQUEST_TO_CLIENT,
    WAITING_FOR_FILE_FROM_CLIENT,
    SENDING_FILE_TO_RTCPD,
    SENDING_NO_MORE_FILES_TO_RTCPD,
    SENDING_ACK_OF_TRANSFERED_TO_RTCPD,
    SENDING_TRANSFERED_TO_CLIENT,
    WAITING_FOR_ACK_FROM_CLIENT

    // MIN and MAX are used to check whether or not an integer N
    // is within the valid range of enumeration values:
    // MIN <= N <= MAX
    MIN = IDLE,
    MAX = WAITING_FOR_ACK_FROM_CLIENT};

  /**
   * The value idle.
   */
  const static RtcpdIOConnState idle;

  /**
   * The value sending_file_request_to_client.
   */
  const static RtcpdIOConnState sending_file_request_to_client;

  /**
   * The value waiting_for_file_from_client.
   */
  const static RtcpdIOConnState waiting_for_file_from_client;

  /**
   * The value sending_file_to_rtcpd.
   */
  const static RtcpdIOConnState waiting_for_file_from_client;


  /**
   * The value verify.
   */
  const static RtcpdIOConnState verify;

  /**
   * Returns the calss enumeration object for the specified string.
   *
   * This method throws an InvalidArgument exception if the specified string is
   * invalid.
   *
   * @param str The string.
   */
  static const RtcpdIOConnState &stringToObject(const char *const str)
    throw(castor::exception::InvalidArgument);

  /**
   * Writes the list of valid enumeration strings to the specified output
   * stream separated by the specified separator.
   *
   * @param os The output stream to be written to.
   * @param separator The separator to be written between the strings.
   */
  static void writeValidStrings(std::ostream &os, const char *const separator);

  /**
   * Returns the enumeration value of this object.
   */
  Value value() const;

  /**
   * Returns the string representation of this object.
   */
  const char *str() const;

  /**
   * == operator.
   */
  friend bool operator== (RtcpdIOConnState const& a, RtcpdIOConnState const& b) {
    return a.m_value == b.m_value;
  }

  /**
   * != operator.
   */
  friend bool operator!= (RtcpdIOConnState const& a, RtcpdIOConnState const& b) {
    return a.m_value != b.m_value;
  }


private:

  /**
   * All of the valid value objects of the enumeration class in enumeration
   * order.
   */
  static const RtcpdIOConnState *s_objects[];

  /**
   * The enumeration value of this RtcpdIOConnState.  This value can be used in switch
   * statements.
   */
  Value m_value;

  /**
   * The string value of this RtcpdIOConnState.
   */
  const char *m_str;

  /**
   * Private constructor preventing RtcpdIOConnState objects from being instantiated.
   */
  RtcpdIOConnState(const Value value, const char *str) throw();

}; // class RtcpdIOConnState

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_RTCPDIOCONNSTATE_HPP
