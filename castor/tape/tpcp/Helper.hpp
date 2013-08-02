/******************************************************************************
 *                 castor/tape/tpcp/Helper.hpp
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

#ifndef CASTOR_TAPE_TPCP_HELPER_HPP
#define CASTOR_TAPE_TPCP_HELPER_HPP 1

#include "castor/tape/utils/utils.hpp"

#include <iostream>
#include <sstream>


namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Collection of static helper functions.
 */
class Helper {
public:

  /**
   * Convenience method that displays the specified received message if debug
   * is on.
   *
   * @param msg   The message to be displayed.
   * @param debug To be set to true if debug is on, else false.
   */
  template<class T> static void displayRcvdMsgIfDebug(T &msg, const bool debug)
    throw() {
    if(debug) {
      std::ostream &os = std::cout;

      os <<
        "Received " << objectTypeToString(msg.type()) <<
        " from tapebridge" << std::endl <<
        msg << std::endl;
    }
  }

  /**
   * Convenience method that displays the specified sent message if debug is on.
   *
   * @param msg   The message to be displayed.
   * @param debug To be set to true if debug is on, else false.
   */
  template<class T> static void displaySentMsgIfDebug(T &msg, const bool debug)
    throw() {
    if(debug) {
      std::ostream &os = std::cout;

      os <<
        "Sent " << objectTypeToString(msg.type()) <<
        " to tapebridge" << std::endl <<
        msg << std::endl;
    }
  }

  /**
   * Returns the string representation of the specified CASTOR object type.
   * In the case of the type being unknown, the returned string is "UNKNOWN".
   *
   * @param type The type of the CASTOR object.
   */
  static const char *objectTypeToString(const unsigned int type) throw();

private:

  /**
   * Private destructor to prevent instances of this class from being
   * instantiated.
   */
  ~Helper();

}; // class Helper

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_HELPER_HPP
