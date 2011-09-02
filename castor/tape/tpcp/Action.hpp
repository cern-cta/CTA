/******************************************************************************
 *                 castor/tape/tpcp/Action.hpp
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

#ifndef CASTOR_TAPE_TPCP_ACTION_HPP
#define CASTOR_TAPE_TPCP_ACTION_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Enumeration class representing the different actions tpcp can perform.
 *
 * This enumeration class has the following advantages over a plain
 * enumeration:
 * <ul>
 * <li>Enumeration integer values are within there own name space, e.g.
 * Action::READ as opposed to ACTION_READ.
 * <li>Enumeration values include both an integer and a string represention,
 * e.g. Action::read.value() == Action::READ and Action::read.string() == "READ"
 * <li>The class provides its string to Enumeration value method, i.e.
 * Action::stringToAction().
 * <li>The class provides a method for writing all of the valid string values
 * to an output stream, i.e. writeValidStrings().
 * </ul>
 *
 * The above advantages fitted well with the requirements of an Action
 * enumeration required in the command-line parsing logic of TpcpCommand.
 */
class Action {
public:

  /**
   * The Enumeration values.
   */
  enum Value {
    READ,
    WRITE,
    DUMP,

    // MIN and MAX are used to check whether or not an integer N
    // is within the valid range of Action enumeration values:
    // MIN <= N <= MAX
    MIN = READ,
    MAX = DUMP};

  /**
   * The value read.
   */
  const static Action read;

  /**
   * The value write.
   */
  const static Action write;

  /**
   * The value dump.
   */
  const static Action dump;

  /**
   * Returns the calss enumeration object for the specified string.
   *
   * This method throws an InvalidArgument exception if the specified string is
   * invalid.
   *
   * @param str The string.
   */
  static const Action &stringToObject(const char *const str)
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
  friend bool operator== (Action const& a, Action const& b) {
    return a.m_value == b.m_value;
  }

  /**
   * != operator.
   */
  friend bool operator!= (Action const& a, Action const& b) {
    return a.m_value != b.m_value;
  }


private:

  /**
   * All of the valid value objects of the enumeration class in enumeration
   * order.
   */
  static const Action *s_objects[];

  /**
   * The enumeration value of this Action.  This value can be used in switch
   * statements.
   */
  Value m_value;

  /**
   * The string value of this Action.
   */
  const char *m_str;

  /**
   * Private constructor preventing Action objects from being instantiated.
   */
  Action(const Value value, const char *str) throw();

}; // class Action

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_ACTION_HPP
