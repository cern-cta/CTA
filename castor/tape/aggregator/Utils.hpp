/******************************************************************************
 *                      Utils.hpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_UTILS_HPP
#define CASTOR_TAPE_AGGREGATOR_UTILS_HPP 1

#include "castor/exception/Exception.hpp"

#include <errno.h>
#include <string.h>
#include <iostream>

namespace castor {
namespace tape {
namespace aggregator {
  	
/**
 * Class of static utility functions.
 */
class Utils {
public:

  /**
   * Sets all the bytes of the specified object to the value of c.
   *
   * @param object The object whose bytes are to be set.
   * @param c The value to set each byte of object.
   */
  template<typename T> static void setBytes(T &object, const int c) {
    memset(&object, c, sizeof(object));
  }

  /**
   * Safely copies source string into destination string.  The destination
   * will always be null terminated if this function is successful.
   *
   * @param dst Destination string.
   * @param src Source string.
   * @param n   The maximum number of characters to be copied from source to
   * destination.
   */
  static void copyString(char *const dst, const char *src, const size_t n)
    throw(castor::exception::Exception);

  /**
   * Safely copies source string into destination string.  The destination
   * will always be null terminated if this function is successful.
   *
   * @param dst Destination string.
   * @param src Source string.
   */
  template<int n> static void copyString(char (&dst)[n], const char *src)
    throw(castor::exception::Exception) {

    copyString(dst, src, n);
  }

private:

  /**
   * Private constructor to inhibit instances of this class from being
   * instantiated.
   */
  Utils() {}

}; // class Utils

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_UTILS_HPP
