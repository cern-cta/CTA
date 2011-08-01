/******************************************************************************
 *                      castor/tape/tapebridge/ConfigParamAndSource.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMANDSOURCE
#define CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMANDSOURCE

#include <string>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Data structure used to store the name, value and source of a configuration
 * parameter, where source is to be used soley for logging purposes and should
 * have user readable values like "environment variable", * "castor.conf" or
 * "compile-time constant".
 */
template <typename ValueType> struct ConfigParamAndSource {

  /**
   * Constructor used to initialise the members of this structure.
   *
   * @param c The category name of the configuration parameter as used in the
   *          castor.conf file.
   * @param n The name of the configuration parameter as used in the
   *          castor.conf file.
   * @param v The value of the configuration parameter.
   * @param s The source of the configuration parameter.
   */
  ConfigParamAndSource(
    const std::string &c,
    const std::string &n,
    const ValueType   &v,
    const std::string &s):
    category(c),
    name(n),
    value(v),
    source(s) {
    // Do nothing
  }

  /**
   * The category name of the configuration parameter as used in the
   * castor.conf file.
   */
  std::string category;

  /**
   * The name of the configuration parameter as used in the castor.conf file.
   */
  std::string name;

  /**
   * The value of the configuration parameter.
   */
  ValueType value;

  /**
   * The source of the configuration paremeter.
   */
  std::string source;
  
}; // class ConfigParamAndSource

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMANDSOURCE
