/******************************************************************************
 *                castor/tape/tapebridge/ConfigParamSource.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMSOURCE_HPP
#define CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMSOURCE_HPP 1

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * The possible sources of a configuration-parameter value.
 */
class ConfigParamSource {
public:

  /**
   * The possible sources of a configuration-parameter value.
   */
  enum Enum {
    UNDEFINED,
    ENVIRONMENT_VARIABLE,
    CASTOR_CONF,
    COMPILE_TIME_DEFAULT,
    MIN_VALUE=UNDEFINED,
    MAX_VALUE=COMPILE_TIME_DEFAULT
  };

  /**
   * Returns the string representation of the specified source of a
   * configuration-parameter value.
   */
  static const char *toString(const Enum source) throw();

}; // class ConfigParamSource

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMSOURCE_HPP
