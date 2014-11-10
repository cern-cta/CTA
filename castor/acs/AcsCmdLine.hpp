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

#include <string>

namespace castor {
namespace acs {

/**
 * Class containing the code common to the parsed command-line of the ACS
 * command-line tools provided by CASTOR.
 */
struct AcsCmdLine {

  /**
   * Parses the specified query interval.
   *
   * @return The parse query interval.
   */
  static int parseQueryInterval(const std::string &s);

  /**
   * Parses the specified timeout.
   *
   * @return The parse query interval.
   */
  static int parseTimeout(const std::string &s);

  /**
   * Handles the specified parameter that is missing a parameter.
   *
   * @param option The option.
   */
  static void handleMissingParameter(const int option);

  /**
   * Handles the specified unknown option.
   *
   * @param option The option.
   */
  static void handleUnknownOption(const int option);

}; // class AcsCmdLine

} // namespace acs
} // namespace castor
