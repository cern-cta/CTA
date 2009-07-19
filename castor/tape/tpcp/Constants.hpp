/******************************************************************************
 *                      castor/tape/tpcp/Constants.hpp
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

#ifndef CASTOR_TAPE_TPCP_CONSTANTS_HPP
#define CASTOR_TAPE_TPCP_CONSTANTS_HPP 1

#include <stdint.h>
#include <stdlib.h>


namespace castor {
namespace tape   {
namespace tpcp   {
  	
  const char *const TPCPPROGRAMNAME = "tpcp";

  /**
   * The minimum number of command-line arguments is 2.  The first command-line
   * argument is the ACTION to be performed by tpcp.  The second command-line
   * argument is the VID of tape to copied to/from.  Any command-line
   * argumenst after the first 2 are considered to be RFIO filenames.
   */
  const int TPCPMINARGS = 2;

  /**
   * The number of seconds to stay blocked while waiting for a callback
   * connection from the aggregator.
   */
  const int WAITCALLBACKTIMEOUT = 60;

  /**
   * The time format specified using the recognized formatting characters of 
   * 'std::strftime'.
   */
  const char *const TIMEFORMAT = "%B %d %I:%M:%S";

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_CONSTANTS_HPP
