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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include <stdint.h>
#include <stdlib.h>


namespace castor {
namespace tape   {
namespace tpcp   {
  	
  /**
   * The number of seconds to stay blocked while waiting for a callback
   * connection from the tape-server daemon.
   */
  const int WAITCALLBACKTIMEOUT = 60;

  /**
   * The time format specified using the recognized formatting characters of 
   * 'std::strftime'.
   */
  const char *const TIMEFORMAT = "%b %d %H:%M:%S";

  /**
   * The default blocksize in bytes to be used when dumping a tape.
   */
  const int32_t DEFAULTDUMPBLOCKSIZE = 262144;

  /**
   * The default file destination to be used when recalling with -n/--nodata
   * option.
   */
  const char *const NODATAFILENAME = "localhost:/dev/null";


} // namespace tpcp
} // namespace tape
} // namespace castor


