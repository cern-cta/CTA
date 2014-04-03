/******************************************************************************
 *         castor/tape/legacymsg/RtcpDumpTapeRqstMsgBody.hpp
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

#pragma once

#include <stdint.h>

namespace castor {
namespace tape {
namespace legacymsg {

/**
 * An RTCP dump tape request message.
 */
struct RtcpDumpTapeRqstMsgBody {
  int32_t maxBytes;
  int32_t blockSize;
  int32_t convert_noLongerUsed;
  int32_t tapeErrAction;
  int32_t startFile;
  int32_t maxFiles;
  int32_t fromBlock;
  int32_t toBlock;

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0.
   */
  RtcpDumpTapeRqstMsgBody() throw();
}; // struct RtcpDumpTapeRqstMsgBody

} // namespace legacymsg
} // namespace tape
} // namespace castor

