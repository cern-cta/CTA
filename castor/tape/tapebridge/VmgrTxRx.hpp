/******************************************************************************
 *                castor/tape/tapebridge/VmgrTxRx.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_VMGRTXRX_HPP
#define CASTOR_TAPE_TAPEBRIDGE_VMGRTXRX_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/VmgrMarshal.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <time.h>


namespace castor     {
namespace tape       {
namespace tapebridge {


/**
 * Provides functions for sending and receiving messages to and from the VMGR.
 */
class VmgrTxRx {

public:

  /**
   * Gets information about a tape from the VMGR by sending and receiving the
   * necessary messages.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param volReqId            The volume request ID to be used for logging.
   * @param netReadWriteTimeout The timeout to be applied when performing
   *                            network read and write operations.
   * @param uid                 The uid of the client.
   * @param gid                 The gid of the client.
   * @param vid                 The VID of the tape.
   * @param reply               Out parameter: The tape information message
   *                            structure to be filled with the reply from the
   *                            VMGR.
   */
  static void getTapeInfoFromVmgr(
    const Cuuid_t                  &cuuid,
    const uint32_t                 volReqId,
    const int                      netReadWriteTimeout,
    const uint32_t                 uid,
    const uint32_t                 gid,
    const char                     *const vid,
    legacymsg::VmgrTapeInfoMsgBody &reply)
    throw(castor::exception::Exception);

}; // class VmgrTxRx

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_VMGRTXRX_HPP
