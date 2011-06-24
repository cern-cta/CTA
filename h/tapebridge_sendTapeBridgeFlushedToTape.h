/******************************************************************************
 *                h/tapebridge_sendTapeBridgeFlushedToTape.h
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

#ifndef H_TAPEBRIDGE_SENDTAPEBRIDGEFLUSHEDTOTAPE_H
#define H_TAPEBRIDGE_SENDTAPEBRIDGEFLUSHEDTOTAPE_H 1

#include "h/osdep.h"
#include "h/tapeBridgeFlushedToTapeMsgBody.h"

#include <stdint.h>

/**
 * Sends a TAPEBRIDGE_TAPEFLUSHED message using the specified socket.
 * 
 * @param socketFd            The file-descriptor of the socket to be written
 *                            to.
 * @param netReadWriteTimeout The timeout in seconds to be used when performing
 *                            network reads and writes.
 * @param msgBody             The message body of the message to be sent.
 * @return                    If successful then the number of bytes written to
 *                            the socket.  On failure this function returns a
 *                            negative value and sets serrno accordingly.
 */
EXTERN_C int32_t tapebridge_sendTapeBridgeFlushedToTape(const int socketFd,
  const int netReadWriteTimeout,
  const tapeBridgeFlushedToTapeMsgBody_t *const msgBody);

#endif /* H_TAPEBRIDGE_SENDTAPEBRIDGEFLUSHEDTOTAPE_H */
