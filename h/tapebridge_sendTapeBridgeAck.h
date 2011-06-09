/******************************************************************************
 *                h/tapebridge_sendTapeBridgeAck.h
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

#ifndef H_TAPEBRIDGE_SENDTAPEBRIDGEACK_H
#define H_TAPEBRIDGE_SENDTAPEBRIDGEACK_H 1

/**
 * Sends a TAPEBRIDGE_CLIENT-acknowledgement message using the specified
 * socket.
 * 
 * @param socketFd            The file-descriptor of the socket to be written
 *                            to.
 * @param netReadWriteTimeout The timeout in seconds to be used when performing
 *                            network reads and writes.
 * @param ackStatus           The status value to marshalled into the message
 *                            body.
 * @param ackErrMsg           The error message to be marshalled into the
 *                            message body.  If the error message is to be
 *                            empty, for example in the case where ackStatus is
 *                            0, then this parameter should be passed an empty
 *                            string.  This parameter should never be passed a
 *                            NULL value.
 * @return                    If successful then the number of bytes written to
 *                            the socket.  On failure this function returns a
 *                            negative value and sets serrno accordingly.
 */
EXTERN_C int32_t tapebridge_sendTapeBridgeAck(const int socketFd,
  const int netReadWriteTimeout, const uint32_t ackStatus,
  const char *const ackErrMsg);

#endif /* H_TAPEBRIDGE_SENDTAPEBRIDGEACK_H */
